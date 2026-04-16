"""
scraper.py — async product page fetcher and content extractor

Fetches one page per unique base product ID and returns cleaned text
containing ingredients, claims, certifications, symptom language, and
review snippets. Used by optimize.py to enrich the feed CSV before
sending it to Claude.

Extraction strategy (in priority order):
  1. JSON-LD schema.org Product markup  — works even on JS-rendered pages
  2. Shopify product JSON API           — /products/<handle>.json
  3. HTML content heuristics            — CSS class / itemprop / table parsing
  4. Full body text fallback
"""

import asyncio
import json
import re
from typing import Callable, Optional
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pl,en-US;q=0.7,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
}

MAX_CONTENT_CHARS = 4000   # per page — fed into brief extractor, not directly to optimizer
REQUEST_TIMEOUT = 20.0     # seconds per attempt
MAX_CONCURRENT = 15        # parallel fetches at once
FETCH_RETRIES = 3          # attempts per URL before giving up


# ---------------------------------------------------------------------------
# JSON-LD extraction (works on JS-rendered pages)
# ---------------------------------------------------------------------------

def _extract_json_ld(soup: BeautifulSoup) -> str:
    """
    Parse all <script type="application/ld+json"> blocks and extract
    product-relevant fields from any schema.org Product objects found.
    Returns a plain-text summary, or "" if nothing useful found.
    """
    parts: list[str] = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string or ""
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        # Normalise to a list so we handle both single objects and @graph arrays
        nodes = data if isinstance(data, list) else data.get("@graph", [data])

        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_type = node.get("@type", "")
            # Accept "Product", ["Product", ...], or "product"
            types = node_type if isinstance(node_type, list) else [node_type]
            if not any(t.lower() == "product" for t in types):
                continue

            name = node.get("name", "")
            if name:
                parts.append(f"Product: {name}")

            desc = node.get("description", "")
            if desc:
                parts.append(f"Description: {desc}")

            brand = node.get("brand", {})
            if isinstance(brand, dict):
                bname = brand.get("name", "")
                if bname:
                    parts.append(f"Brand: {bname}")

            # Ingredients / materials / additionalProperty
            for prop in node.get("additionalProperty", []):
                if isinstance(prop, dict):
                    pname = prop.get("name", "")
                    pval = prop.get("value", "")
                    if pname or pval:
                        parts.append(f"{pname}: {pval}")

            # Aggregate rating
            rating = node.get("aggregateRating", {})
            if isinstance(rating, dict):
                rv = rating.get("ratingValue")
                rc = rating.get("reviewCount")
                if rv:
                    parts.append(f"Rating: {rv}/5 ({rc} reviews)" if rc else f"Rating: {rv}/5")

            # Review snippets (first 3)
            for i, review in enumerate(node.get("review", [])[:3]):
                if isinstance(review, dict):
                    body = review.get("reviewBody", "")
                    if body:
                        parts.append(f"[Review] {body[:300]}")

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Shopify JSON API
# ---------------------------------------------------------------------------

def _shopify_json_url(page_url: str, html: str) -> Optional[str]:
    """
    If the page looks like a Shopify store, return the product JSON API URL.
    Returns None for non-Shopify pages.
    """
    if "cdn.shopify.com" not in html and "Shopify.shop" not in html:
        return None
    # /products/handle → /products/handle.json
    parsed = urlparse(page_url)
    path = parsed.path.rstrip("/")
    if "/products/" in path and not path.endswith(".json"):
        return f"{parsed.scheme}://{parsed.netloc}{path}.json"
    return None


def _parse_shopify_json(data: dict) -> str:
    """Convert a Shopify product JSON response to plain text."""
    product = data.get("product", data)
    parts: list[str] = []

    if product.get("title"):
        parts.append(f"Product: {product['title']}")
    if product.get("vendor"):
        parts.append(f"Brand: {product['vendor']}")
    if product.get("product_type"):
        parts.append(f"Type: {product['product_type']}")

    body = product.get("body_html", "")
    if body:
        text = BeautifulSoup(body, "lxml").get_text(separator=" ", strip=True)
        parts.append(f"Description: {text[:2000]}")

    tags = product.get("tags", [])
    if isinstance(tags, list) and tags:
        parts.append(f"Tags: {', '.join(tags[:20])}")
    elif isinstance(tags, str) and tags:
        parts.append(f"Tags: {tags}")

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# HTML → text extraction (heuristics fallback)
# ---------------------------------------------------------------------------

_NOISE_TAGS = [
    "script", "style", "nav", "footer", "header", "aside",
    "iframe", "noscript", "svg", "button", "form",
]

_PRODUCT_PATTERNS = re.compile(
    r"product.*(description|detail|info|content|about|tab)|"
    r"(description|ingredient|benefit|feature|about|detail|tab.content)",
    re.I,
)

_REVIEW_PATTERNS = re.compile(
    r"review|testimonial|rating.text|customer.comment|opinia",
    re.I,
)

_NOISE_CLASS_PATTERNS = re.compile(r"cookie|popup|modal|overlay|newsletter", re.I)


def extract_page_text(html: str) -> str:
    """
    Extract product-relevant readable text from a raw HTML page.
    Tries JSON-LD first, falls back to HTML heuristics, then full body text.
    Returns a plain text string, truncated to MAX_CONTENT_CHARS.
    """
    soup = BeautifulSoup(html, "lxml")

    # 1. JSON-LD (best signal — present even on JS-rendered pages)
    json_ld_text = _extract_json_ld(soup)
    if len(json_ld_text) > 200:
        return json_ld_text[:MAX_CONTENT_CHARS]

    # 2. HTML heuristics
    for tag in soup(_NOISE_TAGS):
        tag.decompose()
    for tag in soup.find_all(True, {"class": _NOISE_CLASS_PATTERNS}):
        tag.decompose()
    for tag in soup.find_all(True, {"id": _NOISE_CLASS_PATTERNS}):
        tag.decompose()

    parts: list[str] = []

    h1 = soup.find("h1")
    if h1:
        parts.append(h1.get_text(strip=True))

    for el in soup.find_all(True, {"class": _PRODUCT_PATTERNS}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    for el in soup.find_all(True, {"id": _PRODUCT_PATTERNS}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    for el in soup.find_all(True, {"itemprop": "description"}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    for table in soup.find_all("table"):
        t = table.get_text(separator=" | ", strip=True)
        if any(kw in t.lower() for kw in ["ingredient", "składnik", "nutrition", "spec", "contain"]):
            parts.append(t[:1500])

    for ul in soup.find_all(["ul", "ol"]):
        items = [li.get_text(strip=True) for li in ul.find_all("li")]
        joined = " • ".join(i for i in items if len(i) > 10)
        if joined and any(
            kw in joined.lower()
            for kw in ["ingredient", "składnik", "benefit", "korzystn", "certif", "organic", "natural"]
        ):
            parts.append(joined[:1000])

    seen_reviews: set[str] = set()
    for el in soup.find_all(True, {"class": _REVIEW_PATTERNS})[:8]:
        t = el.get_text(separator=" ", strip=True)
        if 20 < len(t) < 500 and t not in seen_reviews:
            seen_reviews.add(t)
            parts.append(f"[Opinia klienta] {t}")
            if len(seen_reviews) >= 5:
                break

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for p in parts:
        key = p[:120]
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    combined = "\n\n".join(deduped)
    combined = re.sub(r"[ \t]{2,}", " ", combined)
    combined = re.sub(r"\n{3,}", "\n\n", combined)

    # 3. Full body fallback
    if not combined.strip():
        body = soup.find("body")
        if body:
            combined = re.sub(r"\s+", " ", body.get_text(separator=" ", strip=True))

    # Prepend JSON-LD snippet if we have any (even short)
    if json_ld_text and combined.strip():
        combined = json_ld_text + "\n\n" + combined
    elif json_ld_text:
        combined = json_ld_text

    return combined[:MAX_CONTENT_CHARS]


# ---------------------------------------------------------------------------
# Async fetching
# ---------------------------------------------------------------------------

async def _fetch_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    base_id: str,
    url: str,
    on_progress: Optional[Callable[[int, int], None]] = None,
    done_counter: Optional[list] = None,
    total: int = 0,
) -> tuple[str, str, bool]:
    """
    Fetch one URL with retry. Returns (base_id, extracted_text, success).
    Tries: HTML → JSON-LD within HTML → Shopify API → retry up to FETCH_RETRIES times.
    """
    async with sem:
        text = ""
        success = False

        for attempt in range(FETCH_RETRIES):
            try:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                try:
                    html = response.content.decode("utf-8")
                except UnicodeDecodeError:
                    html = response.content.decode(response.encoding or "utf-8", errors="replace")

                # Try Shopify JSON API if this looks like a Shopify store
                shopify_url = _shopify_json_url(url, html)
                if shopify_url:
                    try:
                        sj_resp = await client.get(shopify_url, follow_redirects=True)
                        if sj_resp.status_code == 200:
                            shopify_data = sj_resp.json()
                            text = _parse_shopify_json(shopify_data)
                            if len(text) > 100:
                                success = True
                                break
                    except Exception:
                        pass  # fall through to HTML extraction

                text = extract_page_text(html)
                if text.strip():
                    success = True
                    break

            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt < FETCH_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
            except httpx.HTTPStatusError as e:
                # 429 — back off and retry; other 4xx — give up immediately
                if e.response.status_code == 429 and attempt < FETCH_RETRIES - 1:
                    await asyncio.sleep(5 * (attempt + 1))
                elif e.response.status_code >= 400:
                    break
            except Exception:
                break

        if done_counter is not None:
            done_counter[0] += 1
            if on_progress:
                on_progress(done_counter[0], total)

        return base_id, text, success


async def _scrape_all(
    url_map: dict[str, str],
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> dict[str, str]:
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    done_counter = [0]
    total = len(url_map)

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT,
        max_keepalive_connections=MAX_CONCURRENT,
    )
    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
        limits=limits,
    ) as client:
        tasks = [
            _fetch_one(client, sem, base_id, url, on_progress, done_counter, total)
            for base_id, url in url_map.items()
            if url
        ]
        results = await asyncio.gather(*tasks)

    return {base_id: text for base_id, text, _ in results}


def scrape_products(
    url_map: dict[str, str],
    verbose: bool = True,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> dict[str, str]:
    """
    Synchronous entry point.

    Args:
        url_map:     {base_id: url} — one URL per unique product
        verbose:     print summary to stdout
        on_progress: optional callable(done, total) called after each page completes

    Returns:
        {base_id: extracted_page_text} — empty string means scrape failed
    """
    if not url_map:
        return {}

    if verbose:
        print(f"  Fetching {len(url_map)} pages (max {MAX_CONCURRENT} concurrent, {FETCH_RETRIES} retries)...")

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Streamlit (and Jupyter) run their own event loop — use a thread to avoid conflict
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, _scrape_all(url_map, on_progress=on_progress))
            results = future.result()
    else:
        results = asyncio.run(_scrape_all(url_map, on_progress=on_progress))

    if verbose:
        ok = sum(1 for v in results.values() if v.strip())
        failed = len(results) - ok
        print(f"  Result: {ok} OK, {failed} failed")

    return results
