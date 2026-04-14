"""
scraper.py — async product page fetcher and content extractor

Fetches one page per unique base product ID and returns cleaned text
containing ingredients, claims, certifications, symptom language, and
review snippets. Used by optimize.py to enrich the feed CSV before
sending it to Claude.
"""

import asyncio
import re
from typing import Optional

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
REQUEST_TIMEOUT = 20.0     # seconds
MAX_CONCURRENT = 5         # parallel fetches


# ---------------------------------------------------------------------------
# HTML → text extraction
# ---------------------------------------------------------------------------

# Tags that are always noise
_NOISE_TAGS = [
    "script", "style", "nav", "footer", "header", "aside",
    "iframe", "noscript", "svg", "button", "form",
]

# CSS class / id patterns that indicate product content
_PRODUCT_PATTERNS = re.compile(
    r"product.*(description|detail|info|content|about|tab)|"
    r"(description|ingredient|benefit|feature|about|detail|tab.content)",
    re.I,
)

# CSS class / id patterns that indicate review content
_REVIEW_PATTERNS = re.compile(
    r"review|testimonial|rating.text|customer.comment|opinia",
    re.I,
)

# Patterns to strip cookie/popup banners
_NOISE_CLASS_PATTERNS = re.compile(r"cookie|popup|modal|overlay|newsletter", re.I)


def extract_page_text(html: str) -> str:
    """
    Extract product-relevant readable text from a raw HTML page.
    Returns a plain text string, truncated to MAX_CONTENT_CHARS.
    """
    soup = BeautifulSoup(html, "lxml")

    # Remove structural noise
    for tag in soup(_NOISE_TAGS):
        tag.decompose()

    # Remove cookie / popup banners
    for tag in soup.find_all(True, {"class": _NOISE_CLASS_PATTERNS}):
        tag.decompose()
    for tag in soup.find_all(True, {"id": _NOISE_CLASS_PATTERNS}):
        tag.decompose()

    parts: list[str] = []

    # 1. Page title / product name (h1)
    h1 = soup.find("h1")
    if h1:
        parts.append(h1.get_text(strip=True))

    # 2. Product description / detail sections
    for el in soup.find_all(True, {"class": _PRODUCT_PATTERNS}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    for el in soup.find_all(True, {"id": _PRODUCT_PATTERNS}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    # itemprop="description" (schema.org)
    for el in soup.find_all(True, {"itemprop": "description"}):
        t = el.get_text(separator=" ", strip=True)
        if len(t) > 80:
            parts.append(t)

    # 3. Ingredient / nutrition / spec tables
    for table in soup.find_all("table"):
        t = table.get_text(separator=" | ", strip=True)
        if any(kw in t.lower() for kw in ["ingredient", "składnik", "nutrition", "spec", "contain"]):
            parts.append(t[:1500])

    # 4. Bullet / feature lists that mention ingredients or benefits
    for ul in soup.find_all(["ul", "ol"]):
        items = [li.get_text(strip=True) for li in ul.find_all("li")]
        joined = " • ".join(i for i in items if len(i) > 10)
        if joined and any(
            kw in joined.lower()
            for kw in ["ingredient", "składnik", "benefit", "korzystn", "certif", "organic", "natural"]
        ):
            parts.append(joined[:1000])

    # 5. Review / testimonial snippets (first 5 only)
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

    if not combined.strip():
        # Fallback: full body text
        body = soup.find("body")
        if body:
            combined = re.sub(r"\s+", " ", body.get_text(separator=" ", strip=True))

    return combined[:MAX_CONTENT_CHARS]


# ---------------------------------------------------------------------------
# Async fetching
# ---------------------------------------------------------------------------

async def _fetch_one(
    client: httpx.AsyncClient,
    base_id: str,
    url: str,
) -> tuple[str, str, bool]:
    """
    Returns (base_id, extracted_text, success).
    On any error returns (base_id, "", False).
    """
    try:
        response = await client.get(url, follow_redirects=True)
        response.raise_for_status()
        # Decode bytes explicitly: prefer UTF-8, fall back to detected encoding
        try:
            html = response.content.decode("utf-8")
        except UnicodeDecodeError:
            html = response.content.decode(response.encoding or "utf-8", errors="replace")
        text = extract_page_text(html)
        return base_id, text, bool(text.strip())
    except Exception:
        return base_id, "", False


async def _scrape_all(url_map: dict[str, str]) -> dict[str, str]:
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
            _fetch_one(client, base_id, url)
            for base_id, url in url_map.items()
            if url
        ]
        results = await asyncio.gather(*tasks)

    return {base_id: text for base_id, text, _ in results}


def scrape_products(url_map: dict[str, str], verbose: bool = True) -> dict[str, str]:
    """
    Synchronous entry point.

    Args:
        url_map: {base_id: url} — one URL per unique product
        verbose: print progress to stdout

    Returns:
        {base_id: extracted_page_text} — empty string means scrape failed
    """
    if not url_map:
        return {}

    if verbose:
        print(f"  Fetching {len(url_map)} pages concurrently (max {MAX_CONCURRENT} at a time)...")

    results = asyncio.run(_scrape_all(url_map))

    if verbose:
        ok = sum(1 for v in results.values() if v.strip())
        failed = len(results) - ok
        for base_id, text in results.items():
            status = f"{len(text)} chars" if text.strip() else "FAILED"
            print(f"    [{base_id}] {status}")
        print(f"  Result: {ok} OK, {failed} failed")

    return results
