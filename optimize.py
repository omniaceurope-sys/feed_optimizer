#!/usr/bin/env python3
"""
Google Shopping Feed Optimizer
================================
Enriches a product CSV feed with optimized titles, descriptions, custom labels,
and audit flags using Claude AI + live page scraping.

Usage:
    python optimize.py data/input/feed.csv
    python optimize.py data/input/feed.csv --output data/output/feed_optimized.csv
    python optimize.py data/input/feed.csv --no-scrape
    python optimize.py data/input/feed.csv --model claude-sonnet-4-6
"""

import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
import threading
import time

# Ensure stdout can handle Unicode on Windows terminals
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import anthropic

from scraper import scrape_products

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_MODEL = "claude-sonnet-4-6"
BRIEF_MODEL = "claude-haiku-4-5-20251001"   # cheap model used only for brief extraction
MAX_OUTPUT_TOKENS = 64000

# Batching — rows per Claude call for large feeds
BATCH_SIZE = 50
# Retry — how many times to retry on 429, and base delay in seconds
MAX_RETRIES = 6
RETRY_BASE_DELAY = 60

# Column name for the structured brief added to the CSV before sending to optimizer
BRIEF_COL = "product_page_brief"
KEYWORD_ANGLES_COL = "keyword_angles"

# Currency → (language_code, location_code) for Keyword Planner
CURRENCY_TO_LOCALE: dict[str, tuple[str, str]] = {
    "PLN": ("pl", "pl"),
    "GBP": ("en", "gb"),
    "USD": ("en", "us"),
    "EUR": ("de", "de"),   # EUR is ambiguous — override with --language/--location for IT, FR, ES, etc.
    "AUD": ("en", "au"),
    "CAD": ("en", "ca"),
    "SEK": ("sv", "se"),
    "NOK": ("no", "no"),
    "DKK": ("da", "dk"),
    "CZK": ("cs", "cz"),
    "HUF": ("hu", "hu"),
    "RON": ("ro", "ro"),
}

# New columns the agent is expected to produce
EXPECTED_NEW_COLS = [
    "optimized_title",
    "optimized_description",
    "product_type_suggested",
    "custom_label_0",
    "custom_label_1",
    "custom_label_2",
    "custom_label_3",
    "custom_label_4",
    "audit_flags",
]

# Pricing per million tokens (input, output) — update if Anthropic changes rates
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5-20251001": (0.80, 4.00),
    "claude-sonnet-4-6":         (3.00, 15.00),
    "claude-opus-4-6":           (15.00, 75.00),
}

BRIEF_EXTRACTION_PROMPT = """\
Extract a structured product brief from this product page text.
Return ONLY the following format — no extra text, no explanation:

INGREDIENTS: <comma-separated ingredients/materials/components, or — if none>
CLAIMS: <comma-separated benefit or efficacy claims stated on the page, or —>
CERTIFICATIONS: <certifications, quality marks, guarantees, or —>
SYMPTOMS: <exact words/phrases the page uses to describe the problem this product solves, or —>
PRODUCT_FORM: <physical format and size, e.g. "60 kapsułek", "napój w proszku 30 porcji", or —>
REVIEWS: <2–3 short customer phrases describing their problem before buying, or —>\
"""


# ---------------------------------------------------------------------------
# Cost tracking
# ---------------------------------------------------------------------------

class CostTracker:
    """Accumulates token usage across multiple API calls and prints a summary."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def record(self, model: str, input_tokens: int, output_tokens: int, label: str = "") -> None:
        with self._lock:
            self.calls.append({
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "label": label,
            })

    def total_cost(self) -> float:
        total = 0.0
        for call in self.calls:
            in_price, out_price = MODEL_PRICING.get(call["model"], (0, 0))
            total += call["input_tokens"] / 1_000_000 * in_price
            total += call["output_tokens"] / 1_000_000 * out_price
        return total

    def print_summary(self) -> None:
        print("\n--- Cost breakdown ---")
        for call in self.calls:
            in_price, out_price = MODEL_PRICING.get(call["model"], (0, 0))
            cost = (
                call["input_tokens"] / 1_000_000 * in_price
                + call["output_tokens"] / 1_000_000 * out_price
            )
            label = f"  [{call['label']}]" if call["label"] else ""
            print(
                f"  {call['model']}{label}: "
                f"{call['input_tokens']:,} in + {call['output_tokens']:,} out "
                f"= ${cost:.4f}"
            )
        print(f"  TOTAL: ${self.total_cost():.4f}")


# ---------------------------------------------------------------------------
# ID helpers
# ---------------------------------------------------------------------------

def get_base_id(product_id: str) -> str:
    """
    Strip variant suffix from a product ID.

    Examples:
        "29515_g"   -> "29515_g"   (no variant suffix)
        "29515-2_g" -> "29515_g"   (strip -2)
        "29515-3_g" -> "29515_g"   (strip -3)
        "1001-6"    -> "1001"      (no trailing suffix)
        "1001"      -> "1001"
    """
    # Remove -<digits> that appear before a non-digit character or end of string
    return re.sub(r"-\d+(?=\D|$)", "", str(product_id))


# ---------------------------------------------------------------------------
# CSV reading — handles both standard and Google Merchant Center export formats
# ---------------------------------------------------------------------------

# Output columns added by the optimizer — stripped before re-optimizing
_PREV_OUTPUT_COLS = {
    "optimized_title", "optimized_description", "product_type_suggested",
    "custom_label_0", "custom_label_1", "custom_label_2",
    "custom_label_3", "custom_label_4", "audit_flags",
}


def read_feed_csv(path: Path) -> pd.DataFrame:
    """
    Robustly read a feed CSV and normalise it for the optimizer.

    Handles:
    - Google Merchant Center export format (column names with spaces)
    - Extra trailing columns from a previous optimization run
    - Output columns that are stripped before re-optimizing
    - Rows with more fields than header columns (bad CSV from prev run)
    """
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return pd.DataFrame()
        rows = list(reader)

    n_cols = len(header)

    # Fix row length mismatches
    fixed_rows: list[list[str]] = []
    for row in rows:
        if len(row) > n_cols:
            fixed_rows.append(row[:n_cols])   # truncate extra trailing fields
        elif len(row) < n_cols:
            fixed_rows.append(row + [""] * (n_cols - len(row)))  # pad short rows
        else:
            fixed_rows.append(row)

    df = pd.DataFrame(fixed_rows, columns=header).fillna("")

    # Normalize column names: lowercase, strip whitespace, spaces → underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Strip output columns from previous optimizer runs
    df = df.drop(columns=[c for c in df.columns if c in _PREV_OUTPUT_COLS], errors="ignore")

    return df


def get_group_id(row: "pd.Series") -> str:
    """
    Return the base product ID used for grouping variants.
    Prefers item_group_id (Shopify/MC export format) over base-ID stripping.
    """
    group_id = str(row.get("item_group_id", "")).strip()
    if group_id and group_id not in ("", "nan"):
        return group_id
    return get_base_id(str(row.get("id", "")))


# ---------------------------------------------------------------------------
# URL map
# ---------------------------------------------------------------------------

def build_url_map(df: pd.DataFrame) -> dict[str, str]:
    """
    Build {group_id: url} — one URL per unique base product.
    Uses the first row encountered for each group_id (typically the single/base variant).
    """
    url_map: dict[str, str] = {}
    for _, row in df.iterrows():
        gid = get_group_id(row)
        if gid not in url_map:
            url = str(row.get("link", "")).strip()
            if url.startswith("http"):
                url_map[gid] = url
    return url_map


# ---------------------------------------------------------------------------
# Brief extraction (Haiku per product page)
# ---------------------------------------------------------------------------

BRIEF_WORKERS = 20   # concurrent Haiku calls for brief extraction


def extract_structured_briefs(
    scraped: dict[str, str],
    client: anthropic.Anthropic,
    tracker: "CostTracker",
    verbose: bool = True,
    on_progress=None,   # optional callable(done: int, total: int)
) -> dict[str, str]:
    """
    For each scraped page text, call Claude Haiku to produce a structured brief.
    Runs BRIEF_WORKERS calls concurrently for speed on large feeds.
    Returns {base_id: brief_text}. Empty string if page was not scraped.
    """
    if verbose:
        print(f"\nExtracting structured briefs ({BRIEF_MODEL}, {BRIEF_WORKERS} workers)...")

    briefs: dict[str, str] = {}
    done_count = [0]

    # Pre-populate empties so they don't go through the thread pool
    ids_with_text = {bid: text for bid, text in scraped.items() if text.strip()}
    for bid in scraped:
        if not scraped[bid].strip():
            briefs[bid] = ""

    total = len(ids_with_text)

    def _extract_one(base_id: str, page_text: str) -> tuple[str, str, int, int]:
        try:
            message = client.messages.create(
                model=BRIEF_MODEL,
                max_tokens=450,
                messages=[{
                    "role": "user",
                    "content": (
                        f"{BRIEF_EXTRACTION_PROMPT}\n\n"
                        f"Page text:\n{page_text[:4000]}"
                    ),
                }],
            )
            brief = message.content[0].text.strip()
            return base_id, brief, message.usage.input_tokens, message.usage.output_tokens
        except Exception as e:
            if verbose:
                print(f"  [{base_id}] FAILED: {e}")
            return base_id, "", 0, 0

    with ThreadPoolExecutor(max_workers=BRIEF_WORKERS) as executor:
        futures = {
            executor.submit(_extract_one, bid, text): bid
            for bid, text in ids_with_text.items()
        }
        for future in as_completed(futures):
            base_id, brief, in_tok, out_tok = future.result()
            briefs[base_id] = brief
            if in_tok:
                tracker.record(BRIEF_MODEL, in_tok, out_tok, label=base_id)
            done_count[0] += 1
            if on_progress:
                on_progress(done_count[0], total)
            elif verbose:
                print(f"  [{base_id}] {len(brief)} chars ({done_count[0]}/{total})")

    return briefs


# ---------------------------------------------------------------------------
# Keyword Planner integration
# ---------------------------------------------------------------------------

def _parse_brief_section(brief: str, section: str) -> list[str]:
    """Pull a comma-separated list out of one SECTION: line in a structured brief."""
    match = re.search(rf"^{section}:\s*(.+?)(?=\n[A-Z_]+:|$)", brief, re.MULTILINE | re.DOTALL)
    if not match:
        return []
    raw = match.group(1).strip()
    if raw in ("—", "-", ""):
        return []
    return [item.strip() for item in re.split(r",\s*", raw) if item.strip() and item.strip() not in ("—", "-")]


_MAX_SYMPTOM_WORDS = 5  # planner phrases longer than this read unnaturally in titles


def _trim_phrase(phrase: str) -> str:
    """Truncate a phrase to at most _MAX_SYMPTOM_WORDS words."""
    words = phrase.split()
    return " ".join(words[:_MAX_SYMPTOM_WORDS])


def _build_candidates(brief: str) -> tuple[list[str], list[str]]:
    """
    Extract symptom and ingredient candidate phrases from a structured brief.
    Returns (symptom_candidates, ingredient_candidates).

    For supplements: SYMPTOMS + REVIEWS → symptom candidates, INGREDIENTS → ingredient candidates.
    For apparel / non-supplement products: CLAIMS doubles as symptom candidates (search desires),
    PRODUCT_FORM is used as an ingredient candidate (product type keyword).
    """
    symptoms = _parse_brief_section(brief, "SYMPTOMS")
    reviews  = _parse_brief_section(brief, "REVIEWS")
    claims   = _parse_brief_section(brief, "CLAIMS")   # apparel: benefit/desire phrases

    symptom_candidates = list(dict.fromkeys(
        _trim_phrase(s.lower().strip())
        for s in symptoms + reviews + claims
        if len(s.strip()) > 5
    ))[:10]

    raw_ingredients = _parse_brief_section(brief, "INGREDIENTS")
    product_form    = _parse_brief_section(brief, "PRODUCT_FORM")  # apparel: fabric/form keyword

    ingredient_candidates = list(dict.fromkeys(
        i.lower().strip() for i in raw_ingredients + product_form if len(i.strip()) > 3
    ))[:8]

    return symptom_candidates, ingredient_candidates


def _call_planner_script(keywords: list[str], language: str, location: str) -> list[dict]:
    """Call keyword_planner.py as a subprocess and return parsed JSON results."""
    script = Path(__file__).parent / "scripts" / "keyword_planner.py"
    if not script.exists():
        print("  [keyword_planner] ERROR: script not found at", script, file=sys.stderr)
        return []
    cmd = [sys.executable, str(script), "--keywords", *keywords, "--language", language, "--location", location]
    try:
        result = subprocess.run(cmd, capture_output=True, cwd=Path(__file__).parent, timeout=120)
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            print(f"  [keyword_planner] FAILED (exit {result.returncode}): {stderr}", file=sys.stderr)
            return []
        return json.loads(result.stdout.decode("utf-8"))
    except subprocess.TimeoutExpired:
        print("  [keyword_planner] TIMEOUT after 120s", file=sys.stderr)
        return []
    except Exception as exc:
        print(f"  [keyword_planner] EXCEPTION: {exc}", file=sys.stderr)
        return []


def _split_results(planner_results: list[dict], symptom_set: set[str], ingredient_set: set[str]) -> tuple[list[dict], list[dict]]:
    """Split planner output back into symptom and ingredient ranked lists."""
    symptoms    = sorted([r for r in planner_results if r["keyword"] in symptom_set],
                         key=lambda x: (-x["avg_monthly_searches"], {"LOW":0,"MEDIUM":1,"HIGH":2}.get(x["competition"], 3)))
    ingredients = sorted([r for r in planner_results if r["keyword"] in ingredient_set],
                         key=lambda x: (-x["avg_monthly_searches"], {"LOW":0,"MEDIUM":1,"HIGH":2}.get(x["competition"], 3)))
    return symptoms, ingredients


def _pack_label(variant_id: str, base_id: str) -> str:
    """Derive pack label from variant ID suffix: '29188' → 'single', '29188-3' → '3-pack'."""
    remainder = str(variant_id)[len(str(base_id)):]
    m = re.match(r"-(\d+)", remainder)
    return f"{m.group(1)}-pack" if m else "single"


def build_keyword_angles(
    briefs: dict[str, str],
    variants_map: dict[str, list[str]],
    language: str,
    location: str,
    verbose: bool = True,
) -> dict[str, str]:
    """
    For each unique product, call Keyword Planner and return a formatted
    keyword_angles string keyed by base_id.

    Returns {base_id: angles_text} — empty string if planner unavailable.
    """
    if verbose:
        print(f"\nRunning Keyword Planner (language={language}, location={location})...")

    angles: dict[str, str] = {}

    for base_id, brief in briefs.items():
        if not brief.strip():
            angles[base_id] = ""
            continue

        symptom_candidates, ingredient_candidates = _build_candidates(brief)
        all_candidates = symptom_candidates + ingredient_candidates

        if not all_candidates:
            angles[base_id] = ""
            if verbose:
                print(f"  [{base_id}] no candidates extracted from brief")
            continue

        results = _call_planner_script(all_candidates, language, location)

        if not results:
            angles[base_id] = "keyword_planner_unavailable"
            if verbose:
                print(f"  [{base_id}] planner unavailable — will use Claude's estimates")
            continue

        sym_set = {c.lower().strip() for c in symptom_candidates}
        ing_set = {c.lower().strip() for c in ingredient_candidates}
        ranked_sym, ranked_ing = _split_results(results, sym_set, ing_set)

        variant_ids = variants_map.get(base_id, [base_id])
        lines = [f"KEYWORD_ANGLES (ranked by avg monthly searches, {location.upper()}, {language}):"]
        for i, vid in enumerate(variant_ids):
            sym = ranked_sym[i] if i < len(ranked_sym) else (ranked_sym[-1] if ranked_sym else None)
            ing = ranked_ing[i] if i < len(ranked_ing) else (ranked_ing[0]  if ranked_ing else None)
            sym_str = f'"{sym["keyword"]}" ({sym["avg_monthly_searches"]}/mo, {sym["competition"]})' if sym else "n/a"
            ing_str = f'"{ing["keyword"]}" ({ing["avg_monthly_searches"]}/mo, {ing["competition"]})' if ing else "n/a"
            label = _pack_label(vid, base_id)
            lines.append(f"  {vid} ({label}): symptom={sym_str} | ingredient={ing_str}")

        angles[base_id] = "\n".join(lines)

        if verbose:
            top_sym = ranked_sym[0]["keyword"] if ranked_sym else "—"
            top_vol = ranked_sym[0]["avg_monthly_searches"] if ranked_sym else 0
            print(f"  [{base_id}] {len(ranked_sym)} symptoms, {len(ranked_ing)} ingredients "
                  f"— top: \"{top_sym}\" ({top_vol}/mo)")

    return angles


def detect_locale(df: pd.DataFrame) -> tuple[str, str]:
    """Infer language and location from the feed's price currency."""
    prices = df["price"].dropna().astype(str)
    for price in prices:
        for currency, (lang, loc) in CURRENCY_TO_LOCALE.items():
            if currency in price:
                return lang, loc
    return "en", "us"  # fallback


# ---------------------------------------------------------------------------
# CSV enrichment
# ---------------------------------------------------------------------------

def enrich_dataframe(
    df: pd.DataFrame,
    briefs: dict[str, str],
    angles: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Add product_page_brief (and optionally keyword_angles) columns, shared across variants."""
    enriched = df.copy()
    enriched[BRIEF_COL] = enriched.apply(
        lambda row: briefs.get(get_group_id(row), ""), axis=1
    )
    if angles is not None:
        enriched[KEYWORD_ANGLES_COL] = enriched.apply(
            lambda row: angles.get(get_group_id(row), ""), axis=1
        )
    return enriched


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def load_system_prompt() -> str:
    path = Path(__file__).parent / "CLAUDE.md"
    if not path.exists():
        raise FileNotFoundError(f"System prompt not found: {path}")
    return path.read_text(encoding="utf-8")


def call_claude(
    csv_text: str,
    system_prompt: str,
    model: str,
    client: anthropic.Anthropic,
    tracker: "CostTracker",
    columns: list[str] | None = None,
    extra_context: str = "",
) -> str:
    # Determine which output columns Claude should generate
    all_output_cols = [
        "optimized_title",
        "optimized_description",
        "product_type_suggested",
        "custom_label_0",
        "custom_label_1",
        "custom_label_2",
        "custom_label_3",
        "custom_label_4",
    ]
    if columns and set(columns) != set(all_output_cols):
        active = [c for c in all_output_cols if c in columns]
        skipped = [c for c in all_output_cols if c not in columns]
        col_instruction = (
            f"Generate ONLY these output columns (plus id and audit_flags): {', '.join(active)}. "
            f"For the following columns output an empty string: {', '.join(skipped)}.\n\n"
        )
    else:
        active = all_output_cols
        col_instruction = ""

    output_cols_list = ", ".join(["id"] + active + ["audit_flags"])

    user_message = (
        (f"{extra_context}\n\n" if extra_context else "") +
        "Here is the product feed CSV to optimize.\n\n"
        f"The `{BRIEF_COL}` column contains a structured brief extracted from each "
        "product's live page, with these fields: INGREDIENTS, CLAIMS, CERTIFICATIONS, "
        "SYMPTOMS, PRODUCT_FORM, REVIEWS. Use it as your primary source for all "
        "optimization. If the brief is empty for a product, fall back to the `description` "
        "column and add `scrape_failed` to that product's `audit_flags`.\n\n"
        f"The `{KEYWORD_ANGLES_COL}` column contains real search volume data from Google Ads "
        "Keyword Planner, already ranked highest-to-lowest. Each row shows the best symptom "
        "phrase and ingredient phrase to use per variant (single, 2-pack, 3-pack, 6-pack). "
        "You MUST use these exact phrases as the keyword anchors in your optimized titles and "
        "descriptions — do not substitute synonyms. If this column is empty or says "
        "'keyword_planner_unavailable', fall back to your own judgement and add "
        "`keyword_planner_unavailable` to `audit_flags`.\n\n"
        + col_instruction +
        f"IMPORTANT: Output a compact CSV containing ONLY these columns: {output_cols_list}. "
        f"Do NOT repeat or re-output any other original columns — the original data will be "
        f"merged back in Python. Do NOT include the `{BRIEF_COL}` or `{KEYWORD_ANGLES_COL}` columns.\n\n"
        "Return the compact CSV first, then the summary section.\n\n"
        f"```csv\n{csv_text}\n```"
    )

    chunks: list[str] = []
    # System prompt is passed as a content block so Anthropic can cache it across batch calls.
    with client.beta.messages.stream(
        model=model,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=[{
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": user_message}],
        betas=["output-128k-2025-02-19", "prompt-caching-2024-07-31"],
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
        final = stream.get_final_message()

    tracker.record(model, final.usage.input_tokens, final.usage.output_tokens, label="optimizer")
    return "".join(chunks)


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------

def call_claude_with_retry(
    csv_text: str,
    system_prompt: str,
    model: str,
    client: anthropic.Anthropic,
    tracker: "CostTracker",
    columns: list[str] | None = None,
    extra_context: str = "",
    max_retries: int = MAX_RETRIES,
    on_rate_limit=None,
) -> str:
    """
    Wraps call_claude with exponential backoff on HTTP 429 rate-limit errors.
    on_rate_limit: optional callable(wait_secs, attempt) for UI feedback.
    """
    delay = RETRY_BASE_DELAY
    for attempt in range(max_retries):
        try:
            return call_claude(csv_text, system_prompt, model, client, tracker, columns, extra_context)
        except anthropic.RateLimitError as e:
            if attempt >= max_retries - 1:
                raise
            # Honour Retry-After header when present
            wait = delay
            try:
                retry_after = e.response.headers.get("retry-after")
                if retry_after:
                    wait = max(int(retry_after), delay)
            except Exception:
                pass
            if on_rate_limit:
                on_rate_limit(wait, attempt + 1)
            else:
                print(f"  Rate limited (429). Waiting {wait}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait)
            delay = min(delay * 2, 300)  # cap at 5 min


# ---------------------------------------------------------------------------
# Price range helper for consistent tier labels across batches
# ---------------------------------------------------------------------------

def _catalog_price_note(df: pd.DataFrame) -> str:
    """Return a short context string with the full catalog's price range."""
    prices: list[float] = []
    currency = ""
    for price_str in df["price"].dropna().astype(str):
        for curr in CURRENCY_TO_LOCALE:
            if curr in price_str:
                currency = curr
                m = re.search(r"[\d.]+", price_str.replace(",", "."))
                if m:
                    try:
                        prices.append(float(m.group()))
                    except ValueError:
                        pass
                break
    if not prices:
        return ""
    lo, hi = min(prices), max(prices)
    return (
        f"CONTEXT — full catalog price range: {currency} {lo:.2f} to {currency} {hi:.2f}. "
        "Use this for consistent price tier labels (bottom third = budget, middle = mid, top = premium). "
        "This batch is a slice of a larger feed; apply tiers relative to the full range above."
    )


# ---------------------------------------------------------------------------
# Batched optimizer call
# ---------------------------------------------------------------------------

def call_claude_batched(
    csv_text: str,
    system_prompt: str,
    model: str,
    client: anthropic.Anthropic,
    tracker: "CostTracker",
    columns: list[str] | None = None,
    extra_context: str = "",
    batch_size: int = BATCH_SIZE,
    on_batch_start=None,   # (batch_num: int, total: int) -> None
    on_batch_done=None,    # (batch_num: int, total: int) -> None
    on_rate_limit=None,    # (wait_secs: int, attempt: int) -> None
) -> str:
    """
    Split csv_text into batches of batch_size rows, call Claude on each,
    and merge the results into a single CSV string.

    For feeds that fit in one batch, this is a direct pass-through.
    """
    reader_df = pd.read_csv(io.StringIO(csv_text), dtype=str).fillna("")
    total_rows = len(reader_df)

    if total_rows <= batch_size:
        return call_claude_with_retry(
            csv_text, system_prompt, model, client, tracker, columns,
            extra_context=extra_context,
            on_rate_limit=on_rate_limit,
        )

    # Combine caller's extra_context with the price range note
    price_note = _catalog_price_note(reader_df)
    combined_context = "\n\n".join(filter(None, [extra_context, price_note]))

    batches = [reader_df.iloc[i : i + batch_size] for i in range(0, total_rows, batch_size)]
    total_batches = len(batches)
    print(f"Feed has {total_rows} rows — processing in {total_batches} batches of {batch_size}.")

    all_lines: list[str] = []
    header_written = False

    for batch_num, batch_df in enumerate(batches, 1):
        if on_batch_start:
            on_batch_start(batch_num, total_batches)
        else:
            print(f"  Batch {batch_num}/{total_batches}...")

        batch_csv = batch_df.to_csv(index=False)
        raw = call_claude_with_retry(
            batch_csv, system_prompt, model, client, tracker, columns,
            extra_context=combined_context,
            on_rate_limit=on_rate_limit,
        )
        csv_part, _ = extract_csv_and_summary(raw)

        if csv_part and ("optimized_title" in csv_part or "audit_flags" in csv_part):
            lines = csv_part.splitlines()
            if not header_written:
                all_lines.extend(lines)
                header_written = True
            else:
                all_lines.extend(lines[1:])  # skip duplicate header
        else:
            print(f"  WARNING: Batch {batch_num} produced no parseable CSV — skipped.")

        if on_batch_done:
            on_batch_done(batch_num, total_batches)
        else:
            print(f"  Batch {batch_num}/{total_batches} done.")

    return "\n".join(all_lines)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def extract_csv_and_summary(response: str) -> tuple[str, str]:
    """
    Split Claude's response into (csv_content, summary_text).

    Claude is instructed to output CSV first, then a summary. We handle:
    1. CSV wrapped in ```csv ... ``` fences
    2. Raw CSV followed by a markdown summary section
    3. Fallback: entire response treated as CSV
    """
    # Case 1: explicit ```csv ... ``` fence
    match = re.search(r"```(?:csv)?\s*\n(.*?)```", response, re.DOTALL)
    if match:
        csv_part = match.group(1).strip()
        summary_part = response[match.end():].strip()
        return csv_part, summary_part

    # Case 2: find the CSV header line and split at the first summary marker
    lines = response.splitlines()
    csv_lines: list[str] = []
    summary_lines: list[str] = []
    in_csv = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detect CSV start by looking for a header row containing "id,"
        if not in_csv and re.match(r"^id[,\t]", stripped, re.I):
            in_csv = True

        if in_csv:
            # Detect transition to summary: markdown heading or known summary phrases
            if stripped and re.match(
                r"^(#{1,3} |Total rows|Łączna liczba|\*\*(Total|Summary|Podsumowanie))",
                stripped,
            ):
                summary_lines = lines[i:]
                break
            csv_lines.append(line)
        # Lines before the CSV header are discarded (preamble)

    if csv_lines:
        return "\n".join(csv_lines).strip(), "\n".join(summary_lines).strip()

    # Case 3: fallback
    return response.strip(), ""


# ---------------------------------------------------------------------------
# Output merging — join Claude's compact output back into the original df
# ---------------------------------------------------------------------------

OUTPUT_COLS = [
    "optimized_title",
    "optimized_description",
    "product_type_suggested",
    "custom_label_0",
    "custom_label_1",
    "custom_label_2",
    "custom_label_3",
    "custom_label_4",
    "audit_flags",
]


def merge_claude_output(original_df: pd.DataFrame, claude_csv: str) -> pd.DataFrame:
    """
    Parse Claude's compact CSV (id + output cols only) and left-join it back
    onto the original DataFrame. Original columns are never modified.
    Returns a DataFrame with original columns + new output columns appended.
    """
    # Use csv.reader for robust parsing — handles quoted commas, normalises row lengths
    try:
        reader = csv.reader(io.StringIO(claude_csv))
        raw_rows = list(reader)
    except Exception as e:
        print(f"  WARNING: Could not parse Claude output CSV: {e}")
        return original_df

    if not raw_rows:
        print("  WARNING: Claude output CSV is empty — cannot merge.")
        return original_df

    raw_header = [c.strip().lower().replace(" ", "_") for c in raw_rows[0]]
    n_cols = len(raw_header)

    if "id" not in raw_header:
        print("  WARNING: Claude output has no 'id' column — cannot merge.")
        return original_df

    fixed_data = []
    for row in raw_rows[1:]:
        if not any(cell.strip() for cell in row):
            continue  # skip blank rows
        if len(row) > n_cols:
            fixed_data.append(row[:n_cols])   # truncate extra fields
        elif len(row) < n_cols:
            fixed_data.append(row + [""] * (n_cols - len(row)))  # pad short rows
        else:
            fixed_data.append(row)

    claude_df = pd.DataFrame(fixed_data, columns=raw_header).fillna("")

    # Keep only id + known output columns that Claude actually produced
    keep = ["id"] + [c for c in OUTPUT_COLS if c in claude_df.columns]
    claude_df = claude_df[keep].drop_duplicates(subset=["id"])

    result = original_df.copy()
    # Drop any stale output cols from a previous run on the original df
    result = result.drop(columns=[c for c in OUTPUT_COLS if c in result.columns], errors="ignore")

    result = result.merge(claude_df, on="id", how="left")

    # Fill any rows Claude missed with empty strings
    for col in OUTPUT_COLS:
        if col not in result.columns:
            result[col] = ""
        else:
            result[col] = result[col].fillna("")

    return result


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def ensure_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def default_output_path(input_path: Path, suffix: str = "_optimized") -> Path:
    output_dir = input_path.parent.parent / "output"
    return output_dir / (input_path.stem + suffix + ".csv")


def save_raw_response(response: str, output_path: Path) -> None:
    """Save the full raw Claude response alongside the output CSV for debugging."""
    debug_path = output_path.with_suffix(".debug.txt")
    debug_path.write_text(response, encoding="utf-8")
    print(f"  Raw response saved: {debug_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Google Shopping Feed Optimizer — rewrites titles, descriptions, and labels using Claude AI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python optimize.py data/input/feed.csv
  python optimize.py data/input/feed.csv --output data/output/result.csv
  python optimize.py data/input/feed.csv --no-scrape
  python optimize.py data/input/feed.csv --model claude-opus-4-6
        """,
    )
    parser.add_argument("input", help="Path to input CSV file")
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Path to write optimized CSV (default: data/output/<input_stem>_optimized.csv)",
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip page scraping — use only the CSV description column",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Claude model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--save-debug",
        action="store_true",
        help="Save the full raw Claude response to a .debug.txt file",
    )
    parser.add_argument(
        "--no-keyword-planner",
        action="store_true",
        help="Skip Keyword Planner step — Claude estimates keyword priority instead",
    )
    parser.add_argument(
        "--output-suffix",
        default="_optimized_v2",
        help="Suffix appended to input filename for the output file (default: _optimized_v2)",
    )
    parser.add_argument(
        "--language",
        default=None,
        help="Override detected language for Keyword Planner (e.g. 'it' for Italian, 'cs' for Czech)",
    )
    parser.add_argument(
        "--location",
        default=None,
        help="Override detected location for Keyword Planner (e.g. 'it' for Italy, 'cz' for Czech Republic)",
    )
    args = parser.parse_args()

    # ── API key check ──────────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY is not set.")
        print("  Copy .env.example to .env and add your key, or run:")
        print("  export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    # ── Load input CSV ─────────────────────────────────────────────────────
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}")
        sys.exit(1)

    print(f"\nReading: {input_path}")
    df = read_feed_csv(input_path)

    unique_products = df.apply(get_group_id, axis=1).nunique()
    print(f"  {len(df)} rows | {unique_products} unique products")

    # ── Determine output path ──────────────────────────────────────────────
    output_path = Path(args.output) if args.output else default_output_path(input_path, args.output_suffix)
    ensure_output_dir(output_path)

    tracker = CostTracker()
    client = anthropic.Anthropic(api_key=api_key)

    # ── Scraping + brief extraction ────────────────────────────────────────
    if args.no_scrape:
        print("\nScraping skipped (--no-scrape).")
        csv_text = df.to_csv(index=False)
    else:
        print("\nScraping product pages...")
        url_map = build_url_map(df)
        if not url_map:
            print("  WARNING: No valid URLs found in 'link' column. Falling back to CSV data only.")
            csv_text = df.to_csv(index=False)
        else:
            scraped = scrape_products(url_map, verbose=True)
            briefs = extract_structured_briefs(scraped, client, tracker, verbose=True)

            # ── Keyword Planner ────────────────────────────────────────────
            angles: dict[str, str] = {}
            if not args.no_keyword_planner:
                language, location = detect_locale(df)
                if args.language:
                    language = args.language
                if args.location:
                    location = args.location
                variants_map: dict[str, list[str]] = {}
                for _, row in df.iterrows():
                    gid = get_group_id(row)
                    variants_map.setdefault(gid, []).append(str(row["id"]))
                angles = build_keyword_angles(briefs, variants_map, language, location, verbose=True)

            enriched_df = enrich_dataframe(df, briefs, angles if angles else None)
            csv_text = enriched_df.to_csv(index=False)

    # ── Claude optimizer call (batched + retry) ───────────────────────────
    print(f"\nCalling Claude optimizer ({args.model})...")
    system_prompt = load_system_prompt()

    try:
        raw_response = call_claude_batched(csv_text, system_prompt, args.model, client, tracker)
    except anthropic.RateLimitError as e:
        print(f"ERROR: Claude API rate limit exceeded after {MAX_RETRIES} retries: {e.message}")
        sys.exit(1)
    except anthropic.APIStatusError as e:
        print(f"ERROR: Claude API returned an error: {e.status_code} — {e.message}")
        sys.exit(1)
    except anthropic.APIConnectionError as e:
        print(f"ERROR: Could not connect to Claude API: {e}")
        sys.exit(1)

    if args.save_debug:
        save_raw_response(raw_response, output_path)

    # ── Parse response ─────────────────────────────────────────────────────
    csv_output, summary = extract_csv_and_summary(raw_response)

    if not csv_output or "optimized_title" not in csv_output:
        print("\nWARNING: Could not reliably extract CSV from response.")
        print("  Saving full response as .debug.txt for inspection.")
        save_raw_response(raw_response, output_path)
        csv_output = csv_output or raw_response

    # ── Check for missing rows and retry ──────────────────────────────────
    input_ids = set(df["id"].astype(str))
    try:
        output_ids = {r["id"] for r in csv.DictReader(io.StringIO(csv_output))}
        missing_ids = input_ids - output_ids
    except Exception:
        missing_ids = set()

    if missing_ids:
        print(f"\nWARNING: {len(missing_ids)} rows missing from output: {sorted(missing_ids)}")
        print("  Retrying with missing rows only...")
        missing_df = enriched_df[enriched_df["id"].isin(missing_ids)]
        try:
            retry_response = call_claude(
                missing_df.to_csv(index=False), system_prompt, args.model, client, tracker
            )
            retry_csv, _ = extract_csv_and_summary(retry_response)
            if retry_csv and "optimized_title" in retry_csv:
                # Merge retry rows into the main compact csv
                retry_lines = retry_csv.splitlines()
                retry_data = "\n".join(retry_lines[1:])  # drop header
                csv_output = csv_output.rstrip() + "\n" + retry_data
                print(f"  Retry succeeded — appended {len(retry_lines)-1} rows")
            else:
                print("  Retry response could not be parsed — missing rows not recovered")
        except Exception as e:
            print(f"  Retry failed: {e}")

    # ── Merge Claude output back into original DataFrame ───────────────────
    result_df = merge_claude_output(df, csv_output)

    # ── Append brand to optimized_title if present ─────────────────────────
    if "brand" in result_df.columns and "optimized_title" in result_df.columns:
        def _append_brand(row: pd.Series) -> str:
            title = str(row["optimized_title"]).strip()
            brand = str(row["brand"]).strip()
            if not title or not brand or brand.lower() in title.lower():
                return title
            return f"{title} | {brand}"
        result_df["optimized_title"] = result_df.apply(_append_brand, axis=1)

    # ── Write output ───────────────────────────────────────────────────────
    result_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"\nOutput written: {output_path}")

    if summary:
        print("\n" + "-" * 60)
        print("OPTIMIZATION SUMMARY")
        print("-" * 60)
        print(summary)

    tracker.print_summary()
    print("\nDone.")


if __name__ == "__main__":
    main()
