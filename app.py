import io
import os

import pandas as pd
import streamlit as st
import anthropic

from scraper import scrape_products
from optimize import (
    BATCH_SIZE,
    CostTracker,
    build_url_map,
    call_claude_batched,
    enrich_dataframe,
    extract_csv_and_summary,
    extract_structured_briefs,
    get_base_id,
    get_group_id,
    load_system_prompt,
    merge_claude_output,
    read_feed_csv,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Feed Optimizer",
    page_icon="🛒",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MODELS = [
    "claude-sonnet-4-6",
    "claude-haiku-4-5-20251001",
    "claude-opus-4-6",
]

MARKETS = {
    "Global": "0",
    # Western Europe
    "United Kingdom": "2826",
    "Germany": "2276",
    "France": "2250",
    "Italy": "2380",
    "Spain": "2724",
    "Netherlands": "2528",
    "Belgium": "2056",
    "Austria": "2040",
    "Switzerland": "2756",
    "Portugal": "2620",
    "Ireland": "2372",
    "Luxembourg": "2442",
    # Northern Europe
    "Sweden": "2752",
    "Norway": "2578",
    "Denmark": "2208",
    "Finland": "2246",
    # Central & Eastern Europe
    "Poland": "2616",
    "Czech Republic": "2203",
    "Slovakia": "2703",
    "Hungary": "2348",
    "Romania": "2642",
    "Bulgaria": "2100",
    "Croatia": "2191",
    "Slovenia": "2705",
    "Serbia": "2688",
    "Ukraine": "2804",
    # Baltic
    "Estonia": "2233",
    "Latvia": "2428",
    "Lithuania": "2440",
    # Southern Europe
    "Greece": "2300",
    "Cyprus": "2196",
    "Malta": "2470",
    # Other
    "United States": "2840",
}

LANGUAGES = {
    "Auto-detect": "",
    # Major
    "English": "en",
    "German": "de",
    "French": "fr",
    "Italian": "it",
    "Spanish": "es",
    "Polish": "pl",
    "Dutch": "nl",
    "Portuguese": "pt",
    # Northern
    "Swedish": "sv",
    "Norwegian": "no",
    "Danish": "da",
    "Finnish": "fi",
    # Central & Eastern
    "Czech": "cs",
    "Slovak": "sk",
    "Hungarian": "hu",
    "Romanian": "ro",
    "Bulgarian": "bg",
    "Croatian": "hr",
    "Slovenian": "sl",
    "Serbian": "sr",
    "Ukrainian": "uk",
    # Baltic
    "Estonian": "et",
    "Latvian": "lv",
    "Lithuanian": "lt",
    # Southern
    "Greek": "el",
}


def get_secret(key: str) -> str:
    """Read from st.secrets (Streamlit Cloud) or environment variable."""
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return os.environ.get(key, "")


def anthropic_ready() -> bool:
    return bool(get_secret("ANTHROPIC_API_KEY"))


def google_ads_ready() -> bool:
    required = [
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID",
    ]
    return all(get_secret(k) for k in required)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🛒 Feed Optimizer")
    st.caption("Google Shopping feed optimization")

    st.divider()

    st.markdown("**Config status**")
    if anthropic_ready():
        st.success("✓ Anthropic API ready")
    else:
        st.error("✗ Anthropic API key missing")

    if google_ads_ready():
        st.success("✓ Google Ads API ready")
    else:
        st.warning("○ Google Ads API not configured")

    if "last_run" in st.session_state:
        st.divider()
        lr = st.session_state.last_run
        st.markdown("**Last run**")
        st.markdown(f"File: `{lr['filename']}`")
        st.markdown(f"Rows: **{lr['rows']}**")
        st.markdown(f"Products: **{lr['products']}**")
        st.markdown(f"Cost: **${lr['cost']:.4f}**")
        if lr.get("scrape_ok") is not None:
            st.markdown(f"Pages scraped: **{lr['scrape_ok']}/{lr['scrape_total']}**")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

st.markdown("# Feed Optimizer")
st.caption(
    "Upload a Google Shopping feed CSV, scrape product pages for richer data, "
    "and let Claude rewrite titles, descriptions, and custom labels."
)

# ── Upload ──────────────────────────────────────────────────────────────────

uploaded_file = st.file_uploader(
    "Upload product feed CSV",
    type=["csv"],
    label_visibility="collapsed",
)

# ── Options ─────────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
with col1:
    model = st.selectbox("Model", MODELS)
with col2:
    market_label = st.selectbox("Market (geo)", list(MARKETS.keys()))
with col3:
    language_label = st.selectbox("Language", list(LANGUAGES.keys()))
with col4:
    scrape_enabled = st.toggle("Scrape pages", value=True)

# ── Column selection ─────────────────────────────────────────────────────────

with st.expander("Columns to optimize", expanded=True):
    st.caption("Uncheck any column you want to leave as-is. audit_flags is always generated.")

    ccol1, ccol2, ccol3 = st.columns(3)
    with ccol1:
        opt_title       = st.checkbox("Title",                     value=True, key="col_title")
        opt_description = st.checkbox("Description",               value=True, key="col_desc")
        opt_ptype       = st.checkbox("Product type",              value=True, key="col_ptype")
    with ccol2:
        opt_label0      = st.checkbox("Label 0 — Price tier",      value=True, key="col_l0")
        opt_label1      = st.checkbox("Label 1 — Product form",    value=True, key="col_l1")
        opt_label2      = st.checkbox("Label 2 — Primary benefit", value=True, key="col_l2")
    with ccol3:
        opt_label3      = st.checkbox("Label 3 — Pack type",       value=True, key="col_l3")
        opt_label4      = st.checkbox("Label 4 — Audience",        value=True, key="col_l4")

    st.divider()
    include_brand_in_title = st.checkbox(
        "Include brand name in optimized titles",
        value=False,
        key="include_brand",
        help="Enable only if your Merchant Center feed rules do NOT already append the brand.",
    )

selected_columns = [
    col for col, enabled in [
        ("optimized_title",        opt_title),
        ("optimized_description",  opt_description),
        ("product_type_suggested", opt_ptype),
        ("custom_label_0",         opt_label0),
        ("custom_label_1",         opt_label1),
        ("custom_label_2",         opt_label2),
        ("custom_label_3",         opt_label3),
        ("custom_label_4",         opt_label4),
    ]
    if enabled
]

# ── Run button ───────────────────────────────────────────────────────────────

run_clicked = st.button(
    "Run",
    type="primary",
    disabled=not (uploaded_file and anthropic_ready()),
    use_container_width=False,
)

if not anthropic_ready():
    st.info("Add your ANTHROPIC_API_KEY to `.streamlit/secrets.toml` or environment variables to run.")

# ── Pipeline ─────────────────────────────────────────────────────────────────

if run_clicked and uploaded_file:
    df = read_feed_csv(uploaded_file)
    unique_products = df.apply(get_group_id, axis=1).nunique()
    tracker = CostTracker()
    api_key = get_secret("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)

    scrape_ok = 0
    scrape_total = 0

    with st.status("Running optimization...", expanded=True) as status:

        # Step 1: Scrape
        if scrape_enabled:
            st.write(f"Scraping {unique_products} product pages...")
            url_map = build_url_map(df)
            scrape_total = len(url_map)
            if url_map:
                scrape_bar = st.progress(0, text="Fetching pages...")

                def on_scrape_progress(done, total):
                    scrape_bar.progress(done / total, text=f"Scraped {done}/{total} pages...")

                scraped = scrape_products(url_map, verbose=False, on_progress=on_scrape_progress)
                scrape_ok = sum(1 for v in scraped.values() if v.strip())
                scrape_bar.empty()
                st.write(f"✓ {scrape_ok}/{scrape_total} pages scraped")

                # Step 2: Extract structured briefs
                pages_with_content = sum(1 for v in scraped.values() if v.strip())
                if pages_with_content:
                    st.write(f"Extracting structured briefs ({pages_with_content} pages)...")
                    brief_bar = st.progress(0, text="Extracting briefs...")

                    def on_brief_progress(done, total):
                        brief_bar.progress(done / total, text=f"Briefs {done}/{total}...")

                    briefs = extract_structured_briefs(
                        scraped, client, tracker, verbose=False, on_progress=on_brief_progress
                    )
                    brief_bar.empty()
                else:
                    briefs = extract_structured_briefs(scraped, client, tracker, verbose=False)

                enriched_df = enrich_dataframe(df, briefs)
                csv_text = enriched_df.to_csv(index=False)
                st.write("✓ Structured briefs ready")
            else:
                st.warning("No valid URLs found — using CSV descriptions only.")
                csv_text = df.to_csv(index=False)
        else:
            st.write("Scraping skipped.")
            csv_text = df.to_csv(index=False)

        # Step 3: Optimize (batched for large feeds)
        total_rows_for_batch = len(pd.read_csv(io.StringIO(csv_text), dtype=str))
        total_batches = max(1, -(-total_rows_for_batch // BATCH_SIZE))  # ceiling division
        batch_label = f"{total_batches} batch{'es' if total_batches > 1 else ''} of {BATCH_SIZE}"
        st.write(f"Calling Claude ({model}) — {total_rows_for_batch} rows, {batch_label}...")

        progress_bar = st.progress(0, text="Starting...")
        rate_limit_placeholder = st.empty()

        def on_batch_start(batch_num, total):
            progress_bar.progress(
                (batch_num - 1) / total,
                text=f"Batch {batch_num}/{total}...",
            )

        def on_batch_done(batch_num, total):
            progress_bar.progress(
                batch_num / total,
                text=f"✓ Batch {batch_num}/{total} complete",
            )

        def on_rate_limit(wait_secs, attempt):
            rate_limit_placeholder.warning(
                f"Rate limit hit — waiting {wait_secs}s before retry {attempt}/{6}..."
            )

        system_prompt = load_system_prompt()
        brand_instruction = (
            "IMPORTANT OVERRIDE: Do NOT include the brand name at the start of any optimized_title. "
            "The brand will be appended at the end automatically as '| Brand'. "
            "Start every title directly with the symptom or product name, skipping the brand entirely."
        )
        try:
            raw_response = call_claude_batched(
                csv_text,
                system_prompt,
                model,
                client,
                tracker,
                columns=selected_columns,
                extra_context=brand_instruction,
                on_batch_start=on_batch_start,
                on_batch_done=on_batch_done,
                on_rate_limit=on_rate_limit,
            )
        except anthropic.RateLimitError as e:
            status.update(label="API error", state="error")
            st.error(f"Claude API error 429: {e.message}")
            st.stop()
        except anthropic.APIStatusError as e:
            status.update(label="API error", state="error")
            st.error(f"Claude API error {e.status_code}: {e.message}")
            st.stop()
        except anthropic.APIConnectionError as e:
            status.update(label="Connection error", state="error")
            st.error(f"Could not connect to Claude API: {e}")
            st.stop()

        rate_limit_placeholder.empty()
        progress_bar.empty()
        st.write("✓ Optimization complete")
        status.update(label="Done!", state="complete", expanded=False)

    # ── Parse & merge results ────────────────────────────────────────────────

    csv_output, summary = extract_csv_and_summary(raw_response)

    expected_col = selected_columns[0] if selected_columns else "audit_flags"
    if not csv_output or expected_col not in csv_output:
        st.warning("Could not cleanly extract CSV from response. Showing raw output.")
        csv_output = raw_response

    # Merge Claude's compact output back into original DataFrame
    result_df = merge_claude_output(df, csv_output)

    # Always append brand at the end of optimized_title
    if "brand" in result_df.columns and "optimized_title" in result_df.columns:
        def _append_brand(row):
            title = str(row["optimized_title"]).strip()
            brand = str(row["brand"]).strip()
            if not title or not brand:
                return title
            # Strip brand from start if Claude included it despite the instruction
            if title.lower().startswith(brand.lower()):
                title = title[len(brand):].lstrip(" |,-")
            # Append at end if not already there
            if not title.lower().endswith(brand.lower()):
                title = f"{title} | {brand}"
            return title
        result_df["optimized_title"] = result_df.apply(_append_brand, axis=1)

    output_csv = result_df.to_csv(index=False, encoding="utf-8")

    total_cost = tracker.total_cost()

    # Save to session state for sidebar
    st.session_state.last_run = {
        "filename": uploaded_file.name,
        "rows": len(df),
        "products": unique_products,
        "cost": total_cost,
        "scrape_ok": scrape_ok,
        "scrape_total": scrape_total,
    }

    # ── Download ─────────────────────────────────────────────────────────────

    st.success(f"Optimized {len(df)} rows across {unique_products} products — ${total_cost:.4f}")

    output_filename = uploaded_file.name.replace(".csv", "_optimized.csv")
    st.download_button(
        label="⬇ Download optimized CSV",
        data=output_csv.encode("utf-8"),
        file_name=output_filename,
        mime="text/csv",
        type="primary",
    )

    # ── Summary ───────────────────────────────────────────────────────────────

    if summary:
        with st.expander("Optimization summary", expanded=True):
            st.markdown(summary)

    # ── Cost breakdown ────────────────────────────────────────────────────────

    with st.expander("Cost breakdown"):
        rows = []
        for call in tracker.calls:
            from optimize import MODEL_PRICING
            in_price, out_price = MODEL_PRICING.get(call["model"], (0, 0))
            cost = (
                call["input_tokens"] / 1_000_000 * in_price
                + call["output_tokens"] / 1_000_000 * out_price
            )
            rows.append({
                "Step": call["label"],
                "Model": call["model"],
                "Input tokens": call["input_tokens"],
                "Output tokens": call["output_tokens"],
                "Cost ($)": round(cost, 5),
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.markdown(f"**Total: ${total_cost:.4f}**")

    # ── Preview ───────────────────────────────────────────────────────────────

    with st.expander("Output preview"):
        try:
            preview_df = pd.read_csv(io.StringIO(output_csv))
            st.dataframe(preview_df, use_container_width=True)
        except Exception:
            st.text(output_csv[:3000])
