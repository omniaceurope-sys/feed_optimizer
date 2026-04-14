import io
import os

import pandas as pd
import streamlit as st
import anthropic

from scraper import scrape_products
from optimize import (
    CostTracker,
    build_url_map,
    call_claude,
    enrich_dataframe,
    extract_csv_and_summary,
    extract_structured_briefs,
    get_base_id,
    load_system_prompt,
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
    df = pd.read_csv(uploaded_file, dtype=str).fillna("")
    unique_products = df["id"].apply(get_base_id).nunique()
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
                scraped = scrape_products(url_map, verbose=False)
                scrape_ok = sum(1 for v in scraped.values() if v.strip())
                st.write(f"✓ {scrape_ok}/{scrape_total} pages scraped")

                # Step 2: Extract structured briefs
                st.write("Extracting structured briefs...")
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

        # Step 3: Optimize
        st.write(f"Calling Claude ({model})...")
        system_prompt = load_system_prompt()
        try:
            raw_response = call_claude(csv_text, system_prompt, model, client, tracker, columns=selected_columns)
        except anthropic.APIStatusError as e:
            status.update(label="API error", state="error")
            st.error(f"Claude API error {e.status_code}: {e.message}")
            st.stop()
        except anthropic.APIConnectionError as e:
            status.update(label="Connection error", state="error")
            st.error(f"Could not connect to Claude API: {e}")
            st.stop()

        st.write("✓ Optimization complete")
        status.update(label="Done!", state="complete", expanded=False)

    # ── Parse & display results ──────────────────────────────────────────────

    csv_output, summary = extract_csv_and_summary(raw_response)

    expected_col = selected_columns[0] if selected_columns else "audit_flags"
    if not csv_output or expected_col not in csv_output:
        st.warning("Could not cleanly extract CSV from response. Showing raw output.")
        csv_output = raw_response

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
        data=csv_output.encode("utf-8"),
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
            preview_df = pd.read_csv(io.StringIO(csv_output))
            st.dataframe(preview_df, use_container_width=True)
        except Exception:
            st.text(csv_output[:3000])
