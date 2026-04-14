"""
keyword_planner.py — Google Ads Keyword Planner wrapper for feed-optimizer.

Usage (called by Claude Code during symptom extraction):
    python scripts/keyword_planner.py \
        --keywords "bloating relief" "digestive support" "gut health supplement" \
        --language en \
        --location 2840

    python scripts/keyword_planner.py --file keywords.txt --language pl --location 2616

Output: JSON array sorted by avg_monthly_searches descending, written to stdout.

[
  {"keyword": "bloating relief", "avg_monthly_searches": 14800, "competition": "HIGH"},
  {"keyword": "digestive support", "avg_monthly_searches": 5400, "competition": "MEDIUM"},
  ...
]

Exit codes:
    0 — success
    1 — API error (details on stderr)
    2 — missing credentials / bad config

Environment variables (or .env file at project root):
    GOOGLE_ADS_DEVELOPER_TOKEN
    GOOGLE_ADS_CLIENT_ID
    GOOGLE_ADS_CLIENT_SECRET
    GOOGLE_ADS_REFRESH_TOKEN
    GOOGLE_ADS_CUSTOMER_ID      — the Manager (MCC) account ID, no dashes
    GOOGLE_ADS_LOGIN_CUSTOMER_ID — optional, same as CUSTOMER_ID if not using MCC

Dependencies:
    google-ads>=24.0.0
    python-dotenv>=1.0.0
"""

import argparse
import io
import json
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

# Load .env from project root (two levels up from this script)
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"ERROR: environment variable {name} is not set.", file=sys.stderr)
        sys.exit(2)
    return value.strip()


def build_google_ads_client():
    """Return a configured GoogleAdsClient instance."""
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        print(
            "ERROR: google-ads package not installed. Run: pip install google-ads>=24.0.0",
            file=sys.stderr,
        )
        sys.exit(2)

    config = {
        "developer_token": _require_env("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": _require_env("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": _require_env("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": _require_env("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True,
    }
    login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or os.getenv(
        "GOOGLE_ADS_CUSTOMER_ID"
    )
    if login_customer_id:
        config["login_customer_id"] = login_customer_id.replace("-", "")

    return GoogleAdsClient.load_from_dict(config)


# ---------------------------------------------------------------------------
# Keyword Planner call
# ---------------------------------------------------------------------------

# Geo targets: common location IDs
# https://developers.google.com/google-ads/api/data/geotargets
LOCATION_IDS: dict[str, int] = {
    "us": 2840,
    "gb": 2826,
    "pl": 2616,
    "de": 2276,
    "fr": 2250,
    "au": 2036,
    "ca": 2124,
    "cz": 2203,
    "it": 2380,
}

# Language IDs
# https://developers.google.com/google-ads/api/data/codes-formats#languages
LANGUAGE_IDS: dict[str, int] = {
    "en": 1000,
    "pl": 1030,
    "de": 1001,
    "fr": 1002,
    "es": 1003,
    "it": 1004,
    "nl": 1010,
    "pt": 1014,
    "cs": 1021,
}


def get_keyword_volumes(
    keywords: list[str],
    customer_id: str,
    language: str = "en",
    location: int | str = 2840,
) -> list[dict]:
    """
    Query Keyword Planner for historical metrics on the given keywords.

    Args:
        keywords:    List of keyword phrases to look up (max 20 per call is safe).
        customer_id: Google Ads customer ID (digits only, no dashes).
        language:    Two-letter language code or a language resource name.
        location:    Geo target location ID (int) or two-letter country code (str).

    Returns:
        List of dicts sorted by avg_monthly_searches descending:
        [{"keyword": str, "avg_monthly_searches": int, "competition": str}, ...]
    """
    client = build_google_ads_client()
    kp_service = client.get_service("KeywordPlanIdeaService")

    # Resolve location
    if isinstance(location, str):
        if location.isdigit():
            location_id = int(location)
        else:
            location_id = LOCATION_IDS.get(location.lower())
            if location_id is None:
                print(
                    f"WARNING: unknown location code '{location}', defaulting to US (2840).",
                    file=sys.stderr,
                )
                location_id = 2840
    else:
        location_id = int(location)

    # Resolve language
    if isinstance(language, str) and not language.startswith("languageConstants/"):
        lang_id = LANGUAGE_IDS.get(language.lower(), 1000)
        language_resource = f"languageConstants/{lang_id}"
    else:
        language_resource = language

    location_resource = f"geoTargetConstants/{location_id}"

    request = client.get_type("GenerateKeywordIdeasRequest")
    request.customer_id = customer_id.replace("-", "")
    request.language = language_resource
    request.geo_target_constants.append(location_resource)
    request.include_adult_keywords = False
    request.keyword_seed.keywords.extend(keywords)

    try:
        response = kp_service.generate_keyword_ideas(request=request)
    except Exception as exc:
        print(f"ERROR: Google Ads API call failed: {exc}", file=sys.stderr)
        sys.exit(1)

    results = []
    requested = {k.lower().strip() for k in keywords}
    for idea in response:
        kw_text = idea.text.lower().strip()
        if kw_text not in requested:
            continue
        metrics = idea.keyword_idea_metrics
        competition_enum = metrics.competition
        competition_name = (
            competition_enum.name if hasattr(competition_enum, "name") else str(competition_enum)
        )
        results.append(
            {
                "keyword": kw_text,
                "avg_monthly_searches": metrics.avg_monthly_searches,
                "competition": competition_name,
            }
        )

    # Sort highest volume first
    results.sort(key=lambda x: x["avg_monthly_searches"], reverse=True)

    # Pad with zero-volume entries for any keywords the API didn't return
    returned = {r["keyword"] for r in results}
    for kw in keywords:
        if kw.lower().strip() not in returned:
            results.append(
                {
                    "keyword": kw.lower().strip(),
                    "avg_monthly_searches": 0,
                    "competition": "UNKNOWN",
                }
            )

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Look up keyword search volumes via Google Ads Keyword Planner."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--keywords",
        nargs="+",
        metavar="KEYWORD",
        help="One or more keyword phrases to look up.",
    )
    group.add_argument(
        "--file",
        metavar="PATH",
        help="Path to a plain-text file with one keyword per line.",
    )
    parser.add_argument(
        "--language",
        default="en",
        help="Two-letter language code (en, pl, de, fr, …). Default: en.",
    )
    parser.add_argument(
        "--location",
        default="2840",
        help="Geo target: two-letter country code (us, gb, pl, …) or numeric ID. Default: 2840 (US).",
    )
    parser.add_argument(
        "--customer-id",
        default=None,
        help="Google Ads customer ID (overrides GOOGLE_ADS_CUSTOMER_ID env var).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"ERROR: file not found: {path}", file=sys.stderr)
            sys.exit(2)
        keywords = [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        keywords = args.keywords

    if not keywords:
        print("ERROR: no keywords provided.", file=sys.stderr)
        sys.exit(2)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_keywords: list[str] = []
    for kw in keywords:
        lower = kw.lower().strip()
        if lower not in seen:
            seen.add(lower)
            unique_keywords.append(kw.strip())

    customer_id = args.customer_id or _require_env("GOOGLE_ADS_CUSTOMER_ID")

    results = get_keyword_volumes(
        keywords=unique_keywords,
        customer_id=customer_id,
        language=args.language,
        location=args.location,
    )

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
