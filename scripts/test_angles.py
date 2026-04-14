"""
test_angles.py — dry-run angle assignment for all products in the feed.

Runs keyword_planner.py for each unique product, splits results into
symptom / ingredient lists, and prints the proposed title angle per variant.

No Anthropic API calls — pure keyword data + local logic.

Usage:
    python scripts/test_angles.py
"""

import io
import json
import subprocess
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Candidate phrases per product
# Each product has two lists:
#   symptoms   — ways a buyer describes the problem they want solved
#   ingredients — key actives / components buyers search by name
# ---------------------------------------------------------------------------

PRODUCTS = {
    "Aqua Cleanse": {
        "variants": ["29515_g", "29515-2_g", "29515-3_g", "29515-6_g"],
        "symptoms": [
            "zatrzymywanie wody w organizmie",
            "jak pozbyć się wzdęć",
            "obrzęki nóg i rąk",
            "wzdęcia brzucha",
            "oczyszczanie organizmu z toksyn",
            "uczucie ciężkości w brzuchu",
        ],
        "ingredients": [
            "napój detox w proszku",
            "herbata detox na odchudzanie",
            "detoks oczyszczający organizm",
            "suplement odwadniający",
        ],
    },
    "BellyTox": {
        "variants": ["29544_g", "29544-2_g", "29544-3_g", "29544-6_g"],
        "symptoms": [
            "toksyny w organizmie objawy",
            "przyrost wagi mimo diety",
            "spowolniony metabolizm",
            "problemy z trawieniem i wzdęcia",
            "uczucie zatrucia organizmu",
            "jak przyspieszyć metabolizm",
        ],
        "ingredients": [
            "suplement detoksykujący organizm",
            "kapsułki na odchudzanie i detoks",
            "ziołowy suplement oczyszczający",
            "tabletki przyspieszające trawienie",
        ],
    },
    "Complete Biotics": {
        "variants": ["17209_g", "17209-2_g", "17209-3_g", "17209-6_g"],
        "symptoms": [
            "zaburzenia flory jelitowej",
            "osłabiona odporność przyczyny",
            "nieregularne wypróżnienia",
            "wzdęcia po jedzeniu",
            "biegunka i zaparcia naprzemiennie",
            "dysbioza jelitowa",
        ],
        "ingredients": [
            "probiotyki na jelita",
            "kultury bakterii probiotycznych",
            "probiotyk wieloszczepowy",
            "probiotyki na odporność",
        ],
    },
    "Curcuma Cleanse": {
        "variants": ["29456_g", "29456-2_g", "29456-3_g", "29456-6_g"],
        "symptoms": [
            "zdrowie wątroby suplementy",
            "oczyszczanie wątroby naturalnie",
            "odkładanie tłuszczu w wątrobie",
            "wolne rodniki w organizmie",
            "stłuszczenie wątroby dieta",
            "wspieranie funkcji wątroby",
        ],
        "ingredients": [
            "kurkuma suplement",
            "kurkumina kapsułki",
            "ekstrakt z kurkumy na wątrobę",
            "kurkuma właściwości zdrowotne",
        ],
    },
    "Gut Relief": {
        "variants": ["29729_g", "29729-2_g", "29729-3_g", "29729-6_g"],
        "symptoms": [
            "wzdęcia i bóle brzucha",
            "zaburzenia mikrobiomu jelitowego",
            "dyskomfort po jedzeniu",
            "gazy i wzdęcia po posiłku",
            "nieszczelność jelit objawy",
            "bóle brzucha po jedzeniu",
        ],
        "ingredients": [
            "suplement na mikrobiom jelitowy",
            "probiotyki i prebiotyki razem",
            "synbiotyk na jelita",
            "suplement na florę jelitową",
        ],
    },
    "Gut Restore": {
        "variants": ["17213_g", "17213-2_g", "17213-3_g", "17213-6_g"],
        "symptoms": [
            "dyskomfort żołądkowo jelitowy",
            "zaparcia i nieregularne trawienie",
            "zła flora jelitowa objawy",
            "wzdęcia i gazy jelitowe",
            "bóle jelit i wzdęcia",
            "jak poprawić trawienie naturalnie",
        ],
        "ingredients": [
            "prebiotyki na jelita",
            "błonnik prebiotyczny suplement",
            "inulina i FOS suplement",
            "prebiotyk kapsułki",
        ],
    },
    "Pure Cleanse": {
        "variants": ["29293_g", "29293-2_g", "29293-3_g", "29293-6_g"],
        "symptoms": [
            "oczyszczanie organizmu z toksyn",
            "detoks po alkoholu",
            "ochrona przed toksynami środowiskowymi",
            "zmęczenie i brak energii toksyny",
            "jak oczyścić organizm",
            "nagromadzenie toksyn w organizmie",
        ],
        "ingredients": [
            "suplement oczyszczający organizm",
            "kapsułki detoksykujące",
            "tabletki na oczyszczanie organizmu",
            "detoks kapsułki 5 składników",
        ],
    },
}

VARIANT_LABEL = {
    "_g": "single",
    "-2_g": "2-pack",
    "-3_g": "3-pack",
    "-6_g": "6-pack",
}


def variant_label(variant_id: str) -> str:
    for suffix, label in VARIANT_LABEL.items():
        if variant_id.endswith(suffix):
            return label
    return variant_id


def run_planner(keywords: list[str]) -> list[dict]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "keyword_planner.py"),
        "--keywords", *keywords,
        "--language", "pl",
        "--location", "pl",
    ]
    result = subprocess.run(cmd, capture_output=True, cwd=ROOT)
    if result.returncode != 0:
        print(f"  [WARN] keyword_planner.py failed: {result.stderr.decode('utf-8', errors='replace').strip()}", file=sys.stderr)
        return []
    return json.loads(result.stdout.decode("utf-8"))


def rank_list(planner_results: list[dict], candidates: list[str]) -> list[dict]:
    """Return planner results that match candidates, in volume order."""
    candidate_set = {c.lower().strip() for c in candidates}
    ranked = [r for r in planner_results if r["keyword"] in candidate_set]
    # Append zero-volume candidates that weren't returned by API
    returned = {r["keyword"] for r in ranked}
    for c in candidates:
        if c.lower().strip() not in returned:
            ranked.append({"keyword": c.lower().strip(), "avg_monthly_searches": 0, "competition": "UNKNOWN"})
    return ranked


def competition_sort_key(item: dict) -> tuple:
    order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "UNSPECIFIED": 3, "UNKNOWN": 4}
    return (-item["avg_monthly_searches"], order.get(item["competition"], 9))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

print("=" * 70)
print("KEYWORD PLANNER — ANGLE ASSIGNMENT TEST")
print("Language: pl  |  Location: PL")
print("=" * 70)

for product_name, data in PRODUCTS.items():
    all_candidates = data["symptoms"] + data["ingredients"]
    print(f"\n{'─' * 70}")
    print(f"  {product_name}  ({len(data['variants'])} variants)")
    print(f"  Querying {len(all_candidates)} candidates...")

    results = run_planner(all_candidates)

    ranked_symptoms = sorted(rank_list(results, data["symptoms"]), key=competition_sort_key)
    ranked_ingredients = sorted(rank_list(results, data["ingredients"]), key=competition_sort_key)

    print(f"\n  SYMPTOMS (ranked):")
    for i, r in enumerate(ranked_symptoms, 1):
        vol = r["avg_monthly_searches"]
        comp = r["competition"]
        print(f"    {i}. {vol:>6}  [{comp:12}]  {r['keyword']}")

    print(f"\n  INGREDIENTS (ranked):")
    for i, r in enumerate(ranked_ingredients, 1):
        vol = r["avg_monthly_searches"]
        comp = r["competition"]
        print(f"    {i}. {vol:>6}  [{comp:12}]  {r['keyword']}")

    print(f"\n  ANGLE ASSIGNMENT:")
    for i, variant_id in enumerate(data["variants"]):
        label = variant_label(variant_id)
        sym = ranked_symptoms[i] if i < len(ranked_symptoms) else ranked_symptoms[-1]
        ing = ranked_ingredients[i] if i < len(ranked_ingredients) else ranked_ingredients[0]
        print(f"    {label:<8}  symptom: \"{sym['keyword']}\" ({sym['avg_monthly_searches']}/mo)")
        print(f"             ingredient: \"{ing['keyword']}\" ({ing['avg_monthly_searches']}/mo)")

print(f"\n{'=' * 70}")
print("Done.")
