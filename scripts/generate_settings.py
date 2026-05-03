#!/usr/bin/env python3
"""
generate_settings.py

Generates a settings.json file for openavmkit from a seed file.

For each Feature Server source listed in the seed, this script queries the
ArcGIS REST API to discover column names and types, then maps them to
openavmkit's canonical field names (via data_dictionary.json) and produces
a minimal, valid settings.json ready for review.

NOTE: This script only generates the settings file. It does NOT download any
data. The ArcGIS API is queried for column metadata only (field names/types),
not for actual data records.

Usage:
    python scripts/generate_settings.py seeds/seed_us-pa-berks.json
    python scripts/generate_settings.py seeds/seed_us-pa-berks.json --output in/settings.json
    python scripts/generate_settings.py seeds/seed_us-pa-berks.json --dry-run
"""

import argparse
import json
import sys
from difflib import get_close_matches
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Error: 'requests' is required. Run: pip install requests")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ArcGIS field type strings → openavmkit classification kind
ARCGIS_NUMERIC_TYPES = {
    "esriFieldTypeInteger",
    "esriFieldTypeSmallInteger",
    "esriFieldTypeDouble",
    "esriFieldTypeSingle",
    "esriFieldTypeFloat",
}
ARCGIS_STRING_TYPES = {"esriFieldTypeString"}

# These types carry no useful modelling information — skip them entirely
ARCGIS_SKIP_TYPES = {
    "esriFieldTypeOID",
    "esriFieldTypeGeometry",
    "esriFieldTypeBlob",
    "esriFieldTypeRaster",
    "esriFieldTypeGlobalID",
    "esriFieldTypeGUID",
    "esriFieldTypeDate",
}

# Source role → default land/impr/other group.
# None means "determine per-field from data_dictionary or fall back to other".
ROLE_TO_GROUP: dict[str, str | None] = {
    "geo_parcels":       "land",
    "cama_residential":  "impr",
    "cama_commercial":   "impr",
    "cama_master":       "other",
    "flat":              None,
}

# Fuzzy match confidence threshold (0.0–1.0; higher = stricter matching)
FUZZY_CUTOFF = 0.75

# Maximum dep_vars to suggest in models.default
MAX_DEP_VARS = 15

# Path to data_dictionary.json, relative to this script
_SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DICT_PATH = (
    _SCRIPT_DIR.parent / "openavmkit" / "resources" / "settings" / "data_dictionary.json"
)


# ---------------------------------------------------------------------------
# ArcGIS helpers
# ---------------------------------------------------------------------------

def fetch_arcgis_fields(url: str) -> list[dict]:
    """
    Query an ArcGIS Feature Server layer endpoint for its field schema.

    Appends ``?f=json`` to the URL and parses the ``fields`` array from the
    response.  Returns an empty list on any network or API error (with a
    warning printed to stderr).
    """
    meta_url = url.rstrip("/") + "?f=json"
    try:
        resp = requests.get(meta_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"    [!] Network error fetching {url}: {exc}", file=sys.stderr)
        return []

    try:
        data = resp.json()
    except ValueError:
        print(f"    [!] Could not parse JSON from {url}", file=sys.stderr)
        return []

    if "error" in data:
        msg = data["error"].get("message", data["error"])
        print(f"    [!] ArcGIS API error for {url}: {msg}", file=sys.stderr)
        return []

    return data.get("fields", [])


# ---------------------------------------------------------------------------
# Field classification helpers
# ---------------------------------------------------------------------------

def classify_field_kind(field: dict) -> str | None:
    """
    Map an ArcGIS field dict to 'numeric', 'categorical', 'boolean', or None.

    None means the field should be skipped entirely (geometry, OID, etc.).
    Boolean is inferred from common naming conventions on numeric fields.
    """
    ftype = field.get("type", "")
    fname = field.get("name", "").lower()

    if ftype in ARCGIS_SKIP_TYPES:
        return None

    if ftype in ARCGIS_NUMERIC_TYPES:
        # Treat numeric fields that follow boolean naming conventions as boolean
        bool_prefixes = ("is_", "has_", "flag_", "yn_")
        bool_suffixes = ("_flag", "_yn", "_ind", "_bool", "_indicator")
        if fname.startswith(bool_prefixes) or fname.endswith(bool_suffixes):
            return "boolean"
        return "numeric"

    if ftype in ARCGIS_STRING_TYPES:
        return "categorical"

    # Unknown type — skip rather than guess
    return None


def match_canonical(raw_name: str, alias: str, data_dict: dict) -> str | None:
    """
    Try to map a raw ArcGIS field name to a canonical data_dictionary key.

    Tries (in order):
      1. Exact match on lowercased raw name
      2. Exact match on alias converted to snake_case
      3. Fuzzy match on lowercased raw name  (cutoff: FUZZY_CUTOFF)
      4. Fuzzy match on alias snake_case     (cutoff: FUZZY_CUTOFF)

    Returns the canonical key string, or None if no match is found.
    """
    candidates = list(data_dict.keys())
    raw_lower = raw_name.lower()
    alias_snake = alias.lower().replace(" ", "_").replace("-", "_")

    # 1. Exact on raw
    if raw_lower in data_dict:
        return raw_lower

    # 2. Exact on alias
    if alias_snake in data_dict:
        return alias_snake

    # 3. Fuzzy on raw
    hits = get_close_matches(raw_lower, candidates, n=1, cutoff=FUZZY_CUTOFF)
    if hits:
        return hits[0]

    # 4. Fuzzy on alias
    hits = get_close_matches(alias_snake, candidates, n=1, cutoff=FUZZY_CUTOFF)
    if hits:
        return hits[0]

    return None


def infer_group_from_data_dict(canonical: str, data_dict: dict) -> str:
    """
    Look up the data_dictionary ``groups`` tag to decide land / impr / other.
    Falls back to 'other' if the canonical key has no useful group tag.
    """
    entry = data_dict.get(canonical, {})
    groups = entry.get("groups", [])
    if "land" in groups:
        return "land"
    if any(g in groups for g in ("improvement", "impr", "building")):
        return "impr"
    return "other"


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_field_classification(
    sources: list[dict],
    data_dict: dict,
) -> tuple[dict, dict, list, list]:
    """
    Fetch ArcGIS field metadata for every Feature Server source in *sources*,
    classify each field, and build the ``field_classification`` settings block.

    Returns
    -------
    classification : dict
        The populated field_classification block.
    matched : dict
        ``{raw_name_lower: canonical_name}`` for every matched field.
    unmatched : list
        Raw lowercased names of fields that had no match in data_dictionary.
    dep_var_candidates : list
        Canonical names of numeric land/impr fields — good dep_var suggestions.
    """
    classification: dict = {
        "land":  {"numeric": [], "categorical": [], "boolean": []},
        "impr":  {"numeric": [], "categorical": [], "boolean": []},
        "other": {"numeric": [], "categorical": [], "boolean": []},
        "important": {
            "__note": (
                "Map openavmkit's standard role names to your jurisdiction's "
                "actual column names. See the nc-guilford example for reference."
            ),
            "fields": {
                "impr_category":  "",
                "land_category":  "",
                "loc_neighborhood": "",
                "loc_market_area":  "",
                "loc_region":       "",
            },
            "locations": [],
        },
    }

    matched: dict[str, str] = {}
    unmatched: list[str] = []
    dep_var_candidates: list[str] = []

    # Track names already added to avoid duplicates across sources
    seen_raw: set[str] = set()
    seen_display: set[str] = set()

    for source in sources:
        role        = source.get("role", "")
        handle      = source.get("handle", "")
        url         = source.get("url", "")
        source_type = source.get("type", "")

        if source_type != "feature_server":
            print(
                f"  [~] {handle}: type '{source_type}' is not 'feature_server', skipping.",
                file=sys.stderr,
            )
            continue

        print(f"  [→] {handle} ({role})", file=sys.stderr)
        fields = fetch_arcgis_fields(url)
        print(f"      {len(fields)} fields returned", file=sys.stderr)

        default_group: str | None = ROLE_TO_GROUP.get(role)

        for field in fields:
            raw_name  = field.get("name", "")
            alias     = field.get("alias", raw_name)
            raw_lower = raw_name.lower()

            # Deduplicate across sources by raw name
            if raw_lower in seen_raw:
                continue
            seen_raw.add(raw_lower)

            kind = classify_field_kind(field)
            if kind is None:
                continue

            canonical    = match_canonical(raw_name, alias, data_dict)
            display_name = canonical if canonical else raw_lower

            # Deduplicate by display name (two raws → same canonical)
            if display_name in seen_display:
                continue
            seen_display.add(display_name)

            # Determine group: prefer role default, fall back to data_dict
            if default_group is not None:
                group = default_group
            elif canonical:
                group = infer_group_from_data_dict(canonical, data_dict)
            else:
                group = "other"

            classification[group][kind].append(display_name)

            if canonical:
                matched[raw_lower] = canonical
                if group in ("land", "impr") and kind == "numeric":
                    dep_var_candidates.append(canonical)
            else:
                unmatched.append(raw_lower)

    # Remove empty kind lists to keep output tidy
    for group in ("land", "impr", "other"):
        for kind in ("numeric", "categorical", "boolean"):
            if not classification[group][kind]:
                del classification[group][kind]

    return classification, matched, unmatched, dep_var_candidates


def build_settings(
    seed: dict,
    classification: dict,
    dep_var_candidates: list[str],
) -> dict:
    """
    Assemble the final settings.json structure from all collected pieces.

    Keys starting with ``__`` are treated as comments by openavmkit's loader
    and are stripped automatically at runtime — they are included here as
    guidance for whoever reviews and edits this file.
    """
    locality = seed["locality"]
    sources  = seed["sources"]

    # One load entry per source handle
    data_load = {
        source["handle"]: {
            "__note": (
                f"File produced by the data-download step for source '{source['handle']}'. "
                "Adjust the filename if your download pipeline uses a different name."
            ),
            "filename": f"{source['handle']}.parquet",
            "dtypes": {},
        }
        for source in sources
    }

    # Suggest up to MAX_DEP_VARS numeric land+impr fields as default dep_vars
    dep_vars = dep_var_candidates[:MAX_DEP_VARS]

    settings: dict = {
        "__generator": (
            "Generated by scripts/generate_settings.py. "
            "Review all sections — especially modeling_groups, "
            "field_classification.important, and dep_vars — before running the pipeline."
        ),
        "locality": locality,
        "data": {
            "load": data_load,
        },
        "modeling": {
            "metadata": {
                "__fill_in": "Replace the empty strings below before running the pipeline.",
                "modeler":       "",
                "modeler_nick":  "",
                "valuation_date": "",
                "use_sales_from":  2020,
                "test_sales_from": 2024,
            },
            "instructions": {
                "run": [
                    "assessor",
                    "mra",
                    "gwr",
                    "lightgbm",
                    "xgboost",
                    "local_naive_sqft",
                    "local_smart_sqft",
                ],
                "time_adjustment": {"period": "Q"},
                "ensemble":   [],
                "allocation": [],
            },
            "models": {
                "default": {
                    "__dep_vars_note": (
                        "These dep_vars were auto-suggested from matched numeric fields. "
                        "Review and trim to the fields that are actually meaningful predictors."
                    ),
                    "dep_vars": dep_vars,
                    "interactions": {"default": True},
                },
            },
            "__modeling_groups_note": (
                "Define your jurisdiction's property type groups. "
                "Each key is a group ID; 'name' is the human-readable label. "
                "Example: {\"residential_sf\": {\"name\": \"Residential single-family\"}}"
            ),
            "modeling_groups": {},
        },
        "analysis": {
            "ratio_study": {
                "look_back_years": 1,
                "breakdowns": [
                    {"by": "sale_price",              "quantiles": 10},
                    {"by": "bldg_area_finished_sqft",  "quantiles": 10},
                    {"by": "bldg_age_years",           "slice_size": 10},
                    {"by": "<loc_neighborhood>"},
                ],
            },
        },
        "field_classification": classification,
        "data_dictionary": {},
    }

    return settings


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a settings.json for openavmkit from a seed file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/generate_settings.py seeds/seed_us-pa-berks.json
  python scripts/generate_settings.py seeds/seed_us-pa-berks.json --output in/settings.json
  python scripts/generate_settings.py seeds/seed_us-pa-berks.json --dry-run
""",
    )
    parser.add_argument("seed", help="Path to the seed JSON file")
    parser.add_argument(
        "--output", default="in/settings.json",
        help="Output path for settings.json (default: in/settings.json)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the generated JSON to stdout instead of writing a file",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Load seed file
    seed_path = Path(args.seed)
    if not seed_path.exists():
        sys.exit(f"Error: seed file not found: {seed_path}")
    with open(seed_path, encoding="utf-8") as fh:
        seed = json.load(fh)

    locality_name = seed.get("locality", {}).get("name", seed_path.stem)
    print(f"\n[✓] Seed loaded        : {locality_name}", file=sys.stderr)
    print(f"    Sources            : {len(seed.get('sources', []))}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Load data dictionary
    if not DATA_DICT_PATH.exists():
        sys.exit(
            f"Error: data dictionary not found at {DATA_DICT_PATH}\n"
            "Make sure you are running this script from inside the berks-openavmkit repo."
        )
    with open(DATA_DICT_PATH, encoding="utf-8") as fh:
        data_dict = json.load(fh)
    print(f"[✓] Data dictionary    : {len(data_dict)} canonical fields", file=sys.stderr)

    # ------------------------------------------------------------------
    # Fetch ArcGIS field metadata and classify
    print(f"\n[→] Fetching field metadata from ArcGIS...", file=sys.stderr)
    sources = seed.get("sources", [])
    classification, matched, unmatched, dep_var_candidates = build_field_classification(
        sources, data_dict
    )

    # ------------------------------------------------------------------
    # Summary report
    total = len(matched) + len(unmatched)
    print(f"\n[✓] Fields processed   : {total}", file=sys.stderr)
    print(f"    Matched to canonical: {len(matched)}", file=sys.stderr)
    print(f"    Unmatched (kept raw): {len(unmatched)}", file=sys.stderr)
    if unmatched:
        preview = unmatched[:10]
        print("    Unmatched fields (first 10 shown):", file=sys.stderr)
        for name in preview:
            print(f"      - {name}", file=sys.stderr)
        if len(unmatched) > 10:
            print(f"      ... and {len(unmatched) - 10} more", file=sys.stderr)
    print(f"    dep_var suggestions : {len(dep_var_candidates[:MAX_DEP_VARS])}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Assemble settings dict
    settings = build_settings(seed, classification, dep_var_candidates)

    # ------------------------------------------------------------------
    # Write output
    output_json = json.dumps(settings, indent=2)

    if args.dry_run:
        print(output_json)
    else:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(output_json)
        print(f"\n[✓] Written to         : {out_path}", file=sys.stderr)

    print(
        "\n  Next steps:\n"
        "    1. Fill in  modeling.metadata.modeler, modeler_nick, valuation_date\n"
        "    2. Define   modeling.modeling_groups for your property types\n"
        "    3. Complete field_classification.important.fields with your column names\n"
        "    4. Review   models.default.dep_vars — keep only meaningful predictors\n"
        "    5. Verify   field_classification matches your actual downloaded data columns\n",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
