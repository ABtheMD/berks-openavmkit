#!/usr/bin/env python3
"""
Step 3 of the openavmkit pipeline setup: fill in the settings.json sections that
generate_settings.py leaves blank.

Specifically generates:
  - data.load.<source>.load  — maps raw column names → canonical names for every
    source file (parcel id → key, field_classification fields → themselves, sale
    fields → sale_price / sale_date)
  - data.process.merge        — universe and sales merge lists
  - data.load.<sales>.calc   — key_sale, valid_sale, vacant_sale boolean columns

Also handles a one-time parquet fix:
  --patch-dates               Convert any float64 Unix-ms timestamp columns that
                              the ArcGIS downloader left unconverted to datetime64.
                              Run this once if you downloaded data with an older
                              version of download_data.py (before date conversion
                              was added).

Usage:
  python scripts/configure_settings.py seeds/seed_us-pa-berks.json
  python scripts/configure_settings.py seeds/seed_us-pa-berks.json --dry-run
  python scripts/configure_settings.py seeds/seed_us-pa-berks.json --in-dir path/to/in/
  python scripts/configure_settings.py seeds/seed_us-pa-berks.json --patch-dates
"""

import argparse
import json
import os
import sys

import pyarrow.parquet as pq


# ── Column name heuristics ──────────────────────────────────────────────────

PARCEL_ID_PATTERNS = [
    "parid", "propid", "pin", "parcel_id", "parcelnumber", "parcelno",
    "apn", "gis_pin", "locpin", "accountno", "parcel_num",
]

SALE_PRICE_PATTERNS = [
    "price", "saleprice", "sale_price", "saleamt", "saleamount",
    "grantamt", "deed_amount", "deedamount", "granteeprice",
]

SALE_DATE_PATTERNS = [
    "saledt", "sale_date", "saledate", "deeddate", "deed_date",
    "conveyance_date", "dateofsale", "salesdate", "sale_dt",
]

GEO_ROLE = "geo_parcels"

VALID_SALE_MAX = 50_000_000  # $50M upper cap for valid sales

# Values larger than this in a numeric column strongly suggest Unix ms (not seconds)
_MS_THRESHOLD = 1e10  # ~317 years in seconds; any real date > 1970 in ms exceeds this


# ── Helpers ─────────────────────────────────────────────────────────────────

def detect_column(columns: list[str], patterns: list[str]) -> str | None:
    """Return the first column that matches any pattern (case-insensitive exact)."""
    lower = {c.lower(): c for c in columns}
    for p in patterns:
        if p in lower:
            return lower[p]
    return None


def get_classified_fields(settings: dict) -> set[str]:
    """All field names listed anywhere in field_classification."""
    fc = settings.get("field_classification", {})
    fields = set()
    for group in fc.values():
        if not isinstance(group, dict):
            continue
        for kind, field_list in group.items():
            if isinstance(field_list, list):
                fields.update(field_list)
    return fields


def patch_date_columns(in_dir: str, sources: list[dict]) -> None:
    """
    One-time fix for parquets downloaded before date conversion was added to
    download_data.py.  Any numeric (float/int) column whose non-null values
    exceed _MS_THRESHOLD is treated as a Unix millisecond timestamp and
    converted to datetime64[ns], then the parquet is overwritten.

    Safe to run multiple times — columns already in datetime64 are left alone.
    """
    import pandas as pd
    import geopandas as gpd

    for source in sources:
        handle = source["handle"]
        path = os.path.join(in_dir, f"{handle}.parquet")
        if not os.path.exists(path):
            continue

        schema = pq.read_schema(path)
        candidate_cols = []
        for i, field in enumerate(schema):
            import pyarrow as pa
            if pa.types.is_floating(field.type) or pa.types.is_integer(field.type):
                candidate_cols.append(field.name)

        if not candidate_cols:
            continue

        is_geo = "geometry" in schema.names
        df = gpd.read_parquet(path) if is_geo else pd.read_parquet(path)

        patched = []
        for col in candidate_cols:
            if col not in df.columns:
                continue
            sample = df[col].dropna()
            if sample.empty:
                continue
            if (sample.abs() > _MS_THRESHOLD).any():
                df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
                patched.append(col)

        if patched:
            df.to_parquet(path, index=False)
            print(f"  {handle}: patched {len(patched)} date column(s): {patched}")
        else:
            print(f"  {handle}: no date columns needed patching")


def load_parquet_schemas(in_dir: str, sources: list[dict]) -> dict[str, list[str]]:
    """Map handle → list of column names (from parquet schema, no data loaded)."""
    schemas = {}
    for source in sources:
        handle = source["handle"]
        path = os.path.join(in_dir, f"{handle}.parquet")
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found — skipping {handle}")
            continue
        schemas[handle] = pq.read_schema(path).names
    return schemas


def build_load_map(
    columns: list[str],
    classified: set[str],
    parcel_id_col: str,
    already_claimed: set[str],
    sale_price_col: str | None = None,
    sale_date_col: str | None = None,
) -> dict[str, str]:
    """
    Build the load mapping for one source.

    Rules:
    - parcel_id_col → "key" (always)
    - sale_price_col → "sale_price", sale_date_col → "sale_date" (sales source only)
    - Every column that appears in field_classification and is NOT already claimed
      by an earlier source is mapped to itself (raw_name → raw_name)
    - 'already_claimed' is updated in-place so each field is loaded from at most
      one source (avoids suffixed duplicates in the merge)
    """
    load = {}

    if parcel_id_col:
        load["key"] = parcel_id_col

    if sale_price_col:
        load["sale_price"] = sale_price_col
    if sale_date_col:
        load["sale_date"] = sale_date_col

    for col in columns:
        if col == parcel_id_col:
            continue  # already mapped to key
        if col in (sale_price_col, sale_date_col):
            continue  # already mapped to canonical sale fields
        if col in classified and col not in already_claimed:
            load[col] = col
            already_claimed.add(col)

    return load


def detect_sales_source(
    sources: list[dict],
    schemas: dict[str, list[str]],
) -> tuple[str, str, str] | None:
    """
    Find the source most likely to contain sales data.
    Returns (handle, price_col, date_col) or None.
    Prefers sources whose role contains 'master' or 'sales'.
    """
    candidates = []
    for source in sources:
        handle = source["handle"]
        role = source.get("role", "")
        if handle not in schemas:
            continue
        if role == GEO_ROLE:
            continue
        cols = schemas[handle]
        price_col = detect_column(cols, SALE_PRICE_PATTERNS)
        date_col = detect_column(cols, SALE_DATE_PATTERNS)
        if price_col and date_col:
            # Score: prefer 'master' or 'sales' roles
            score = 2 if ("master" in role or "sales" in role) else 1
            candidates.append((score, handle, price_col, date_col))

    if not candidates:
        return None
    candidates.sort(reverse=True)
    _, handle, price_col, date_col = candidates[0]
    return handle, price_col, date_col


def build_sales_calcs(sale_price_canonical: str = "sale_price") -> dict:
    """
    Calc operations added to the sales source's load entry.

    key_sale  = str(key) + "_" + str(sale_date)   — unique sale transaction id
    valid_sale = sale_price > 0 AND sale_price < VALID_SALE_MAX
    vacant_sale = sale_price < 0                   — always False for normal data;
                  user should refine if vacancy data is available
    """
    return {
        "key_sale": [
            "+",
            ["asstr", "key"],
            ["+", "str:_", ["asstr", "sale_date"]],
        ],
        "valid_sale": [
            "?",
            ["and",
             [">", sale_price_canonical, 0],
             ["<", sale_price_canonical, VALID_SALE_MAX]],
        ],
        "vacant_sale": [
            "?", ["<", sale_price_canonical, 0]
        ],
    }


def build_merge(
    sources: list[dict],
    schemas: dict[str, list[str]],
    geo_handle: str,
    sales_handle: str,
) -> dict:
    """
    Build data.process.merge.universe and data.process.merge.sales.

    Universe: geo_parcels base + left-join each other source on 'key'.
    Sales:    sales_handle only, joined on 'key_sale'.
    """
    universe = [geo_handle]
    for source in sources:
        handle = source["handle"]
        if handle == geo_handle or handle not in schemas:
            continue
        universe.append({"id": handle, "how": "left", "on": "key"})

    sales = [{"id": sales_handle, "how": "left", "on": "key_sale"}]

    return {"universe": universe, "sales": sales}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fill in data.load and data.process.merge in a generated settings.json."
    )
    parser.add_argument("seed_file", help="Path to the seed JSON file (e.g. seeds/seed_us-pa-berks.json)")
    parser.add_argument(
        "--in-dir",
        help="Directory containing settings.json and *.parquet files "
             "(default: notebooks/pipeline/data/{slug}/in/)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument(
        "--patch-dates",
        action="store_true",
        help="Convert any float Unix-ms timestamp columns in existing parquets to datetime64. "
             "Run once if data was downloaded with an older version of download_data.py.",
    )
    args = parser.parse_args()

    # ── Load seed ──────────────────────────────────────────────────────────
    if not os.path.exists(args.seed_file):
        print(f"ERROR: seed file not found: {args.seed_file}", file=sys.stderr)
        sys.exit(1)

    with open(args.seed_file) as f:
        seed = json.load(f)

    slug = seed["locality"]["slug"]
    sources = seed.get("sources", [])
    print(f"Seed: {slug}  ({len(sources)} sources)")

    # ── Locate in/ directory and settings.json ─────────────────────────────
    in_dir = args.in_dir or os.path.join("notebooks", "pipeline", "data", slug, "in")
    settings_path = os.path.join(in_dir, "settings.json")

    if not os.path.exists(settings_path):
        print(f"ERROR: settings.json not found at {settings_path}", file=sys.stderr)
        print("Run generate_settings.py first.", file=sys.stderr)
        sys.exit(1)

    with open(settings_path) as f:
        settings = json.load(f)

    print(f"Settings: {settings_path}")

    # ── Patch date columns if requested ───────────────────────────────────
    if args.patch_dates:
        if args.dry_run:
            print("\n[dry-run] Would patch Unix-ms date columns in parquet files (skipped)")
        else:
            print("\nPatching Unix-ms date columns in parquet files...")
            patch_date_columns(in_dir, sources)

    # ── Load parquet schemas ───────────────────────────────────────────────
    print("\nReading parquet schemas...")
    schemas = load_parquet_schemas(in_dir, sources)
    for handle, cols in schemas.items():
        print(f"  {handle}: {len(cols)} columns")

    if not schemas:
        print("ERROR: no parquet files found in", in_dir, file=sys.stderr)
        sys.exit(1)

    # ── Collect field_classification fields ────────────────────────────────
    classified = get_classified_fields(settings)
    print(f"\nField classification: {len(classified)} fields to load")

    # ── Find geo and sales sources ─────────────────────────────────────────
    geo_handle = next(
        (s["handle"] for s in sources if s.get("role") == GEO_ROLE and s["handle"] in schemas),
        None,
    )
    if not geo_handle:
        print("WARNING: no geo_parcels source found — universe merge will be incomplete")

    sales_info = detect_sales_source(sources, schemas)
    if sales_info:
        sales_handle, price_col, date_col = sales_info
        print(f"\nSales source: {sales_handle}  (price={price_col}, date={date_col})")
    else:
        sales_handle = None
        print("\nWARNING: could not detect a sales source (no price+date column pair found)")

    # ── Build load mappings ────────────────────────────────────────────────
    print("\nBuilding load mappings...")
    already_claimed: set[str] = set()

    # Process geo_parcels first so it wins conflicts
    ordered_sources = sorted(
        [s for s in sources if s["handle"] in schemas],
        key=lambda s: (0 if s.get("role") == GEO_ROLE else 1, s["handle"]),
    )

    for source in ordered_sources:
        handle = source["handle"]
        cols = schemas[handle]
        is_sales = (handle == sales_handle)

        parcel_id_col = detect_column(cols, PARCEL_ID_PATTERNS)
        if not parcel_id_col:
            print(f"  WARNING: {handle} — could not detect parcel ID column")

        load_map = build_load_map(
            columns=cols,
            classified=classified,
            parcel_id_col=parcel_id_col,
            already_claimed=already_claimed,
            sale_price_col=price_col if is_sales else None,
            sale_date_col=date_col if is_sales else None,
        )

        entry = settings["data"]["load"].get(handle, {})
        entry["load"] = load_map
        if source.get("role") == GEO_ROLE:
            entry["dupes"] = {"subset": ["key"], "sort_by": ["key", "asc"], "drop": True}
        elif is_sales:
            entry["dupes"] = {"subset": ["key_sale"], "sort_by": ["key_sale", "asc"], "drop": True}
        settings["data"]["load"][handle] = entry

        print(f"  {handle}: {len(load_map)} fields mapped  (key={parcel_id_col})")

        if is_sales:
            calcs = build_sales_calcs("sale_price")
            entry["calc"] = calcs
            print(f"    + calc: key_sale, valid_sale, vacant_sale")

    # ── Build data.process.merge ───────────────────────────────────────────
    if geo_handle and sales_handle:
        print("\nBuilding data.process.merge...")
        merge = build_merge(sources, schemas, geo_handle, sales_handle)

        if "process" not in settings["data"]:
            settings["data"]["process"] = {}
        settings["data"]["process"]["merge"] = merge

        univ_len = len(merge["universe"])
        print(f"  universe: {univ_len} sources  ({merge['universe'][0]} + {univ_len-1} left-joins)")
        print(f"  sales:    {merge['sales'][0]['id']} on key_sale")
    else:
        print("\nWARNING: skipping data.process.merge (missing geo or sales source)")

    # ── Report fields not found in any parquet ─────────────────────────────
    all_parquet_cols: set[str] = set()
    for cols in schemas.values():
        all_parquet_cols.update(cols)
    missing = sorted(classified - all_parquet_cols)
    if missing:
        print(f"\nFields in field_classification NOT found in any parquet ({len(missing)}):")
        for f in missing:
            print(f"  {f}")
        print("  -> These fields will be silently skipped at load time (not an error).")

    # ── Write / print ──────────────────────────────────────────────────────
    if args.dry_run:
        print("\n--- dry run: data section ---")
        print(json.dumps(settings["data"], indent=2))
    else:
        with open(settings_path, "w") as f:
            json.dump(settings, f, indent=2)
        print(f"\nPatched {settings_path}")
        print("Run 01-assemble.ipynb (or _run_assemble.py) to validate.")


if __name__ == "__main__":
    main()
