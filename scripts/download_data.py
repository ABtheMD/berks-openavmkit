#!/usr/bin/env python3
"""
download_data.py

Downloads parquet / geoparquet files from the ArcGIS Feature Server
sources listed in a seed file, and saves them to the pipeline's input
directory so the openavmkit notebook pipeline can run.

NOTE: This script downloads actual data records — it may take several
minutes for large layers.  The settings generator (generate_settings.py)
only touches field metadata; this script downloads the rows.

Geometry is detected automatically from the ArcGIS layer metadata
(geometryType field) rather than inferred from the source role.  This
means flat files with point geometry (e.g. Philadelphia OPA) are saved
as GeoParquet correctly, not as plain tabular parquet.

After downloading, the script also patches any settings.json found in
the output directory to:
  - add  "geometry": true  for any source whose layer has geometry
  - rename  "dtypes": {}   →  "load": {}  (pipeline key name)

Usage:
    python scripts/download_data.py seeds/seed_us-pa-berks.json
    python scripts/download_data.py seeds/seed_us-pa-berks.json --out-dir path/to/in/
    python scripts/download_data.py seeds/seed_us-pa-berks.json --source geo_parcels
    python scripts/download_data.py seeds/seed_us-pa-berks.json --page-size 2000
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Error: 'requests' is required. Run: pip install requests")

try:
    import pandas as pd
except ImportError:
    sys.exit("Error: 'pandas' is required. Run: pip install pandas")

try:
    import geopandas as gpd
except ImportError:
    sys.exit("Error: 'geopandas' is required. Run: pip install geopandas")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default records-per-page for ArcGIS pagination
DEFAULT_PAGE_SIZE = 1000

# Resolved path to this script's directory
_SCRIPT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# ArcGIS helpers
# ---------------------------------------------------------------------------

def get_layer_metadata(url: str) -> dict:
    """
    Fetch ArcGIS layer metadata JSON.  Returns {} on failure.
    """
    meta_url = url.rstrip("/") + "?f=json"
    try:
        resp = requests.get(meta_url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"    [!] Could not fetch layer metadata: {exc}", file=sys.stderr)
        return {}


def get_date_field_names(url: str) -> set[str]:
    """
    Return the lowercased names of all esriFieldTypeDate fields in the layer.
    ArcGIS returns Date fields as Unix millisecond timestamps (float/integer).
    """
    meta = get_layer_metadata(url)
    date_fields = set()
    for field in meta.get("fields", []):
        if field.get("type") == "esriFieldTypeDate":
            date_fields.add(field["name"].lower())
    return date_fields


def convert_date_columns(df: "pd.DataFrame", date_cols: set[str]) -> "pd.DataFrame":
    """
    Convert ArcGIS Unix-millisecond timestamp columns to datetime64[ns].

    ArcGIS stores Date fields as integer/float milliseconds since the Unix
    epoch.  Saving these as-is produces float64 parquet columns that the
    openavmkit pipeline cannot parse without special unit handling.
    Converting here means downstream consumers see proper datetime64 columns.
    """
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
    return df


def get_record_count(url: str) -> int:
    """
    Ask ArcGIS how many records exist in the layer.
    Returns 0 if the count cannot be retrieved.
    """
    query_url = url.rstrip("/") + "/query"
    params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
    try:
        resp = requests.get(query_url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("count", 0)
    except Exception as exc:
        print(f"    [!] Could not get record count: {exc}", file=sys.stderr)
        return 0


def fetch_geo_page(url: str, offset: int, page_size: int) -> list[dict]:
    """
    Fetch one page of features as GeoJSON feature dicts.
    Requests coordinates in WGS84 (EPSG:4326) via outSR=4326.
    """
    query_url = url.rstrip("/") + "/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "geojson",
    }
    resp = requests.get(query_url, params=params, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")
    return data.get("features", [])


def fetch_tabular_page(url: str, offset: int, page_size: int) -> list[dict]:
    """
    Fetch one page of records as plain attribute dicts (no geometry).
    """
    query_url = url.rstrip("/") + "/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "returnGeometry": "false",
        "f": "json",
    }
    resp = requests.get(query_url, params=params, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")
    return [feature["attributes"] for feature in data.get("features", [])]


# ---------------------------------------------------------------------------
# Download functions
# ---------------------------------------------------------------------------

def download_geo_source(url: str, handle: str, page_size: int) -> "gpd.GeoDataFrame":
    """
    Download all records from a geometry-bearing ArcGIS layer.
    Returns a GeoDataFrame in EPSG:4326 with lowercased column names.
    """
    total = get_record_count(url)
    print(f"    Total records : {total:,}", file=sys.stderr)

    all_features: list[dict] = []
    offset = 0

    while True:
        fetched = len(all_features)
        end = min(fetched + page_size, total) if total else fetched + page_size
        print(
            f"    Fetching {fetched + 1:>7,} – {end:>7,}"
            + (f" / {total:,}" if total else "")
            + " ...",
            file=sys.stderr,
        )
        page = fetch_geo_page(url, offset, page_size)
        if not page:
            break
        all_features.extend(page)
        offset += len(page)
        if total and offset >= total:
            break
        if len(page) < page_size:
            # Server returned fewer than requested — we've hit the end
            break

    print(f"    Downloaded    : {len(all_features):,} records", file=sys.stderr)

    gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326")

    # Lowercase all column names for consistency with openavmkit conventions
    gdf.columns = [col.lower() for col in gdf.columns]

    return gdf


def download_tabular_source(url: str, handle: str, page_size: int) -> "pd.DataFrame":
    """
    Download all records from a non-geometry ArcGIS layer.
    Returns a DataFrame with lowercased column names.
    """
    total = get_record_count(url)
    print(f"    Total records : {total:,}", file=sys.stderr)

    all_records: list[dict] = []
    offset = 0

    while True:
        fetched = len(all_records)
        end = min(fetched + page_size, total) if total else fetched + page_size
        print(
            f"    Fetching {fetched + 1:>7,} – {end:>7,}"
            + (f" / {total:,}" if total else "")
            + " ...",
            file=sys.stderr,
        )
        page = fetch_tabular_page(url, offset, page_size)
        if not page:
            break
        all_records.extend(page)
        offset += len(page)
        if total and offset >= total:
            break
        if len(page) < page_size:
            break

    print(f"    Downloaded    : {len(all_records):,} records", file=sys.stderr)

    df = pd.DataFrame(all_records)
    df.columns = [col.lower() for col in df.columns]
    return df


# ---------------------------------------------------------------------------
# Settings.json patcher
# ---------------------------------------------------------------------------

def patch_settings_json(settings_path: Path, geo_handles: set[str]) -> None:
    """
    Patch an existing settings.json after downloading:

    1. Rename  "dtypes": {}  →  "load": {}  in each data.load entry
       (openavmkit's loader uses the "load" key, not "dtypes")

    2. Add  "geometry": true  to any data.load entry whose handle is in
       geo_handles — the set of handles whose layers were found to carry
       geometry during the download step.

    geo_handles is determined by live ArcGIS metadata inspection, not by
    role name, so flat files with geometry are handled correctly.
    """
    if not settings_path.exists():
        print(
            f"\n  [~] No settings.json found at {settings_path} — skipping patch.\n"
            f"      Run generate_settings.py first, then copy the output here.",
            file=sys.stderr,
        )
        return

    with open(settings_path, encoding="utf-8") as fh:
        settings = json.load(fh)

    changed = False

    for handle, entry in settings.get("data", {}).get("load", {}).items():
        # 1. Fix dtypes → load
        if "dtypes" in entry:
            entry["load"] = entry.pop("dtypes")
            changed = True

        # 2. Add geometry flag
        if handle in geo_handles and entry.get("geometry") is not True:
            entry["geometry"] = True
            changed = True

    if changed:
        with open(settings_path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
        print(f"  [✓] Patched settings.json at {settings_path}", file=sys.stderr)
    else:
        print(f"  [~] settings.json already up to date — no changes needed.", file=sys.stderr)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download parquet/geoparquet data files from ArcGIS Feature Server sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/download_data.py seeds/seed_us-pa-berks.json
  python scripts/download_data.py seeds/seed_us-pa-berks.json --out-dir notebooks/pipeline/data/us-pa-berks/in
  python scripts/download_data.py seeds/seed_us-pa-berks.json --source geo_parcels
  python scripts/download_data.py seeds/seed_us-pa-berks.json --page-size 2000
""",
    )
    parser.add_argument("seed", help="Path to the seed JSON file")
    parser.add_argument(
        "--out-dir",
        help=(
            "Directory to write parquet files. "
            "Default: notebooks/pipeline/data/{slug}/in/"
        ),
    )
    parser.add_argument(
        "--source",
        help="Only download this source handle (e.g. geo_parcels). Downloads all if omitted.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Records per ArcGIS request page (default: {DEFAULT_PAGE_SIZE})",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Load seed
    seed_path = Path(args.seed)
    if not seed_path.exists():
        sys.exit(f"Error: seed file not found: {seed_path}")
    with open(seed_path, encoding="utf-8") as fh:
        seed = json.load(fh)

    slug    = seed["locality"]["slug"]
    name    = seed["locality"]["name"]
    sources = seed.get("sources", [])

    print(f"\n[✓] Seed loaded : {name} ({slug})", file=sys.stderr)
    print(f"    Sources     : {len(sources)}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Resolve output directory
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        repo_root = _SCRIPT_DIR.parent
        out_dir = repo_root / "notebooks" / "pipeline" / "data" / slug / "in"

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"    Output dir  : {out_dir}\n", file=sys.stderr)

    # ------------------------------------------------------------------
    # Filter to a single source if --source given
    if args.source:
        sources = [s for s in sources if s["handle"] == args.source]
        if not sources:
            sys.exit(f"Error: no source with handle '{args.source}' in seed.")

    # ------------------------------------------------------------------
    # Download each source
    failed: list[str] = []
    geo_handles: set[str] = set()  # handles whose layers have geometry

    for source in sources:
        handle      = source.get("handle", "")
        role        = source.get("role", "")
        url         = source.get("url", "")
        source_type = source.get("type", "")

        print(f"[→] {handle}  (role: {role})", file=sys.stderr)

        if source_type != "feature_server":
            print(
                f"    [~] type '{source_type}' is not 'feature_server' — skipping.",
                file=sys.stderr,
            )
            continue

        # Auto-detect geometry and date fields from ArcGIS layer metadata
        meta = get_layer_metadata(url)
        geometry_type = meta.get("geometryType") or None
        is_geo = geometry_type is not None
        if is_geo:
            print(f"    Geometry      : {geometry_type}", file=sys.stderr)
            geo_handles.add(handle)

        date_cols = {f["name"].lower() for f in meta.get("fields", [])
                     if f.get("type") == "esriFieldTypeDate"}
        if date_cols:
            print(f"    Date fields   : {', '.join(sorted(date_cols))}", file=sys.stderr)

        out_file = out_dir / f"{handle}.parquet"

        try:
            if is_geo:
                gdf = download_geo_source(url, handle, args.page_size)
                gdf = convert_date_columns(gdf, date_cols)
                gdf.to_parquet(out_file, index=False)
                print(
                    f"    [✓] Saved GeoParquet → {out_file}\n"
                    f"        Rows: {len(gdf):,}  |  Columns: {len(gdf.columns)}  |  CRS: {gdf.crs}",
                    file=sys.stderr,
                )
            else:
                df = download_tabular_source(url, handle, args.page_size)
                df = convert_date_columns(df, date_cols)
                df.to_parquet(out_file, index=False)
                print(
                    f"    [✓] Saved Parquet → {out_file}\n"
                    f"        Rows: {len(df):,}  |  Columns: {len(df.columns)}",
                    file=sys.stderr,
                )
        except Exception as exc:
            print(f"    [!] FAILED — {exc}", file=sys.stderr)
            failed.append(handle)

        print("", file=sys.stderr)

    # ------------------------------------------------------------------
    # Patch settings.json if present
    print("[→] Checking for settings.json to patch...", file=sys.stderr)
    patch_settings_json(out_dir / "settings.json", geo_handles)

    # ------------------------------------------------------------------
    # Final summary
    succeeded = len(sources) - len(failed)
    print(
        f"\n[{'✓' if not failed else '!'}] Done  —  "
        f"{succeeded} source(s) saved, {len(failed)} failed.",
        file=sys.stderr,
    )
    if failed:
        print(f"    Failed sources: {', '.join(failed)}", file=sys.stderr)

    print(
        f"\n  Next steps:\n"
        f"    1. Copy your settings.json into {out_dir} if not already there\n"
        f"       (run: python scripts/generate_settings.py {seed_path})\n"
        f"    2. Fill in field_classification.important.fields and modeling_groups\n"
        f"    3. Open notebooks/pipeline/01-assemble.ipynb and run the pipeline\n",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
