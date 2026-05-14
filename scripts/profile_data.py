"""
profile_data.py

Reads downloaded parquet files for a locality and computes a data profile
dict. This is pure computation -- no Claude calls, no network I/O.

The returned dict matches the schema in the harness design spec S5.1 and
is passed directly to claude_settings.generate_initial().
"""
from pathlib import Path
import pandas as pd

_REPO_ROOT = Path(__file__).parent.parent
DATA_BASE_DIR = _REPO_ROOT / "notebooks" / "pipeline" / "data"


def _simplify_dtype(dtype) -> str:
    """Map a pandas/numpy dtype to a simplified category string."""
    name = str(dtype).lower()
    if "int" in name:
        return "int"
    if "float" in name:
        return "float"
    if name in ("bool", "boolean"):
        return "bool"
    if name in ("object", "string") or "string" in name:
        return "string"
    return "other"


def build_data_profile(locality_slug: str, data_base_dir: Path = None) -> dict:
    if data_base_dir is None:
        data_base_dir = DATA_BASE_DIR

    in_dir = Path(data_base_dir) / locality_slug / "in"
    master_path = in_dir / "cama_master.parquet"
    geo_path = in_dir / "geo_parcels.parquet"

    if not master_path.exists():
        raise FileNotFoundError(
            f"Master parquet not found: {master_path}\n"
            f"Run the download stage first: python scripts/harness.py {locality_slug} --to download"
        )

    df_master = pd.read_parquet(master_path)
    df_geo = pd.read_parquet(geo_path) if geo_path.exists() else pd.DataFrame()

    df_class = df_master if "class" in df_master.columns else (
        df_geo if "class" in df_geo.columns else df_master
    )

    sale_col = next(
        (c for c in df_master.columns if c.lower() == "sale_price"), None
    ) or next(
        (c for c in df_master.columns if "sale" in c.lower() and "price" in c.lower()), None
    )

    class_distribution: dict = {}
    he_id_fill: dict = {}
    land_he_id_fill: dict = {}
    total_sales = 0

    if "class" in df_class.columns:
        for cls_val, group in df_class.groupby("class"):
            key = str(cls_val)
            sales = int(group[sale_col].notna().sum()) if sale_col else 0
            class_distribution[key] = {
                "parcels": len(group),
                "sales": sales,
            }
            total_sales += sales
            if "he_id" in group.columns:
                he_id_fill[key] = float(group["he_id"].notna().mean())
            if "land_he_id" in group.columns:
                land_he_id_fill[key] = float(group["land_he_id"].notna().mean())

    total_parcels = len(df_class)

    annual_sales_volume = total_sales
    year_col = next(
        (c for c in df_master.columns if "sale" in c.lower() and "year" in c.lower()), None
    )
    if year_col and sale_col:
        years = df_master[year_col].dropna()
        if len(years) > 0:
            year_range = max(1, int(years.max()) - int(years.min()))
            annual_sales_volume = max(1, total_sales // year_range)

    all_columns = list(df_master.columns)
    if not df_geo.empty:
        all_columns = sorted(set(all_columns) | set(df_geo.columns))

    return {
        "locality": locality_slug,
        "total_parcels": total_parcels,
        "total_sales": total_sales,
        "annual_sales_volume": annual_sales_volume,
        "class_distribution": class_distribution,
        "he_id_fill_rate_by_class": he_id_fill,
        "land_he_id_fill_rate_by_class": land_he_id_fill,
        "has_spatial_data": not df_geo.empty,
        "available_columns": all_columns,
        "jurisdiction_tier": infer_jurisdiction_tier(total_parcels, annual_sales_volume),
    }


def infer_jurisdiction_tier(total_parcels: int, annual_sales_volume: int) -> str:
    if total_parcels > 500_000 or annual_sales_volume > 50_000:
        return "very_large"
    if total_parcels >= 50_000:
        return "large_to_mid"
    return "rural_small"
