"""Tests for scripts/profile_data.py"""
import sys
from pathlib import Path
import pandas as pd
import pytest

# Add scripts/ to path so we can import profile_data directly
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from profile_data import build_data_profile, infer_jurisdiction_tier


@pytest.fixture
def tmp_locality(tmp_path):
    """Create a minimal fake locality with parquet files."""
    in_dir = tmp_path / "fake-county" / "in"
    in_dir.mkdir(parents=True)

    master = pd.DataFrame({
        "key":        ["A", "B", "C", "D", "E"],
        "class":      ["R", "R", "C", "R", "A"],
        "sale_price": [200_000, 250_000, None, 180_000, None],
        "he_id":      [1, 2, None, 3, None],
        "land_he_id": [10, 20, None, 30, None],
    })
    master.to_parquet(in_dir / "cama_master.parquet", index=False)

    geo = pd.DataFrame({"key": ["A", "B", "C", "D", "E"], "lat": [1, 2, 3, 4, 5]})
    geo.to_parquet(in_dir / "geo_parcels.parquet", index=False)

    return tmp_path


def test_tier_very_large_by_parcels():
    assert infer_jurisdiction_tier(600_000, 10_000) == "very_large"

def test_tier_very_large_by_sales():
    assert infer_jurisdiction_tier(40_000, 60_000) == "very_large"

def test_tier_large_to_mid():
    assert infer_jurisdiction_tier(169_000, 9_000) == "large_to_mid"

def test_tier_rural_small():
    assert infer_jurisdiction_tier(20_000, 1_000) == "rural_small"

def test_profile_returns_required_keys(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    for key in [
        "locality", "total_parcels", "total_sales", "annual_sales_volume",
        "class_distribution", "he_id_fill_rate_by_class",
        "has_spatial_data", "available_columns", "jurisdiction_tier",
    ]:
        assert key in profile, f"Missing key: {key}"

def test_profile_total_parcels(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["total_parcels"] == 5

def test_profile_class_distribution_keys(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert set(profile["class_distribution"].keys()) == {"R", "C", "A"}

def test_profile_res_parcel_count(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["class_distribution"]["R"]["parcels"] == 3

def test_profile_res_sales_count(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["class_distribution"]["R"]["sales"] == 3

def test_profile_com_sales_count(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["class_distribution"]["C"]["sales"] == 0

def test_profile_he_id_fill_rate_res(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["he_id_fill_rate_by_class"]["R"] == pytest.approx(1.0)

def test_profile_he_id_fill_rate_com(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert profile["he_id_fill_rate_by_class"]["C"] == pytest.approx(0.0)

def test_profile_missing_master_raises(tmp_path):
    (tmp_path / "no-data" / "in").mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        build_data_profile("no-data", data_base_dir=tmp_path)
