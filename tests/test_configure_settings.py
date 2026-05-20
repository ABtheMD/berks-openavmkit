"""Tests for scripts/configure_settings.py — unit tests for calc and merge builders."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import configure_settings


# ---------------------------------------------------------------------------
# build_sales_calcs
# ---------------------------------------------------------------------------

def test_valid_sale_lower_bound_is_1000():
    calcs = configure_settings.build_sales_calcs()
    vs = calcs["valid_sale"]
    # Structure: ["?", ["and", [">", "sale_price", 1000], ["<", "sale_price", 50000000]]]
    and_clause = vs[1]
    lower_bound = and_clause[1]  # [">", "sale_price", 1000]
    assert lower_bound[2] == 1000


def test_vacant_sale_uses_bldg_value():
    calcs = configure_settings.build_sales_calcs()
    vs = calcs["vacant_sale"]
    # Structure: ["?", ["==", "bldg_value", 0]]
    assert vs[1][0] == "=="
    assert vs[1][1] == "bldg_value"
    assert vs[1][2] == 0


def test_key_sale_structure():
    calcs = configure_settings.build_sales_calcs()
    ks = calcs["key_sale"]
    assert ks[0] == "+"
    assert ks[1] == ["asstr", "key"]


# ---------------------------------------------------------------------------
# detect_column — building value detection
# ---------------------------------------------------------------------------

def test_detect_bldg_value_valubldg():
    cols = ["parid", "class", "valubldg", "valuland"]
    result = configure_settings.detect_column(cols, configure_settings.BLDG_VALUE_PATTERNS)
    assert result == "valubldg"


def test_detect_bldg_value_not_found():
    cols = ["parid", "class", "sale_price"]
    result = configure_settings.detect_column(cols, configure_settings.BLDG_VALUE_PATTERNS)
    assert result is None


# ---------------------------------------------------------------------------
# build_merge
# ---------------------------------------------------------------------------

def test_build_merge_universe_includes_all_sources():
    sources = [
        {"handle": "geo_parcels", "role": "geo_parcels"},
        {"handle": "cama_master", "role": "cama_master"},
        {"handle": "cama_residential", "role": "cama_residential"},
        {"handle": "cama_commercial", "role": "cama_commercial"},
    ]
    schemas = {h["handle"]: ["col1"] for h in sources}
    merge = configure_settings.build_merge(sources, schemas, "geo_parcels", "cama_master")

    assert merge["universe"][0] == "geo_parcels"
    ids = [e["id"] for e in merge["universe"][1:]]
    assert "cama_master" in ids
    assert "cama_residential" in ids
    assert "cama_commercial" in ids


def test_build_merge_sales_includes_all_cama_sources():
    sources = [
        {"handle": "geo_parcels", "role": "geo_parcels"},
        {"handle": "cama_master", "role": "cama_master"},
        {"handle": "cama_residential", "role": "cama_residential"},
        {"handle": "cama_commercial", "role": "cama_commercial"},
    ]
    schemas = {h["handle"]: ["col1"] for h in sources}
    merge = configure_settings.build_merge(sources, schemas, "geo_parcels", "cama_master")

    sales_ids = [e["id"] for e in merge["sales"]]
    assert "cama_master" in sales_ids
    assert "cama_residential" in sales_ids
    assert "cama_commercial" in sales_ids


def test_build_merge_sales_residential_joins_on_key():
    sources = [
        {"handle": "geo_parcels", "role": "geo_parcels"},
        {"handle": "cama_master", "role": "cama_master"},
        {"handle": "cama_residential", "role": "cama_residential"},
    ]
    schemas = {h["handle"]: ["col1"] for h in sources}
    merge = configure_settings.build_merge(sources, schemas, "geo_parcels", "cama_master")

    res_entry = [e for e in merge["sales"] if e["id"] == "cama_residential"][0]
    assert res_entry["on"] == "key"
    assert res_entry["how"] == "left"


def test_build_merge_sales_non_cama_excluded():
    """Non-CAMA sources should not be added to the sales merge."""
    sources = [
        {"handle": "geo_parcels", "role": "geo_parcels"},
        {"handle": "cama_master", "role": "cama_master"},
        {"handle": "cama_residential", "role": "cama_residential"},
        {"handle": "permits", "role": "permits"},
    ]
    schemas = {h["handle"]: ["col1"] for h in sources}
    merge = configure_settings.build_merge(sources, schemas, "geo_parcels", "cama_master")

    sales_ids = [e["id"] for e in merge["sales"]]
    assert "cama_residential" in sales_ids
    assert "permits" not in sales_ids


# ---------------------------------------------------------------------------
# detect_location_field
# ---------------------------------------------------------------------------

def test_detect_location_field_neighborhood():
    schemas = {"src": ["parid", "neighborhood", "class"]}
    classified = {"parid", "neighborhood", "class"}
    assert configure_settings.detect_location_field(schemas, classified) == "neighborhood"


def test_detect_location_field_location():
    schemas = {"src": ["parid", "location", "class"]}
    classified = {"parid", "location", "class"}
    assert configure_settings.detect_location_field(schemas, classified) == "location"


def test_detect_location_field_prefers_neighborhood_over_location():
    """When both exist, neighborhood wins (earlier in LOCATION_PATTERNS)."""
    schemas = {"src": ["parid", "neighborhood", "location"]}
    classified = {"parid", "neighborhood", "location"}
    assert configure_settings.detect_location_field(schemas, classified) == "neighborhood"


def test_detect_location_field_not_classified():
    """A column must be in field_classification to be detected."""
    schemas = {"src": ["parid", "location"]}
    classified = {"parid"}  # location not classified
    assert configure_settings.detect_location_field(schemas, classified) is None


def test_detect_location_field_none_when_absent():
    schemas = {"src": ["parid", "class"]}
    classified = {"parid", "class"}
    assert configure_settings.detect_location_field(schemas, classified) is None


def test_build_merge_sales_master_joins_on_key_sale():
    sources = [
        {"handle": "geo_parcels", "role": "geo_parcels"},
        {"handle": "cama_master", "role": "cama_master"},
    ]
    schemas = {h["handle"]: ["col1"] for h in sources}
    merge = configure_settings.build_merge(sources, schemas, "geo_parcels", "cama_master")

    master_entry = merge["sales"][0]
    assert master_entry["id"] == "cama_master"
    assert master_entry["on"] == "key_sale"
