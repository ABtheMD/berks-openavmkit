"""Tests for scripts/validate_field_mapping.py."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from validate_field_mapping import _extract_calc_fields, validate_field_mapping


# ---------------------------------------------------------------------------
# _extract_calc_fields
# ---------------------------------------------------------------------------

def test_extract_simple_binary():
    """Binary operator extracts both field operands."""
    expr = [">", "sale_price", 1000]
    assert _extract_calc_fields(expr) == {"sale_price"}

def test_extract_ignores_string_literals():
    """String literals prefixed with 'str:' are not field references."""
    expr = ["+", "key", "str:_"]
    assert _extract_calc_fields(expr) == {"key"}

def test_extract_nested():
    """Nested expressions extract fields at all levels."""
    expr = ["+", ["asstr", "key"], ["+", "str:_", ["asstr", "sale_date"]]]
    assert _extract_calc_fields(expr) == {"key", "sale_date"}

def test_extract_boolean_condition():
    """Boolean condition with ? operator extracts inner fields."""
    expr = ["?", ["and", [">", "sale_price", 1000], ["<", "sale_price", 50000000]]]
    assert _extract_calc_fields(expr) == {"sale_price"}

def test_extract_isin_skips_value_list():
    """isin operator: second element is a field, third is a value list (not fields)."""
    expr = ["isin", "class", ["A", "C", "F"]]
    assert _extract_calc_fields(expr) == {"class"}

def test_extract_complex_calc():
    """Full vacant_sale calc from Berks settings."""
    expr = ["?", ["or", ["<", "sale_price", 0], ["isin", "class", ["A", "C", "F", "I", "E", "UT"]]]]
    assert _extract_calc_fields(expr) == {"sale_price", "class"}

def test_extract_empty_list():
    """Empty list returns empty set."""
    assert _extract_calc_fields([]) == set()

def test_extract_bare_string():
    """A bare string (not in a list) is a field reference."""
    assert _extract_calc_fields("sale_price") == {"sale_price"}

def test_extract_bare_number():
    """A bare number returns empty set."""
    assert _extract_calc_fields(42) == set()

def test_extract_bare_operator():
    """A bare operator string is not a field reference."""
    assert _extract_calc_fields("+") == set()

def test_extract_bool_value():
    """A boolean value (Python bool is subclass of int) returns empty set."""
    assert _extract_calc_fields(True) == set()
    assert _extract_calc_fields(False) == set()

def test_extract_none_value():
    """None is silently ignored."""
    assert _extract_calc_fields(None) == set()


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

FIELD_MAPPING_PROFILE = {
    "locality": "us-pa-test",
    "total_parcels": 5000,
    "total_sales": 1000,
    "annual_sales_volume": 500,
    "class_distribution": {"R": {"parcels": 4000, "sales": 800}},
    "he_id_fill_rate_by_class": {"R": 0.98},
    "land_he_id_fill_rate_by_class": {"R": 0.97},
    "has_spatial_data": True,
    "column_profiles": {
        "cama_master": {
            "parid": {"dtype": "string", "non_null": 5000, "unique": 5000},
            "price": {"dtype": "float", "non_null": 3000, "unique": 2000},
            "saledt": {"dtype": "string", "non_null": 3000, "unique": 2500},
            "class": {"dtype": "string", "non_null": 5000, "unique": 5},
            "livunit": {"dtype": "int", "non_null": 4000, "unique": 10},
        },
        "geo_parcels": {
            "propid": {"dtype": "string", "non_null": 5000, "unique": 5000},
            "acreage": {"dtype": "float", "non_null": 4900, "unique": 4000},
            "school": {"dtype": "string", "non_null": 5000, "unique": 15},
        },
    },
    "jurisdiction_tier": "large_to_mid",
}

COMPLETE_SETTINGS = {
    "data": {
        "load": {
            "cama_master": {
                "filename": "cama_master.parquet",
                "load": {
                    "key": "parid",
                    "sale_price": "price",
                    "sale_date": "saledt",
                    "class": "class",
                },
                "calc": {
                    "valid_sale": ["?", ["and", [">", "sale_price", 1000], ["<", "sale_price", 50000000]]],
                    "vacant_sale": ["?", ["isin", "class", ["A", "C"]]],
                },
            },
            "geo_parcels": {
                "filename": "geo_parcels.parquet",
                "load": {
                    "key": "propid",
                    "acreage": "acreage",
                },
            },
        }
    }
}


# ---------------------------------------------------------------------------
# Check 1: Critical field completeness
# ---------------------------------------------------------------------------

def test_check1_all_critical_present():
    """All critical fields mapped — no errors."""
    result = validate_field_mapping(COMPLETE_SETTINGS, FIELD_MAPPING_PROFILE)
    critical_errors = [e for e in result["errors"] if "critical" in e.lower() or "missing" in e.lower()]
    assert critical_errors == []

def test_check1_missing_key_is_error():
    """Missing 'key' field in all sources should produce an error."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("key" in e for e in result["errors"])

def test_check1_missing_sale_price_is_error():
    """Missing 'sale_price' field should produce an error."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("sale_price" in e for e in result["errors"])

def test_check1_field_in_calc_counts():
    """A critical field defined via calc (not load) should count as present."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                    "calc": {
                        "key": ["+", "str:P", ["asstr", "sale_price"]],
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    key_errors = [e for e in result["errors"] if "'key'" in e]
    assert key_errors == []

def test_check1_missing_valid_sale_is_warning():
    """Missing 'valid_sale' (important, not critical) should produce a warning."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("valid_sale" in w for w in result["warnings"])
    # Should NOT be in errors
    assert not any("valid_sale" in e for e in result["errors"])

def test_check1_missing_he_id_is_warning():
    """Missing 'he_id' should produce a warning, not an error."""
    result = validate_field_mapping(COMPLETE_SETTINGS, FIELD_MAPPING_PROFILE)
    assert any("he_id" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# Check 2: Source column existence
# ---------------------------------------------------------------------------

def test_check2_all_source_columns_exist():
    """All mapped source columns exist in column_profiles — no errors."""
    result = validate_field_mapping(COMPLETE_SETTINGS, FIELD_MAPPING_PROFILE)
    source_errors = [e for e in result["errors"] if "does not exist" in e]
    assert source_errors == []

def test_check2_missing_source_column_is_error():
    """Mapped source column that doesn't exist in parquet should produce an error."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "nonexistent_column",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("nonexistent_column" in e and "cama_master" in e for e in result["errors"])

def test_check2_complex_mapping_extracts_column():
    """Complex mapping format ['col', 'type', 'fmt'] extracts source column correctly."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": ["saledt", "datetime", "%Y-%m-%d"],
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    source_errors = [e for e in result["errors"] if "does not exist" in e]
    assert source_errors == []

def test_check2_complex_mapping_missing_column():
    """Complex mapping with nonexistent source column should error."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": ["bad_col", "datetime", "%Y-%m-%d"],
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("bad_col" in e for e in result["errors"])

def test_check2_skips_sources_without_column_profiles():
    """Sources not in column_profiles are skipped (no false positives)."""
    settings = {
        "data": {
            "load": {
                "unknown_source": {
                    "filename": "unknown.parquet",
                    "load": {"key": "some_col"},
                },
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    source_errors = [e for e in result["errors"] if "does not exist" in e]
    assert source_errors == []


# ---------------------------------------------------------------------------
# Check 3: Calc dependency resolution
# ---------------------------------------------------------------------------

def test_check3_calc_deps_resolved():
    """Calc referencing fields in load produces no warnings."""
    result = validate_field_mapping(COMPLETE_SETTINGS, FIELD_MAPPING_PROFILE)
    calc_warnings = [w for w in result["warnings"] if "calc" in w.lower() and "unresolved" in w.lower()]
    assert calc_warnings == []

def test_check3_unresolved_calc_dep_is_warning():
    """Calc referencing a field not in load or prior calc produces a warning."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                    "calc": {
                        "test_field": ["+", "ghost_field", 1],
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert any("ghost_field" in w for w in result["warnings"])

def test_check3_calc_can_reference_prior_calc():
    """A calc can reference a field defined by an earlier calc in the same source."""
    settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {
                        "key": "parid",
                        "sale_price": "price",
                        "sale_date": "saledt",
                        "class": "class",
                    },
                    "calc": {
                        "key_sale": ["+", ["asstr", "key"], "str:_"],
                        "valid_key_sale": ["?", [">", "key_sale", "str:"]],
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    calc_warnings = [w for w in result["warnings"] if "key_sale" in w and "unresolved" in w.lower()]
    assert calc_warnings == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_validate_empty_settings():
    """Settings with no data.load should report all critical fields missing."""
    result = validate_field_mapping({}, FIELD_MAPPING_PROFILE)
    assert len(result["errors"]) >= 4  # key, sale_price, sale_date, class

def test_validate_no_column_profiles():
    """Profile without column_profiles skips Check 2 (no false positives)."""
    profile = {
        "locality": "us-pa-test",
        "total_parcels": 5000,
        "total_sales": 1000,
        "annual_sales_volume": 500,
        "class_distribution": {},
        "he_id_fill_rate_by_class": {},
        "land_he_id_fill_rate_by_class": {},
        "has_spatial_data": False,
        "jurisdiction_tier": "large_to_mid",
    }
    result = validate_field_mapping(COMPLETE_SETTINGS, profile)
    source_errors = [e for e in result["errors"] if "does not exist" in e]
    assert source_errors == []
