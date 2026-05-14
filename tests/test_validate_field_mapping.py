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


# ---------------------------------------------------------------------------
# validate_sales_qualification
# ---------------------------------------------------------------------------

import numpy as np
import pandas as pd
from validate_field_mapping import (
    validate_sales_qualification,
    VALID_SALE_LOW_THRESHOLD,
    VALID_SALE_HIGH_THRESHOLD,
)


def _make_sales_df(n_sales, valid_rate=0.5, vacant_rate=0.2, n_non_sale=10,
                   include_valid_sale=True, include_vacant_sale=True):
    """Helper: build a DataFrame with sale rows and non-sale rows."""
    n_valid = int(n_sales * valid_rate)
    n_vacant = int(n_sales * vacant_rate)

    sale_prices = [100_000.0] * n_sales + [float("nan")] * n_non_sale
    rows = {"sale_price": sale_prices}

    if include_valid_sale:
        valid_flags = [True] * n_valid + [False] * (n_sales - n_valid) + [float("nan")] * n_non_sale
        rows["valid_sale"] = valid_flags

    if include_vacant_sale:
        vacant_flags = [True] * n_vacant + [False] * (n_sales - n_vacant) + [float("nan")] * n_non_sale
        rows["vacant_sale"] = vacant_flags

    return pd.DataFrame(rows)


# -- Check 1: No sale rows --

def test_sq_no_sale_rows_all_nan():
    """All sale_price values are NaN — error."""
    df = pd.DataFrame({"sale_price": [float("nan")] * 10,
                        "valid_sale": [True] * 10,
                        "vacant_sale": [False] * 10})
    result = validate_sales_qualification(df)
    assert any("no sale" in e.lower() or "no sales" in e.lower() for e in result["errors"])


def test_sq_empty_dataframe():
    """Empty DataFrame — error (no sale rows)."""
    df = pd.DataFrame({"sale_price": pd.Series(dtype="float"),
                        "valid_sale": pd.Series(dtype="bool"),
                        "vacant_sale": pd.Series(dtype="bool")})
    result = validate_sales_qualification(df)
    assert len(result["errors"]) >= 1


# -- Check 2 & 3: Column existence --

def test_sq_valid_sale_missing():
    """valid_sale column missing — warning."""
    df = pd.DataFrame({"sale_price": [100_000.0] * 10,
                        "vacant_sale": [True] * 5 + [False] * 5})
    result = validate_sales_qualification(df)
    assert any("valid_sale" in w for w in result["warnings"])
    # Should NOT error for missing column
    assert not any("valid_sale" in e for e in result["errors"])


def test_sq_vacant_sale_missing():
    """vacant_sale column missing — warning."""
    df = pd.DataFrame({"sale_price": [100_000.0] * 10,
                        "valid_sale": [True] * 5 + [False] * 5})
    result = validate_sales_qualification(df)
    assert any("vacant_sale" in w for w in result["warnings"])


# -- Check 4: Zero valid sales --

def test_sq_zero_valid_sales():
    """All valid_sale == False among sale rows — error."""
    df = _make_sales_df(100, valid_rate=0.0, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    assert any("0%" in e or "zero" in e.lower() or "no valid" in e.lower() for e in result["errors"])


def test_sq_valid_sale_all_nan():
    """valid_sale column exists but all NaN among sale rows — treated as 0% — error."""
    df = pd.DataFrame({"sale_price": [100_000.0] * 20,
                        "valid_sale": [float("nan")] * 20,
                        "vacant_sale": [True] * 10 + [False] * 10})
    result = validate_sales_qualification(df)
    assert any("0%" in e or "zero" in e.lower() or "no valid" in e.lower() for e in result["errors"])


# -- Check 5: Filter too restrictive --

def test_sq_filter_too_restrictive():
    """3% valid sales — warning."""
    df = _make_sales_df(100, valid_rate=0.03, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    assert any("restrictive" in w.lower() or "low" in w.lower() or "3" in w for w in result["warnings"])
    # Should NOT be an error (it's >0%)
    assert result["errors"] == []


# -- Check 6: Filter too loose --

def test_sq_filter_too_loose():
    """98% valid sales — warning."""
    df = _make_sales_df(100, valid_rate=0.98, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    assert any("loose" in w.lower() or "high" in w.lower() or "98" in w for w in result["warnings"])


# -- Check 7: No vacant sales --

def test_sq_no_vacant_sales():
    """All vacant_sale == False — warning."""
    df = _make_sales_df(100, valid_rate=0.5, vacant_rate=0.0)
    result = validate_sales_qualification(df)
    assert any("vacant" in w.lower() for w in result["warnings"])


# -- Happy path --

def test_sq_happy_path():
    """50% valid, 20% vacant — no errors, no warnings."""
    df = _make_sales_df(100, valid_rate=0.5, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    assert result["errors"] == []
    assert result["warnings"] == []


# -- Threshold boundaries --

def test_sq_exactly_5_percent_no_warning():
    """Exactly 5% valid — no warning (threshold is strict less-than)."""
    df = _make_sales_df(100, valid_rate=0.05, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    restrictive_warnings = [w for w in result["warnings"] if "restrictive" in w.lower() or "low" in w.lower()]
    assert restrictive_warnings == []
    assert result["errors"] == []


def test_sq_exactly_95_percent_no_warning():
    """Exactly 95% valid — no warning (threshold is strict greater-than)."""
    df = _make_sales_df(100, valid_rate=0.95, vacant_rate=0.2)
    result = validate_sales_qualification(df)
    loose_warnings = [w for w in result["warnings"] if "loose" in w.lower() or "high" in w.lower()]
    assert loose_warnings == []


# -- Constants --

def test_sq_thresholds_exist():
    """Threshold constants are importable and have expected values."""
    assert VALID_SALE_LOW_THRESHOLD == 0.05
    assert VALID_SALE_HIGH_THRESHOLD == 0.95
