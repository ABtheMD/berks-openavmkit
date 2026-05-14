"""Tests for scripts/validate_field_mapping.py."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from validate_field_mapping import _extract_calc_fields


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
