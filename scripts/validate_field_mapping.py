"""
validate_field_mapping.py

Validates field mappings in settings.json against the data profile.
Checks that critical fields are mapped, source columns exist in parquet
files, and calc expressions reference resolvable fields.
"""

# Operators used in the calc DSL — not field references
_CALC_OPERATORS = {
    "+", "-", "*", "/",
    ">", "<", "==", "!=", ">=", "<=",
    "and", "or", "?", "not",
    "isin",
    "asstr", "asint", "asfloat", "asbool",
    "abs", "log", "exp", "sqrt",
    "min", "max",
    "if", "coalesce",
}


def _extract_calc_fields(expr) -> set:
    """
    Recursively extract field name references from a calc DSL expression.

    Returns a set of field name strings. Ignores operators, string literals
    (prefixed with 'str:'), and numeric values.
    """
    if isinstance(expr, (int, float)):
        return set()

    if isinstance(expr, str):
        if expr in _CALC_OPERATORS:
            return set()
        if expr.startswith("str:"):
            return set()
        return {expr}

    if isinstance(expr, list):
        if len(expr) == 0:
            return set()

        op = expr[0] if isinstance(expr[0], str) else None
        fields = set()

        # For isin, the third element is a value list — skip it
        if op == "isin":
            # expr[1] is the field, expr[2] is the value list
            if len(expr) >= 2:
                fields |= _extract_calc_fields(expr[1])
            # Skip expr[2] — it's a list of literal values
            return fields

        # For all other operators, recurse into elements after the operator
        for i, element in enumerate(expr):
            if i == 0 and isinstance(element, str) and element in _CALC_OPERATORS:
                continue  # skip the operator itself
            fields |= _extract_calc_fields(element)

        return fields

    return set()
