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
    (prefixed with 'str:'), numeric values, and booleans. Any value of an
    unrecognised type (e.g. None, dict) is silently ignored.
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
        if not expr:
            return set()

        op = expr[0] if isinstance(expr[0], str) else None
        fields = set()

        # For isin, extract the field operand (expr[1]) but skip the value
        # list (expr[2]) — it contains literal values, not field references.
        if op == "isin":
            if len(expr) >= 2:
                fields |= _extract_calc_fields(expr[1])
            return fields

        # For all other operators, recurse into elements after the operator
        for i, element in enumerate(expr):
            if i == 0 and isinstance(element, str) and element in _CALC_OPERATORS:
                continue  # skip the operator itself
            fields |= _extract_calc_fields(element)

        return fields

    return set()


# ---------------------------------------------------------------------------
# Field registries
# ---------------------------------------------------------------------------

CRITICAL_FIELDS = {"key", "sale_price", "sale_date", "class"}
IMPORTANT_FIELDS = {"valid_sale", "vacant_sale", "he_id", "assr_market_value"}

# Sales qualification distribution thresholds
VALID_SALE_LOW_THRESHOLD = 0.05    # <5% valid → warning
VALID_SALE_HIGH_THRESHOLD = 0.95   # >95% valid → warning


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_field_mapping(settings: dict, data_profile: dict) -> dict:
    """
    Validate field mappings in settings against the data profile.

    Runs three checks:
      1. Critical field completeness — are required fields mapped or calculated?
      2. Source column existence — do mapped source columns exist in parquet files?
      3. Calc dependency resolution — do calc expressions reference available fields?

    Returns {"errors": [...], "warnings": [...]} where each entry is a
    human-readable string. Errors should block the pipeline; warnings are
    informational.
    """
    errors = []
    warnings = []

    data_load = settings.get("data", {}).get("load", {})
    column_profiles = data_profile.get("column_profiles", {})

    # Collect all mapped canonical field names and calc-defined names
    all_mapped_fields = set()
    all_calc_fields = set()
    for source_handle, source_cfg in data_load.items():
        if not isinstance(source_cfg, dict):
            continue
        load_map = source_cfg.get("load", {})
        for canonical_name in load_map:
            all_mapped_fields.add(canonical_name)
        calc_map = source_cfg.get("calc", {})
        for calc_name in calc_map:
            all_calc_fields.add(calc_name)

    all_available = all_mapped_fields | all_calc_fields

    # ==================================================================
    # Check 1: Critical field completeness
    # ==================================================================

    for field in sorted(CRITICAL_FIELDS):
        if field not in all_available:
            errors.append(
                f"Missing critical field '{field}': not mapped in any "
                f"source's load or calc section"
            )

    for field in sorted(IMPORTANT_FIELDS):
        if field not in all_available:
            warnings.append(
                f"Missing important field '{field}': not mapped in any "
                f"source's load or calc section"
            )

    # ==================================================================
    # Check 2: Source column existence
    # ==================================================================

    if column_profiles:
        for source_handle, source_cfg in data_load.items():
            if not isinstance(source_cfg, dict):
                continue
            source_columns = column_profiles.get(source_handle, None)
            if source_columns is None:
                # Source not in column_profiles — skip (maybe not downloaded yet)
                continue

            load_map = source_cfg.get("load", {})
            for canonical_name, source_ref in load_map.items():
                # Extract the source column name
                if isinstance(source_ref, list):
                    # Complex mapping: [column, dtype, format]
                    source_col = source_ref[0] if len(source_ref) >= 1 else None
                elif isinstance(source_ref, str):
                    source_col = source_ref
                else:
                    continue

                if source_col and source_col not in source_columns:
                    errors.append(
                        f"Source column '{source_col}' does not exist in "
                        f"'{source_handle}' (mapped as '{canonical_name}')"
                    )

    # ==================================================================
    # Check 3: Calc dependency resolution
    # ==================================================================

    for source_handle, source_cfg in data_load.items():
        if not isinstance(source_cfg, dict):
            continue
        load_map = source_cfg.get("load", {})
        calc_map = source_cfg.get("calc", {})
        if not calc_map:
            continue

        # Fields available in this source: load keys + prior calc keys
        available_in_source = set(load_map.keys())

        for calc_name, calc_expr in calc_map.items():
            referenced = _extract_calc_fields(calc_expr)
            for ref in sorted(referenced):
                if ref not in available_in_source:
                    warnings.append(
                        f"Unresolved calc dependency in '{source_handle}': "
                        f"calc '{calc_name}' references '{ref}' which is not "
                        f"in load or a prior calc"
                    )
            # This calc's output is now available for subsequent calcs
            available_in_source.add(calc_name)

    return {"errors": errors, "warnings": warnings}


def validate_sales_qualification(df) -> dict:
    """
    Validate sales qualification flags in the assembled DataFrame.

    Checks the actual distributions of valid_sale and vacant_sale after
    assembly. Catastrophic conditions (zero valid sales, zero sale rows)
    produce errors that block the pipeline. Suspicious distributions
    produce warnings.

    Parameters
    ----------
    df : pd.DataFrame
        The assembled "sup" DataFrame containing all parcels with their
        mapped and calculated fields. Must have a 'sale_price' column.

    Returns
    -------
    dict
        {"errors": [...], "warnings": [...]} where each entry is a
        human-readable string.
    """
    errors = []
    warnings = []

    # Identify sale rows: non-null sale_price
    if "sale_price" not in df.columns:
        errors.append("Column 'sale_price' not found in assembled DataFrame")
        return {"errors": errors, "warnings": warnings}

    sale_mask = df["sale_price"].notna()
    n_sales = int(sale_mask.sum())

    # ------------------------------------------------------------------
    # Check 1: No sale rows
    # ------------------------------------------------------------------
    if n_sales == 0:
        errors.append(
            "No sales found in assembled data (all sale_price values are "
            "null). Modeling requires sale rows to train on."
        )
        return {"errors": errors, "warnings": warnings}

    # ------------------------------------------------------------------
    # Check 2: valid_sale column existence
    # ------------------------------------------------------------------
    has_valid_sale = "valid_sale" in df.columns
    if not has_valid_sale:
        warnings.append(
            "Column 'valid_sale' not found in assembled DataFrame. "
            "All sales will be treated as valid."
        )

    # ------------------------------------------------------------------
    # Check 3: vacant_sale column existence
    # ------------------------------------------------------------------
    has_vacant_sale = "vacant_sale" in df.columns
    if not has_vacant_sale:
        warnings.append(
            "Column 'vacant_sale' not found in assembled DataFrame. "
            "Land model will have no vacant sale indicators."
        )

    # ------------------------------------------------------------------
    # Checks 4-6: valid_sale distribution
    # ------------------------------------------------------------------
    if has_valid_sale:
        valid_among_sales = df.loc[sale_mask, "valid_sale"].eq(True)
        n_valid = int(valid_among_sales.sum())
        valid_rate = n_valid / n_sales

        # Check 4: Zero valid sales
        if n_valid == 0:
            errors.append(
                f"No valid sales found: 0 of {n_sales} sale rows have "
                f"valid_sale == True. Modeling will have zero training rows."
            )
        else:
            # Check 5: Filter too restrictive (only if >0%)
            if valid_rate < VALID_SALE_LOW_THRESHOLD:
                pct = f"{valid_rate:.1%}"
                warnings.append(
                    f"Sales filter may be too restrictive: only {pct} of "
                    f"{n_sales} sales are marked valid ({n_valid} rows)."
                )

            # Check 6: Filter too loose
            if valid_rate > VALID_SALE_HIGH_THRESHOLD:
                pct = f"{valid_rate:.1%}"
                warnings.append(
                    f"Sales filter may be too loose: {pct} of {n_sales} "
                    f"sales are marked valid ({n_valid} rows). The filter "
                    f"may not be excluding non-arm's-length transactions."
                )

    # ------------------------------------------------------------------
    # Check 7: No vacant sales
    # ------------------------------------------------------------------
    if has_vacant_sale:
        vacant_among_sales = df.loc[sale_mask, "vacant_sale"].eq(True)
        n_vacant = int(vacant_among_sales.sum())

        if n_vacant == 0:
            warnings.append(
                f"No vacant sales found: 0 of {n_sales} sale rows have "
                f"vacant_sale == True. Land model will have no training data."
            )

    return {"errors": errors, "warnings": warnings}
