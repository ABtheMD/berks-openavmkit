# Sales Qualification Validation

**Date:** 2026-05-14
**Branch:** `feature/sales-qualification` (new, cut from master)
**Status:** Approved for implementation

---

## 1. Problem

The openavmkit pipeline derives two boolean flags during assembly — `valid_sale` (arm's-length transaction filter) and `vacant_sale` (vacant parcel indicator) — via calc expressions in `settings.json`. These flags determine which sales enter modeling and ratio studies. When the flags are misconfigured, two failure modes occur:

1. **Zero valid sales** — the calc expression is too restrictive (e.g., wrong price field, inverted logic), so every sale is marked invalid. The pipeline continues into modeling with zero training rows and crashes deep inside LightGBM with a cryptic error.
2. **No-op filter** — the calc expression matches everything (e.g., `sale_price > 0` when all prices are positive), so no sales are filtered. The model trains on non-arm's-length transactions and produces poor valuations.

PR #15 (field mapping completeness) catches structural problems at configure time — missing `valid_sale`/`vacant_sale` fields produce warnings, and broken calc references produce dependency warnings. But structural checks cannot catch semantic problems: a syntactically valid calc that produces the wrong distribution. That requires inspecting the actual data after assembly.

## 2. Design

### 2.1 Approach: Warn + Block on Catastrophic

Post-assembly validation checks the actual distributions of `valid_sale` and `vacant_sale` in the assembled DataFrame. Catastrophic conditions (zero valid sales, zero sale rows) block the pipeline with an error. Suspicious but non-fatal distributions produce warnings.

No Claude API calls. No settings modifications. Pure programmatic validation.

### 2.2 `validate_sales_qualification()` — new function in `validate_field_mapping.py`

```python
def validate_sales_qualification(df: pd.DataFrame) -> dict:
```

**Parameters:**
- `df` — the assembled DataFrame (the "sup" DataFrame after `01-assemble` completes), containing all parcels with their mapped and calculated fields

**Returns:** `{"errors": [...], "warnings": [...]}` — same format as `validate_field_mapping()` for consistency. Errors should block the pipeline; warnings are informational.

**Checks (executed in order):**

| # | Check | Condition | Level | Rationale |
|---|-------|-----------|-------|-----------|
| 1 | No sale rows | Zero rows with non-null `sale_price` | Error | No sales data at all — nothing to model |
| 2 | `valid_sale` missing | Column not in DataFrame | Warning | Already flagged at configure time by PR #15; repeated here for completeness |
| 3 | `vacant_sale` missing | Column not in DataFrame | Warning | Same as above |
| 4 | Zero valid sales | 0% of sale rows have `valid_sale == True` | Error | Modeling has zero training rows — guaranteed crash |
| 5 | Filter too restrictive | >0% but <5% of sale rows have `valid_sale == True` | Warning | Suspiciously few qualified sales |
| 6 | Filter too loose | >95% of sale rows have `valid_sale == True` | Warning | Filter may not be doing anything |
| 7 | No vacant sales | 0% of sale rows have `vacant_sale == True` | Warning | Land model will have no training data |

**Check execution logic:**
- Check 1 runs first and short-circuits — if there are no sale rows, distribution checks are meaningless.
- Checks 2–3 (column existence) run independently. If a column is missing, its distribution checks (4–7) are skipped.
- Checks 4–6 only run if `valid_sale` exists. Check 5 only fires when check 4 does not (0% is an error, not a warning).
- Check 7 only runs if `vacant_sale` exists.
- "Sale rows" are defined as rows where `sale_price` is not null (`df["sale_price"].notna()`).
- A sale counts as "valid" if `valid_sale == True` (truthy after boolean coercion). NaN values in `valid_sale` are treated as not valid.

**Thresholds:**
- The 5% and 95% thresholds are hardcoded constants at module level. Comparisons are strict: `rate < 0.05` for check 5, `rate > 0.95` for check 6. A rate of exactly 5% does not trigger check 5; exactly 95% does not trigger check 6.
- If future jurisdictions need different thresholds, promoting them to settings.json is a small change, but YAGNI for now.

### 2.3 Integration into `harness.py`

The harness's assembly stage calls `validate_sales_qualification()` after the assembled DataFrame is produced, before the clean stage begins.

**New exception:** `SalesQualificationError(Exception)` — raised when `validate_sales_qualification()` returns errors. Same pattern as `FieldMappingError` from PR #15.

**Integration point:** Inside `run_assemble()` (or between assemble and clean in the stage runner), after the assembled pickle is written:

1. Load the assembled DataFrame
2. Call `validate_sales_qualification(df)`
3. Log all warnings
4. If errors: raise `SalesQualificationError` with the error messages
5. If no errors: continue to clean stage

**Why after assembly, not during:** The assembly stage is run by the notebook (`01-assemble.ipynb`) which calls multiple pipeline functions. The validation runs after all assembly is complete, so it sees the final state of the data including all calc expressions evaluated.

### 2.4 Constants

```python
VALID_SALE_LOW_THRESHOLD = 0.05   # <5% valid → warning
VALID_SALE_HIGH_THRESHOLD = 0.95  # >95% valid → warning
```

Defined at module level in `validate_field_mapping.py` alongside `CRITICAL_FIELDS` and `IMPORTANT_FIELDS`.

## 3. Files Changed

| File | Change |
|------|--------|
| `scripts/validate_field_mapping.py` | Add `validate_sales_qualification()` function and threshold constants |
| `scripts/harness.py` | Call `validate_sales_qualification()` after assembly; add `SalesQualificationError` exception |
| `tests/test_validate_field_mapping.py` | Tests for all 7 checks plus edge cases |

## 4. What Doesn't Change

- `validate_field_mapping()` — unchanged (still handles configure-time checks)
- `refine_field_mapping()` — unchanged (Claude field mapping refinement)
- `generate_settings.py` — unchanged (fuzzy matching)
- `claude_settings.py` — unchanged (no Claude involvement in sales qualification)
- `openavmkit/cleaning.py` — unchanged (`clean_valid_sales()` still runs after this validation)
- The calc DSL — unchanged
- `settings.json` calc expressions for valid_sale / vacant_sale — unchanged

## 5. Testing Strategy

Unit tests for `validate_sales_qualification()`:

- **Check 1 (no sale rows):** DataFrame where all `sale_price` values are NaN → error
- **Check 2 (valid_sale missing):** DataFrame without `valid_sale` column → warning
- **Check 3 (vacant_sale missing):** DataFrame without `vacant_sale` column → warning
- **Check 4 (zero valid sales):** All `valid_sale == False` among sale rows → error
- **Check 5 (filter too restrictive):** 3% valid sales → warning
- **Check 6 (filter too loose):** 98% valid sales → warning
- **Check 7 (no vacant sales):** All `vacant_sale == False` among sale rows → warning
- **Happy path:** 50% valid sales, 20% vacant sales → no errors, no warnings
- **Threshold boundaries:** Exactly 5% valid → no warning (threshold is strict less-than); exactly 95% → no warning
- **Edge case:** DataFrame with valid_sale column but all NaN among sale rows → treated as 0% valid → error
- **Edge case:** Empty DataFrame → error (no sale rows)

Integration test for harness:
- `SalesQualificationError` is raised when validation returns errors
- Warnings are logged but do not block the pipeline
