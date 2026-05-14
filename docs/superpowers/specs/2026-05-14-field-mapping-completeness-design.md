# Field Mapping Completeness

**Date:** 2026-05-14
**Branch:** `feature/field-mapping-completeness` (new, cut from master)
**Status:** Approved for implementation

---

## 1. Problem

The openavmkit pipeline maps jurisdiction-specific column names to canonical field names via the `data.load` section of `settings.json`. Today, `generate_settings.py` builds these mappings by fuzzy-matching ArcGIS field metadata against `data_dictionary.json` (cutoff: 0.75 similarity). This process has three failure modes:

1. **Wrong match** — the fuzzy matcher maps a source column to the wrong canonical field (e.g., `totalarea` → `land_area_sqft` when it should be `bldg_area_finished_sqft`)
2. **Missing match** — a required canonical field has no fuzzy match and is omitted entirely
3. **Source column absent** — the mapped source column name doesn't exist in the downloaded parquet file (ArcGIS metadata diverged from actual data)

All three cause pipeline crashes during assembly or modeling. Error recovery (PR #12) catches these crashes, but the iteration is wasted. Different jurisdictions use wildly different column naming conventions (Berks has `parid`, Philly might have `parcel_num`), so fuzzy matching alone is insufficient.

## 2. Design

### 2.1 Two-step approach

**Step A — Claude refines field mappings.** After the fuzzy matcher produces initial mappings and data is downloaded, Claude reviews the mappings using `column_profiles` (PR #13). Claude sees what the fuzzy matcher produced, what columns actually exist in each parquet file, and the canonical field names the pipeline expects. Claude corrects wrong matches and fills gaps.

**Step B — Programmatic validation.** After Claude's refinements, a validator checks completeness and correctness. Critical field gaps block the pipeline with a clear error. Optional field gaps produce warnings.

This mirrors the pattern from PRs #13 + #14: give Claude better data, then validate Claude's output.

### 2.2 `refine_field_mapping()` — new function in `claude_settings.py`

A new public function alongside `generate_initial()` and `refine_after_model()`:

```python
def refine_field_mapping(
    data_profile: dict,
    current_settings: dict,
    reasoning_log: Path = None,
) -> dict:
```

**Parameters:**
- `data_profile` — output of `build_data_profile()`, contains `column_profiles` with dtype/non_null/unique for every column in every parquet file
- `current_settings` — the settings.json after `generate_settings.py` ran (contains fuzzy-matched `data.load` section)
- `reasoning_log` — JSONL file for Claude's reasoning

**Returns:** A settings delta for the `data` section. Same `{"settings": ..., "reasoning": ...}` response format as existing functions. The delta is merged via the existing `_merge_settings()`.

**System prompt** (`_SYSTEM_FIELD_MAPPING`): Instructs Claude to:
1. Review each source's `load` mapping against actual `column_profiles`
2. Fix wrong matches — if a mapped source column doesn't exist in the parquet, find the correct column name from `column_profiles`
3. Fill gaps — if critical canonical fields (key, sale_price, sale_date, class) aren't mapped in any source, find the best match from available columns
4. Preserve correct mappings — don't change mappings that are already right
5. Return only the `data.load` portion of the delta, plus reasoning

**Validation integration:** The returned delta is passed through `validate_field_mapping()` (see 2.3) before being applied. If violations are found, Claude is re-prompted once (same pattern as `_validate_and_reprompt` from PR #14).

### 2.3 `validate_field_mapping()` — new function in `validate_field_mapping.py`

```python
def validate_field_mapping(
    settings: dict,
    data_profile: dict,
) -> dict:
```

**Returns:** `{"errors": [...], "warnings": [...]}` where each entry is a human-readable string.

**Three validation checks:**

**Check 1 — Critical field completeness.** Verify these fields appear in at least one source's `load` mapping OR `calc` expressions:

| Field | Why critical |
|-------|-------------|
| `key` | Parcel identifier — all joins depend on it |
| `sale_price` | Sale amount — required for ratio studies and modeling |
| `sale_date` | Transaction date — required for temporal filtering |
| `class` | Property class — required for model_group assignment |

Missing critical field → error.

Additionally, check these important fields and warn if absent:

| Field | Why important |
|-------|--------------|
| `valid_sale` | Sale qualification flag — must exist (usually via `calc`) |
| `vacant_sale` | Vacant property flag — must exist (usually via `calc`) |
| `he_id` | Horizontal equity clustering — needed for ratio studies |

Missing important field → warning.

**Check 2 — Source column existence.** For every source in `data.load`, check that each mapped source column name (the right-hand value, e.g., `"parid"` in `"key": "parid"`) exists in that source's `column_profiles`. This catches the "ArcGIS said one thing, parquet has another" failure.

For complex mappings (list format like `["saledt", "datetime", "%Y-%m-%d"]`), extract the source column from index 0.

Missing source column → error (with message identifying which source file and which column).

**Check 3 — Calc dependency resolution.** For each `calc` expression in `data.load[source].calc`, extract field names it references. A referenced field must be either:
- Mapped in the same source's `load` section, OR
- Defined by a `calc` entry in the same source (order matters — calcs are processed sequentially)

Unresolvable calc dependency → warning (not error, because some references may be cross-source fields added during merge).

### 2.4 Integration into `harness.py`

The configure stage currently runs:
1. `generate_settings.py` → initial `settings.json`
2. `build_data_profile()` → `column_profiles`
3. `generate_initial()` → model_groups

Updated sequence:
1. `generate_settings.py` → initial `settings.json` (unchanged)
2. `build_data_profile()` → `column_profiles` (unchanged)
3. **`validate_field_mapping()`** → pre-check fuzzy-matched mappings
4. **If errors: `refine_field_mapping()`** → Claude fixes mappings, then re-validate
5. **If still errors: raise `FieldMappingError`**
6. `generate_initial()` → model_groups (unchanged)

**Early-exit optimization:** If the fuzzy-matched mappings already pass validation (no errors), skip the Claude call entirely. This avoids an unnecessary API call for jurisdictions where the fuzzy matcher got everything right. The Claude refinement only fires when the validator detects problems.

If `validate_field_mapping()` returns errors after both the fuzzy match and Claude's refinement, the harness raises `FieldMappingError` with a message listing all errors and warnings, and stops before model_groups generation.

### 2.5 Re-prompt on validation failure

`refine_field_mapping()` uses a `_validate_and_reprompt_field_mapping()` helper that follows the same pattern as `_validate_and_reprompt()` from PR #14 but with one key difference: **field mapping errors cannot be recovered by stripping.** A missing critical field means the pipeline cannot run, so the helper does not return a "cleaned" delta. Instead:

1. Call `validate_field_mapping()` on Claude's delta (merged with current settings)
2. If no errors → return the delta
3. If errors:
   a. Log violations to the reasoning JSONL file
   b. Build a re-prompt listing the errors and the current mappings
   c. Call Claude again with the errors as context
   d. Validate the second response
   e. If still errors → return `None` (caller raises `FieldMappingError`)
   f. If clean → return the corrected delta

The harness checks the return value: `None` means Claude couldn't fix the mappings, and the harness raises `FieldMappingError` with the accumulated error messages.

### 2.6 Extracting field references from calc expressions

The calc DSL uses a Lisp-like syntax: `["+", "field_a", "field_b"]`, `["?", ["and", [">", "sale_price", 1000], ...]]`. The validator needs to walk these expressions and extract field name references.

A field reference is any string element in a calc expression that:
- Is not an operator (`+`, `-`, `*`, `/`, `>`, `<`, `==`, `!=`, `>=`, `<=`, `and`, `or`, `?`, `isin`, `asstr`, `asint`, `asfloat`)
- Is not a string literal (prefixed with `str:`)
- Is not a numeric literal

The extraction function recursively walks the expression tree and collects all field references.

## 3. Files Changed

| File | Change |
|---|---|
| `scripts/claude_settings.py` | Add `_SYSTEM_FIELD_MAPPING` prompt; add `refine_field_mapping()` function; add `_validate_and_reprompt_field_mapping()` helper |
| `scripts/validate_field_mapping.py` | New file: `validate_field_mapping()` with three checks + `_extract_calc_fields()` helper |
| `scripts/harness.py` | Update `run_configure()` to call `refine_field_mapping()` then `validate_field_mapping()` between data profiling and model_groups generation; add `FieldMappingError` exception |
| `tests/test_validate_field_mapping.py` | New file: tests for all three validation checks + calc field extraction |
| `tests/test_claude_settings.py` | Add tests for `refine_field_mapping()` integration |

## 4. What Doesn't Change

- `generate_settings.py` — still does fuzzy matching as the first pass
- `generate_initial()` / `refine_after_model()` — unchanged
- `validate_settings_delta()` — unchanged (validates model settings, not field mappings)
- `profile_data.py` — unchanged (already provides `column_profiles`)
- `_merge_settings()` — unchanged (merges field mapping deltas the same way)
- The calc DSL in `openavmkit/calculations.py` — unchanged

## 5. Testing Strategy

Unit tests for `validate_field_mapping()`:
- Each of the 3 checks gets tests for both pass and fail cases
- Test critical field completeness: all present → no errors; `key` missing → error; `valid_sale` missing → warning (important, not critical)
- Test source column existence: mapped column exists → no error; mapped column missing → error with source file identified
- Test complex mapping format: `["saledt", "datetime", "%Y-%m-%d"]` → extracts `"saledt"` correctly
- Test calc dependency resolution: field referenced in calc exists in load → ok; missing → warning
- Test `_extract_calc_fields()`: correctly walks nested expressions, ignores operators and string literals

Unit tests for `refine_field_mapping()`:
- Mock Claude to return corrected mappings, verify they're applied
- Mock Claude to return invalid mappings, verify re-prompt occurs
- Mock Claude to return unfixable mappings on both attempts, verify `None` is returned

Integration tests for `harness.py` changes:
- Early-exit: all critical fields already mapped → Claude is NOT called (zero API calls)
- Claude fixes mappings: missing field in initial settings → Claude adds it → validation passes

- Full configure flow: unfixable mapping → `FieldMappingError` raised with clear message
