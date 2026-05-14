# Assessor Baseline Comparison

**Date:** 2026-05-14
**Branch:** `feature/assessor-baseline` (new, cut from master)
**Status:** Approved for implementation

---

## 1. Problem

The openavmkit pipeline produces model-based property valuations and evaluates them using IAAO ratio study metrics (median ratio, COD) against sale prices. But it never compares the model's performance against the assessor's existing values. Without this comparison, there's no way to know whether the model actually improves upon current assessments — which is the whole point of the exercise.

The infrastructure for this comparison largely exists: `ratio_study.py` already runs dual-track ratio studies (assessor vs model), and `pred_sales.parquet` already contains both `assr_ratio` and `prediction_ratio` columns. The gap is that the harness never reads or surfaces the assessor metrics.

## 2. Design

### 2.1 Approach: Read existing data, report comparison

After the model stage completes, read assessor ratio metrics from the same output files the pipeline already produces. Print a side-by-side comparison table showing assessor vs model performance per model group. Purely informational — no impact on pass/fail or iteration logic.

At configure time, warn if `assr_market_value` isn't mapped, since baseline comparison requires it.

### 2.2 Configure-time validation

Add `assr_market_value` to the `IMPORTANT_FIELDS` set in `scripts/validate_field_mapping.py`. The existing `validate_field_mapping()` function already checks this set and produces warnings for unmapped fields. No new code path needed — just a set membership change.

Current `IMPORTANT_FIELDS`:
```python
IMPORTANT_FIELDS = {"valid_sale", "vacant_sale", "he_id"}
```

Updated:
```python
IMPORTANT_FIELDS = {"valid_sale", "vacant_sale", "he_id", "assr_market_value"}
```

If `assr_market_value` is not mapped in any source's `load` or `calc` section, the existing machinery produces:
> `Missing important field 'assr_market_value': not mapped in any source's load or calc section`

This does not block the pipeline.

### 2.3 `_read_assessor_metrics()` — new function in `harness.py`

```python
def _read_assessor_metrics(locality_data_dir: Path) -> dict:
```

Mirrors `_read_model_metrics()` but reads from `pred_sales.parquet` (not `pred_test.parquet`) and uses the `assr_ratio` column (not `prediction_ratio`).

**Parameters:**
- `locality_data_dir` — path to the locality's data directory (e.g., `notebooks/pipeline/data/us-pa-berks`)

**Returns:** `{"group_name": {"median_ratio": float, "cod": float, "count": int}, ...}`

Same format as `_read_model_metrics()` for easy comparison.

**Logic:**
1. Iterate over `out/models/*/main/ensemble/pred_sales.parquet`
2. Read the `assr_ratio` column
3. Drop NaN values
4. If no valid ratios, skip the group
5. Compute median ratio and COD using the same formula as `_read_model_metrics()`:
   - `median = ratios.median()`
   - `cod = (|ratios - median|).mean() / median * 100`
6. Return dict keyed by group name

**Why pred_sales.parquet, not pred_test.parquet:** The assessor ratio (`assr_ratio`) is pre-computed in `pred_sales.parquet` during the ratio study phase. `pred_test.parquet` contains model prediction ratios but may not have `assr_ratio`. Using `pred_sales.parquet` ensures we get the assessor values that were used in the ratio study report.

**If `assr_ratio` column is missing:** Return empty dict for that group (the jurisdiction may not have assessor values mapped). This is a graceful skip, not an error.

### 2.4 Comparison output in `run_model()`

After the model iteration loop completes in `run_model()`, add a comparison section:

```python
model_metrics = _read_model_metrics(data_dir)
assessor_metrics = _read_assessor_metrics(data_dir)

if assessor_metrics:
    _print_baseline_comparison(model_metrics, assessor_metrics)
else:
    print("[harness] No assessor baseline available (assr_market_value not mapped).")
```

### 2.5 `_print_baseline_comparison()` — new function in `harness.py`

```python
def _print_baseline_comparison(model_metrics: dict, assessor_metrics: dict):
```

Prints a formatted comparison table to stdout:

```
[harness] === ASSESSOR BASELINE COMPARISON ===
  Group      Assessor COD   Model COD    Assessor Ratio   Model Ratio
  res        40.00          12.10        0.3600           1.0100
  com        32.10          18.50        0.4200           0.9800
```

**Logic:**
- Iterate over model groups that appear in both `model_metrics` and `assessor_metrics`
- For groups in model but not assessor (or vice versa), skip silently
- Column alignment uses fixed-width formatting
- No pass/fail indicators — this is purely informational

## 3. Files Changed

| File | Change |
|------|--------|
| `scripts/validate_field_mapping.py` | Add `assr_market_value` to `IMPORTANT_FIELDS` |
| `scripts/harness.py` | Add `_read_assessor_metrics()` and `_print_baseline_comparison()`; call after model iteration loop in `run_model()` |
| `tests/test_validate_field_mapping.py` | Test that missing `assr_market_value` produces warning |
| `tests/test_harness.py` | Tests for `_read_assessor_metrics()` and comparison output |

## 4. What Doesn't Change

- `_read_model_metrics()` — unchanged
- `_passes_iaao()` / `_best_iteration()` — unchanged (no impact on pass/fail logic)
- `ratio_study.py` — unchanged (already produces the data we read)
- `validate_sales_qualification()` — unchanged
- `refine_field_mapping()` — unchanged
- The model iteration loop logic — unchanged

## 5. Testing Strategy

**validate_field_mapping tests:**
- Missing `assr_market_value` produces a warning (not error)
- Present `assr_market_value` produces no warning

**_read_assessor_metrics tests:**
- Happy path: pred_sales.parquet with `assr_ratio` column → returns correct median_ratio and COD
- Missing `assr_ratio` column → returns empty dict for that group
- No models directory → returns empty dict
- Multiple model groups → returns metrics for each

**_print_baseline_comparison tests:**
- Both metrics populated → prints formatted table
- Empty assessor metrics → prints "no baseline" message
- Groups only in one dict → those groups are skipped

**Integration test:**
- After run_model with assessor data available → comparison table appears in output
