# Data Profile Enrichment

**Date:** 2026-05-13
**Branch:** `feature/data-profile-enrichment` (new, cut from master)
**Status:** Approved for implementation

---

## 1. Problem

The current data profile (`profile_data.py`) returns `available_columns` as a
flat list of column names. Claude has no information about column dtypes,
cardinality, or source file provenance. This caused Claude to add string
columns (`school`, `municipalname`) as model features, crashing LightGBM
which only accepts int/float/bool.

## 2. Design

### 2.1 Replace `available_columns` with `column_profiles`

Remove the `available_columns` key from the profile dict. Replace it with
`column_profiles`, a nested dict organized by source parquet file.

Each column entry contains:
- `dtype`: pandas dtype simplified to `"string"`, `"float"`, `"int"`, `"bool"`,
  or `"other"`
- `non_null`: count of non-null values (int)
- `unique`: count of unique values (int)

Example:

```json
{
  "column_profiles": {
    "cama_master": {
      "parid": {"dtype": "string", "non_null": 163965, "unique": 163965},
      "price": {"dtype": "float", "non_null": 120235, "unique": 8432},
      "class": {"dtype": "string", "non_null": 163965, "unique": 9}
    },
    "geo_parcels": {
      "propid": {"dtype": "string", "non_null": 156430, "unique": 156430},
      "valutotal": {"dtype": "float", "non_null": 155800, "unique": 42000},
      "municipalname": {"dtype": "string", "non_null": 156430, "unique": 73}
    },
    "cama_residential": {},
    "cama_commercial": {}
  }
}
```

### 2.2 Source file discovery

The profiler currently hardcodes `cama_master.parquet` and `geo_parcels.parquet`.
Extend it to scan all `.parquet` files in the `in/` directory. Each file becomes
a key in `column_profiles` (filename without extension).

### 2.3 Dtype simplification

Map pandas dtypes to simplified categories:

| Pandas dtype | Simplified |
|---|---|
| `int8`, `int16`, `int32`, `int64`, `Int8`..`Int64` | `"int"` |
| `float16`, `float32`, `float64`, `Float32`, `Float64` | `"float"` |
| `bool`, `boolean` | `"bool"` |
| `object`, `string`, `string[python]`, `string[pyarrow]` | `"string"` |
| `datetime64[*]`, `category`, anything else | `"other"` |

### 2.4 Claude prompt update

Update `_SYSTEM_CONFIGURE` in `claude_settings.py` to:
1. Reference `column_profiles` instead of `available_columns`
2. Add a rule: "Never add string-typed columns directly as model features.
   String columns must be listed in `field_classification.important` as
   categorical before they can be used."

Update `_SYSTEM_REFINE` similarly — add the same string-feature warning.

### 2.5 Backward compatibility

The `available_columns` key is removed. Any code referencing it must be
updated. The only consumers are `claude_settings.py` (system prompts that
mention "available_columns") and tests.

## 3. Files Changed

| File | Change |
|---|---|
| `scripts/profile_data.py` | Replace `available_columns` with `column_profiles`; scan all parquets in `in/`; add `_simplify_dtype()` helper |
| `scripts/claude_settings.py` | Update `_SYSTEM_CONFIGURE` and `_SYSTEM_REFINE` prompts to reference `column_profiles` and warn about string features |
| `tests/test_profile_data.py` | Update tests: replace `available_columns` assertions with `column_profiles` assertions; add dtype/cardinality tests |
| `tests/test_claude_settings.py` | No changes expected (tests mock Claude responses, don't inspect prompt content) |

## 4. What Doesn't Change

- `infer_jurisdiction_tier()` — unchanged
- `class_distribution`, `he_id_fill_rate_by_class`, `land_he_id_fill_rate_by_class` — unchanged
- `has_spatial_data`, `total_parcels`, `total_sales`, `annual_sales_volume` — unchanged
- `harness.py` — passes profile dict through, never inspects `available_columns`
- Claude response parsing and retry logic — unchanged
