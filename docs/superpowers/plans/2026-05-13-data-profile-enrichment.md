# Data Profile Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the flat `available_columns` list in the data profile with a per-source-file `column_profiles` dict containing dtype, non_null count, and unique count for each column — so Claude knows which columns are strings and can avoid adding them as raw model features.

**Architecture:** Add a `_simplify_dtype()` helper and a `_profile_columns()` function to `profile_data.py` that scans all parquet files in `in/`. Update the `build_data_profile()` return dict. Update Claude system prompts in `claude_settings.py` to reference the new format and warn about string features. Update tests.

**Tech Stack:** Python, pandas, pytest

---

### Task 1: Add `_simplify_dtype` helper with tests

**Files:**
- Modify: `scripts/profile_data.py`
- Modify: `tests/test_profile_data.py`

- [ ] **Step 1: Write tests for dtype simplification**

Add these tests to `tests/test_profile_data.py`. First add the import at the top (after the existing `from profile_data import` line):

Change the import line from:
```python
from profile_data import build_data_profile, infer_jurisdiction_tier
```
to:
```python
from profile_data import build_data_profile, infer_jurisdiction_tier, _simplify_dtype
```

Then add these tests after `test_tier_rural_small`:

```python
def test_simplify_dtype_int():
    assert _simplify_dtype(pd.Series([1, 2, 3]).dtype) == "int"

def test_simplify_dtype_float():
    assert _simplify_dtype(pd.Series([1.0, 2.5]).dtype) == "float"

def test_simplify_dtype_bool():
    assert _simplify_dtype(pd.Series([True, False]).dtype) == "bool"

def test_simplify_dtype_string_object():
    assert _simplify_dtype(pd.Series(["a", "b"]).dtype) == "string"

def test_simplify_dtype_string_explicit():
    assert _simplify_dtype(pd.StringDtype()) == "string"

def test_simplify_dtype_datetime():
    assert _simplify_dtype(pd.Series(pd.to_datetime(["2020-01-01"])).dtype) == "other"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_profile_data.py::test_simplify_dtype_int -v`
Expected: FAIL with `ImportError` (function doesn't exist yet)

- [ ] **Step 3: Implement `_simplify_dtype`**

Add this function to `scripts/profile_data.py` after the `DATA_BASE_DIR` line and before `build_data_profile`:

```python
def _simplify_dtype(dtype) -> str:
    """Map a pandas/numpy dtype to a simplified category string."""
    name = str(dtype).lower()
    if "int" in name:
        return "int"
    if "float" in name:
        return "float"
    if name in ("bool", "boolean"):
        return "bool"
    if name in ("object", "string") or "string" in name:
        return "string"
    return "other"
```

- [ ] **Step 4: Run all dtype tests to verify they pass**

Run: `pytest tests/test_profile_data.py -k "simplify_dtype" -v`
Expected: All 6 pass

- [ ] **Step 5: Commit**

```bash
git add scripts/profile_data.py tests/test_profile_data.py
git commit -m "feat: add _simplify_dtype helper for column type classification"
```

### Task 2: Replace `available_columns` with `column_profiles`

**Files:**
- Modify: `scripts/profile_data.py:17-90`
- Modify: `tests/test_profile_data.py`

- [ ] **Step 1: Write tests for column_profiles structure**

Update the import in `tests/test_profile_data.py` to also import `_profile_columns`:

```python
from profile_data import build_data_profile, infer_jurisdiction_tier, _simplify_dtype, _profile_columns
```

Add these tests after the `test_simplify_dtype_*` tests:

```python
def test_profile_columns_returns_dict():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    result = _profile_columns(df)
    assert isinstance(result, dict)
    assert "a" in result
    assert "b" in result

def test_profile_columns_dtype():
    df = pd.DataFrame({"price": [100.0, 200.0], "name": ["a", "b"]})
    result = _profile_columns(df)
    assert result["price"]["dtype"] == "float"
    assert result["name"]["dtype"] == "string"

def test_profile_columns_non_null():
    df = pd.DataFrame({"a": [1, None, 3]})
    result = _profile_columns(df)
    assert result["a"]["non_null"] == 2

def test_profile_columns_unique():
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    result = _profile_columns(df)
    assert result["a"]["unique"] == 3

def test_profile_has_column_profiles_not_available_columns(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert "column_profiles" in profile
    assert "available_columns" not in profile

def test_profile_column_profiles_has_source_files(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    assert "cama_master" in profile["column_profiles"]
    assert "geo_parcels" in profile["column_profiles"]

def test_profile_column_profiles_master_columns(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    master = profile["column_profiles"]["cama_master"]
    assert "key" in master
    assert "sale_price" in master
    assert master["key"]["dtype"] == "string"

def test_profile_column_profiles_geo_columns(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    geo = profile["column_profiles"]["geo_parcels"]
    assert "lat" in geo
    assert geo["lat"]["dtype"] == "int"
```

- [ ] **Step 2: Run new tests to verify they fail**

Run: `pytest tests/test_profile_data.py::test_profile_has_column_profiles_not_available_columns -v`
Expected: FAIL (profile still has `available_columns`, not `column_profiles`)

- [ ] **Step 3: Implement `_profile_columns` and update `build_data_profile`**

Add `_profile_columns` to `scripts/profile_data.py` after `_simplify_dtype`:

```python
def _profile_columns(df: pd.DataFrame) -> dict:
    """Build a {column: {dtype, non_null, unique}} dict for a DataFrame."""
    result = {}
    for col in df.columns:
        result[col] = {
            "dtype": _simplify_dtype(df[col].dtype),
            "non_null": int(df[col].notna().sum()),
            "unique": int(df[col].nunique()),
        }
    return result
```

Then in `build_data_profile`, replace lines 75-77:

```python
    all_columns = list(df_master.columns)
    if not df_geo.empty:
        all_columns = sorted(set(all_columns) | set(df_geo.columns))
```

With:

```python
    column_profiles = {}
    for parquet_file in sorted(in_dir.glob("*.parquet")):
        source_name = parquet_file.stem
        df = pd.read_parquet(parquet_file)
        column_profiles[source_name] = _profile_columns(df)
```

And in the return dict, replace:

```python
        "available_columns": all_columns,
```

With:

```python
        "column_profiles": column_profiles,
```

- [ ] **Step 4: Update existing test for required keys**

In `tests/test_profile_data.py`, update `test_profile_returns_required_keys` — change `"available_columns"` to `"column_profiles"` in the required keys list:

```python
def test_profile_returns_required_keys(tmp_locality):
    profile = build_data_profile("fake-county", data_base_dir=tmp_locality)
    for key in [
        "locality", "total_parcels", "total_sales", "annual_sales_volume",
        "class_distribution", "he_id_fill_rate_by_class",
        "has_spatial_data", "column_profiles", "jurisdiction_tier",
    ]:
        assert key in profile, f"Missing key: {key}"
```

- [ ] **Step 5: Run the full test suite to verify all pass**

Run: `pytest tests/test_profile_data.py -v`
Expected: All tests pass (including updated existing tests)

- [ ] **Step 6: Commit**

```bash
git add scripts/profile_data.py tests/test_profile_data.py
git commit -m "feat: replace available_columns with per-source column_profiles"
```

### Task 3: Update Claude system prompts

**Files:**
- Modify: `scripts/claude_settings.py:41-83`

- [ ] **Step 1: Update `_SYSTEM_CONFIGURE` prompt**

In `scripts/claude_settings.py`, replace the `_SYSTEM_CONFIGURE` string (lines 41-62) with:

```python
_SYSTEM_CONFIGURE = """
You are a mass appraisal settings expert generating configuration for the
openavmkit pipeline. You will receive a data profile for a jurisdiction and
a partially-generated settings.json. Your job is to fill in the missing
sections — primarily model_groups filters and skip rules — using the
actual data, not guesses.

Rules:
1. model_groups: Map each unique class value to a group. Use the filter
   syntax ["==", "class", "<value>"]. Skip groups with <50 sales entirely
   (set skip: ["all"]).
2. HE fields (he_id, land_he_id): Check both he_id_fill_rate_by_class and
   land_he_id_fill_rate_by_class in the data profile. If either fill rate
   for a group is <0.05, add the corresponding field(s) to that group's
   exclude_features list. If has_spatial_data is true for the locality,
   flag spatial_he_inheritance=true for those groups instead of excluding.
3. Column types: The data profile includes column_profiles with dtype info
   for each source file. NEVER add string-typed columns directly as model
   features — they will crash LightGBM. String columns must first be listed
   in field_classification.important as "categorical" before the pipeline
   can encode and use them.
4. Respond with a JSON object with exactly two keys:
   - "settings": the settings delta (will be merged into settings.json)
   - "reasoning": a plain-text explanation of each decision you made

Return ONLY the JSON object. No preamble, no markdown prose outside the object.
""".strip()
```

- [ ] **Step 2: Update `_SYSTEM_REFINE` prompt**

In `scripts/claude_settings.py`, replace the `_SYSTEM_REFINE` string (lines 64-83) with:

```python
_SYSTEM_REFINE = """
You are a mass appraisal settings expert reviewing model results and
adjusting openavmkit pipeline settings to improve assessment quality.

{iaao_table}

Rules:
1. Read the model_metrics carefully. Identify which groups are outside
   their IAAO COD range for the jurisdiction tier.
2. For out-of-range groups, propose specific settings changes:
   - Add or remove dep_vars
   - Add or remove features from exclude_features
   - Adjust skip rules if a group has too few sales to model reliably
3. Column types: The data profile includes column_profiles with dtype info.
   NEVER add string-typed columns as model features — they will crash
   LightGBM. String columns must first be listed in
   field_classification.important as "categorical".
4. State which IAAO tier you assigned and which COD range you used.
5. Respond with a JSON object with exactly two keys:
   - "settings": the settings delta to merge into settings.json
   - "reasoning": explanation of each change and which IAAO threshold applies

Return ONLY the JSON object. No preamble, no markdown prose outside the object.
""".format(iaao_table=_IAAO_COD_TABLE).strip()
```

- [ ] **Step 3: Run the full test suite**

Run: `pytest tests/ -v --tb=short`
Expected: All tests pass. The `claude_settings` tests mock Claude responses and don't inspect prompt content, so they should pass unchanged.

- [ ] **Step 4: Commit**

```bash
git add scripts/claude_settings.py
git commit -m "feat: update Claude prompts to reference column_profiles and warn about string features"
```
