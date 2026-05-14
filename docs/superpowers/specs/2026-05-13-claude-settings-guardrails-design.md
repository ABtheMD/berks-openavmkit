# Claude Settings Guardrails

**Date:** 2026-05-13
**Branch:** `feature/claude-settings-guardrails` (new, cut from master)
**Status:** Approved for implementation

---

## 1. Problem

Claude generates settings.json deltas via `claude_settings.py` (both `generate_initial()` and `refine_after_model()`). These deltas are applied unconditionally via `_merge_settings()` with no validation. When Claude produces invalid settings — adding string columns as features, referencing nonexistent columns, or producing malformed structure — the pipeline crashes during the model subprocess.

Error recovery (PR #12) catches these crashes and reverts settings, but the model iteration is wasted. Data profile enrichment (PR #13) gives Claude dtype information, but Claude can still ignore its instructions. We need programmatic validation that catches invalid deltas before they reach the pipeline.

## 2. Design

### 2.1 Validator function

Add a `validate_settings_delta(delta, data_profile)` function to `claude_settings.py`. It takes the parsed delta dict and the data profile (which contains `column_profiles` with dtype info for every column in every source file), runs all validation rules, and returns a result with two fields:

- `cleaned`: the delta with invalid parts stripped
- `violations`: a list of human-readable strings describing what was removed and why

If `violations` is empty, the delta is valid and can be applied as-is.

### 2.2 Validation rules

Rules are organized by severity. All rules run on every delta.

**Critical (would crash the pipeline):**

1. **String features** — Collect all column names from `column_profiles` that have `dtype: "string"`. Scan the delta for any new column references that are string-typed: columns added to `dep_vars`, columns implicitly included by not being in `exclude_features` for a group, or columns explicitly added in any feature list. If a string-typed column is referenced and is NOT declared as `"categorical"` in `field_classification.important` (checking both the delta and the existing base settings), add it to the relevant group's `exclude_features` list and report a violation. This rule focuses on columns the delta introduces or exposes, not columns already managed by existing settings.

2. **Nonexistent columns** — Build a set of all known column names across all source files in `column_profiles`. Any column referenced in `exclude_features`, `dep_vars`, or filter expressions that does not appear in this set is removed and reported as a violation.

**Structural (would produce malformed settings):**

3. **model_groups structure** — Each entry in `modeling.model_groups` must be a dict. If a group has a `filter` key, its value must be a list. The first element must be a valid operator string: one of `==`, `!=`, `>`, `<`, `>=`, `<=`, `isin`, `and`, `or`. The field name referenced in the filter (second element for comparison operators) must exist in `column_profiles`. Groups with invalid structure are removed entirely.

4. **skip values** — If a group has a `skip` key, it must be a list of strings. Each element must be one of `"all"`, `"model"`, `"report"`. Invalid skip values are removed from the list; if the entire value is not a list, it is removed.

5. **exclude_features type** — If a group has `exclude_features`, it must be a list of strings. If it is not a list, it is removed. Non-string elements within the list are removed.

6. **dep_vars type** — If the delta contains `dep_vars`, it must be a list of strings. Each entry must exist in `column_profiles` and have a numeric dtype (`int` or `float`). Invalid entries are removed.

**Semantic (warning only, not stripped):**

7. **Empty model_groups** — If the delta's `model_groups` would result in zero active (non-skipped) groups, log a warning but do not strip. Claude may be intentionally skipping all groups for a reason the validator cannot assess.

### 2.3 Known column set construction

The validator builds the known column set from `data_profile["column_profiles"]` by collecting all column names across all source files:

```python
known_columns = set()
for source, cols in data_profile["column_profiles"].items():
    known_columns.update(cols.keys())
```

The string-typed column set is built similarly, filtering for `dtype == "string"`.

### 2.4 Integration into public API

Both `generate_initial()` and `refine_after_model()` call `validate_settings_delta()` after `_call_with_retry()` returns the parsed delta. The flow:

1. Parse Claude's response (existing `_call_with_retry`)
2. Extract `settings` key from parsed dict (existing)
3. Call `validate_settings_delta(delta, data_profile)`
4. If violations:
   a. Log all violations to the reasoning JSONL file
   b. Build a re-prompt message listing the violations and the cleaned delta
   c. Call Claude again with the violations as context
   d. Parse and validate the second response
   e. Log any remaining violations
5. Return the cleaned delta (from whichever pass produced it)

### 2.5 Re-prompt message format

When violations are found, the re-prompt message is appended to the conversation:

```
Your settings delta had validation errors:
- [violation 1]
- [violation 2]

The invalid parts have been removed. The cleaned delta is:
```json
{cleaned_delta}
```

Please provide a corrected settings delta that addresses these issues.
Respond with ONLY a JSON object with keys "settings" and "reasoning".
```

### 2.6 Retry budget

The validation re-prompt is separate from the parse-failure retry in `_call_with_retry`. The maximum call sequence for a single stage is:

1. Initial call → parse failure → parse retry (existing `_call_with_retry`)
2. Successful parse → validation failure → re-prompt
3. Re-prompt response → parse failure → parse retry (existing `_call_with_retry`)
4. Successful parse → validation (final) → return cleaned

Maximum: 4 Claude API calls per stage. Typical: 1-2.

### 2.7 Violation logging

All violations are logged to the reasoning JSONL file using the existing `_write_reasoning()` helper, with `call_type` set to `"validation"`. This creates an audit trail of what Claude gets wrong over time.

## 3. Files Changed

| File | Change |
|---|---|
| `scripts/claude_settings.py` | Add `validate_settings_delta()` function; update `generate_initial()` and `refine_after_model()` to call it after parsing; add validation re-prompt logic |
| `tests/test_claude_settings.py` | Add tests for each validation rule; add integration tests for the validate-and-re-prompt flow |

## 4. What Doesn't Change

- `_merge_settings()` — unchanged, still does RFC 7396 merge
- `_extract_json_block()` — unchanged
- `_call_with_retry()` — unchanged (validation is a separate layer)
- `harness.py` — unchanged (validation happens inside `claude_settings.py`)
- `profile_data.py` — unchanged (already provides `column_profiles`)
- System prompts (`_SYSTEM_CONFIGURE`, `_SYSTEM_REFINE`) — unchanged (already warn about string features from PR #13)

## 5. Testing Strategy

Unit tests for `validate_settings_delta()`:
- Each of the 7 rules gets at least one test with a delta that triggers the rule
- Test that valid deltas pass through unchanged
- Test that multiple violations in one delta are all caught
- Test that the cleaned delta is structurally valid after stripping

Integration tests:
- Mock Claude to return a delta with string features, verify they are stripped and re-prompt is sent
- Mock Claude to return a valid delta, verify no re-prompt occurs
- Test the full flow: invalid first response → re-prompt → valid second response
