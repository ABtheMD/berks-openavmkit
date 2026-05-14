# Assessor Baseline Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface a side-by-side comparison of assessor vs model performance after the model stage, so users can see whether the model actually improves upon existing assessed values.

**Architecture:** Read assessor ratio metrics from `pred_sales.parquet` (already produced by the pipeline), compare against model metrics from `pred_test.parquet`, and print a formatted table. Add `assr_market_value` to the important-fields warning set so users are alerted if baseline comparison won't be available.

**Tech Stack:** Python, pandas, pytest

---

## Files

| File | Change |
|------|--------|
| `scripts/validate_field_mapping.py:70` | Add `"assr_market_value"` to `IMPORTANT_FIELDS` set |
| `tests/test_validate_field_mapping.py` | Add test that missing `assr_market_value` produces warning |
| `scripts/harness.py:191-215` | Add `_read_assessor_metrics()` function (mirrors `_read_model_metrics()`) |
| `scripts/harness.py:86-90` | Add `_print_baseline_comparison()` function |
| `scripts/harness.py:491-568` | Modify `run_model()` to call baseline comparison after iteration loop |
| `tests/test_harness.py` | Add tests for `_read_assessor_metrics()`, `_print_baseline_comparison()`, and integration |

---

### Task 1: Add `assr_market_value` to `IMPORTANT_FIELDS`

**Files:**
- Modify: `scripts/validate_field_mapping.py:70`
- Test: `tests/test_validate_field_mapping.py`

- [ ] **Step 1: Write the failing test**

Add at the end of the "Check 1" test section in `tests/test_validate_field_mapping.py` (after the `test_check1_missing_he_id_is_warning` test around line 224):

```python
def test_check1_missing_assr_market_value_is_warning():
    """Missing 'assr_market_value' should produce a warning, not an error."""
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
    assert any("assr_market_value" in w for w in result["warnings"])
    # Should NOT be in errors
    assert not any("assr_market_value" in e for e in result["errors"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_validate_field_mapping.py::test_check1_missing_assr_market_value_is_warning -v`
Expected: FAIL — `assr_market_value` is not yet in `IMPORTANT_FIELDS`, so no warning is produced.

- [ ] **Step 3: Add `assr_market_value` to `IMPORTANT_FIELDS`**

In `scripts/validate_field_mapping.py`, line 70, change:

```python
IMPORTANT_FIELDS = {"valid_sale", "vacant_sale", "he_id"}
```

to:

```python
IMPORTANT_FIELDS = {"valid_sale", "vacant_sale", "he_id", "assr_market_value"}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_validate_field_mapping.py::test_check1_missing_assr_market_value_is_warning -v`
Expected: PASS

- [ ] **Step 5: Write test that present `assr_market_value` produces no warning**

Add immediately after the previous test:

```python
def test_check1_assr_market_value_present_no_warning():
    """When assr_market_value is mapped, no warning about it is produced."""
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
                        "assr_market_value": ["asint", "sale_price"],
                    },
                },
            }
        }
    }
    result = validate_field_mapping(settings, FIELD_MAPPING_PROFILE)
    assert not any("assr_market_value" in w for w in result["warnings"])
```

- [ ] **Step 6: Run both assr_market_value tests**

Run: `python -m pytest tests/test_validate_field_mapping.py -k "assr_market_value" -v`
Expected: Both PASS

- [ ] **Step 7: Run full validate_field_mapping test suite**

Run: `python -m pytest tests/test_validate_field_mapping.py -v`
Expected: All tests pass (existing tests use `COMPLETE_SETTINGS` which does not include `assr_market_value`, so they get an additional warning which they don't assert against).

- [ ] **Step 8: Commit**

```bash
git add scripts/validate_field_mapping.py tests/test_validate_field_mapping.py
git commit -m "feat: warn when assr_market_value is unmapped

Add assr_market_value to IMPORTANT_FIELDS so the field mapping
validator warns users that assessor baseline comparison won't
be available without it."
```

---

### Task 2: Add `_read_assessor_metrics()` and `_print_baseline_comparison()`

**Files:**
- Modify: `scripts/harness.py:191-215` (add new function after `_read_model_metrics`)
- Test: `tests/test_harness.py`

- [ ] **Step 1: Write the failing test for `_read_assessor_metrics` happy path**

Add a new test section in `tests/test_harness.py` after the existing "Metric reading" section (after line 118):

```python
# ---------------------------------------------------------------------------
# Assessor metrics reading
# ---------------------------------------------------------------------------

def test_read_assessor_metrics_returns_dict(tmp_path):
    """Happy path: pred_sales.parquet with assr_ratio column returns metrics."""
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    df = pd.DataFrame({"assr_ratio": [0.36, 0.40, 0.38, 0.42, 0.35]})
    df.to_parquet(group_dir / "pred_sales.parquet", index=False)

    metrics = harness._read_assessor_metrics(tmp_path)
    assert "res" in metrics
    assert "median_ratio" in metrics["res"]
    assert "cod" in metrics["res"]
    assert "count" in metrics["res"]
    assert metrics["res"]["count"] == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_harness.py::test_read_assessor_metrics_returns_dict -v`
Expected: FAIL — `_read_assessor_metrics` does not exist yet.

- [ ] **Step 3: Write the minimal `_read_assessor_metrics` implementation**

In `scripts/harness.py`, add immediately after `_read_model_metrics()` (after line 215):

```python
def _read_assessor_metrics(locality_data_dir: Path) -> dict:
    """Read assessor ratio metrics from pred_sales.parquet files.

    Mirrors _read_model_metrics() but reads assr_ratio from
    pred_sales.parquet instead of prediction_ratio from pred_test.parquet.
    Returns {"group_name": {"median_ratio": float, "cod": float, "count": int}}.
    """
    models_dir = locality_data_dir / "out" / "models"
    if not models_dir.exists():
        return {}

    metrics = {}
    for group_dir in sorted(models_dir.iterdir()):
        if not group_dir.is_dir():
            continue
        pred_path = group_dir / "main" / "ensemble" / "pred_sales.parquet"
        if not pred_path.exists():
            continue
        try:
            df = pd.read_parquet(pred_path, columns=["assr_ratio"])
        except (KeyError, ValueError):
            # assr_ratio column not present in this parquet
            continue
        ratios = df["assr_ratio"].dropna()
        if len(ratios) == 0:
            continue
        median = float(ratios.median())
        cod = float((ratios - median).abs().mean() / median * 100)
        metrics[group_dir.name] = {
            "median_ratio": round(median, 4),
            "cod": round(cod, 2),
            "count": len(ratios),
        }

    return metrics
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_harness.py::test_read_assessor_metrics_returns_dict -v`
Expected: PASS

- [ ] **Step 5: Write additional tests for `_read_assessor_metrics`**

Add these tests after the happy-path test:

```python
def test_read_assessor_metrics_cod_formula(tmp_path):
    """COD = mean of absolute deviations from median / median x 100."""
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    # Ratios: [0.80, 1.00, 1.00, 1.00, 1.20] -> median=1.00
    # Absolute deviations: [0.20, 0.00, 0.00, 0.00, 0.20]
    # Mean of deviations: 0.08
    # COD = 0.08 / 1.00 * 100 = 8.0
    df = pd.DataFrame({"assr_ratio": [0.80, 1.00, 1.00, 1.00, 1.20]})
    df.to_parquet(group_dir / "pred_sales.parquet", index=False)

    metrics = harness._read_assessor_metrics(tmp_path)
    assert metrics["res"]["cod"] == pytest.approx(8.0, abs=0.1)


def test_read_assessor_metrics_missing_column(tmp_path):
    """pred_sales.parquet without assr_ratio column returns empty dict for group."""
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    df = pd.DataFrame({"prediction_ratio": [1.0, 1.1]})
    df.to_parquet(group_dir / "pred_sales.parquet", index=False)

    metrics = harness._read_assessor_metrics(tmp_path)
    assert metrics == {}


def test_read_assessor_metrics_no_models_dir(tmp_path):
    """No models directory returns empty dict."""
    metrics = harness._read_assessor_metrics(tmp_path)
    assert metrics == {}


def test_read_assessor_metrics_multiple_groups(tmp_path):
    """Multiple model groups each get their own metrics."""
    for group_name in ["res", "com"]:
        group_dir = tmp_path / "out" / "models" / group_name / "main" / "ensemble"
        group_dir.mkdir(parents=True)
        df = pd.DataFrame({"assr_ratio": [0.90, 1.00, 1.10]})
        df.to_parquet(group_dir / "pred_sales.parquet", index=False)

    metrics = harness._read_assessor_metrics(tmp_path)
    assert "res" in metrics
    assert "com" in metrics
    assert metrics["res"]["count"] == 3
    assert metrics["com"]["count"] == 3


def test_read_assessor_metrics_all_nan(tmp_path):
    """All-NaN assr_ratio column is skipped (returns empty for that group)."""
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    df = pd.DataFrame({"assr_ratio": [float("nan"), float("nan")]})
    df.to_parquet(group_dir / "pred_sales.parquet", index=False)

    metrics = harness._read_assessor_metrics(tmp_path)
    assert metrics == {}
```

- [ ] **Step 6: Run all assessor metrics tests**

Run: `python -m pytest tests/test_harness.py -k "test_read_assessor" -v`
Expected: All 6 tests PASS

- [ ] **Step 7: Write the test for `_print_baseline_comparison` — both populated**

Add a new test section after the assessor metrics tests:

```python
# ---------------------------------------------------------------------------
# Baseline comparison output
# ---------------------------------------------------------------------------

def test_print_baseline_comparison_table(capsys):
    """Both metrics populated prints formatted comparison table."""
    model_metrics = {
        "res": {"median_ratio": 1.01, "cod": 12.10, "count": 500},
        "com": {"median_ratio": 0.98, "cod": 18.50, "count": 100},
    }
    assessor_metrics = {
        "res": {"median_ratio": 0.36, "cod": 40.00, "count": 500},
        "com": {"median_ratio": 0.42, "cod": 32.10, "count": 100},
    }

    harness._print_baseline_comparison(model_metrics, assessor_metrics)

    captured = capsys.readouterr()
    assert "ASSESSOR BASELINE COMPARISON" in captured.out
    assert "res" in captured.out
    assert "com" in captured.out
    assert "40.00" in captured.out   # assessor COD for res
    assert "12.10" in captured.out   # model COD for res
    assert "0.3600" in captured.out  # assessor ratio for res
    assert "1.0100" in captured.out  # model ratio for res
```

- [ ] **Step 8: Write `_print_baseline_comparison` implementation**

In `scripts/harness.py`, add after `_read_assessor_metrics()`:

```python
def _print_baseline_comparison(model_metrics: dict, assessor_metrics: dict):
    """Print a side-by-side comparison of assessor vs model performance."""
    # Only compare groups that appear in both dicts
    common_groups = sorted(
        set(model_metrics.keys()) & set(assessor_metrics.keys())
    )
    if not common_groups:
        return

    print("[harness] === ASSESSOR BASELINE COMPARISON ===")
    print(
        f"  {'Group':<12} {'Assessor COD':>14} {'Model COD':>11} "
        f"{'Assessor Ratio':>16} {'Model Ratio':>13}"
    )
    for group in common_groups:
        a = assessor_metrics[group]
        m = model_metrics[group]
        print(
            f"  {group:<12} {a['cod']:>14.2f} {m['cod']:>11.2f} "
            f"{a['median_ratio']:>16.4f} {m['median_ratio']:>13.4f}"
        )
```

- [ ] **Step 9: Run the comparison table test**

Run: `python -m pytest tests/test_harness.py::test_print_baseline_comparison_table -v`
Expected: PASS

- [ ] **Step 10: Write test for groups only in one dict**

```python
def test_print_baseline_comparison_skips_unmatched_groups(capsys):
    """Groups only in one dict are silently skipped."""
    model_metrics = {
        "res": {"median_ratio": 1.00, "cod": 10.0, "count": 500},
        "ind": {"median_ratio": 1.05, "cod": 15.0, "count": 50},
    }
    assessor_metrics = {
        "res": {"median_ratio": 0.40, "cod": 35.0, "count": 500},
        "ag":  {"median_ratio": 0.50, "cod": 45.0, "count": 30},
    }

    harness._print_baseline_comparison(model_metrics, assessor_metrics)

    captured = capsys.readouterr()
    assert "res" in captured.out
    assert "ind" not in captured.out
    assert "ag" not in captured.out
```

- [ ] **Step 11: Write test for no common groups**

```python
def test_print_baseline_comparison_no_common_groups(capsys):
    """No overlapping groups prints nothing."""
    model_metrics = {"res": {"median_ratio": 1.00, "cod": 10.0, "count": 500}}
    assessor_metrics = {"com": {"median_ratio": 0.40, "cod": 35.0, "count": 100}}

    harness._print_baseline_comparison(model_metrics, assessor_metrics)

    captured = capsys.readouterr()
    assert captured.out == ""
```

- [ ] **Step 12: Run all comparison tests**

Run: `python -m pytest tests/test_harness.py -k "test_print_baseline" -v`
Expected: All 3 tests PASS

- [ ] **Step 13: Run full test suite**

Run: `python -m pytest tests/test_harness.py -v`
Expected: All tests pass

- [ ] **Step 14: Commit**

```bash
git add scripts/harness.py tests/test_harness.py
git commit -m "feat: add _read_assessor_metrics and _print_baseline_comparison

_read_assessor_metrics mirrors _read_model_metrics but reads
assr_ratio from pred_sales.parquet. _print_baseline_comparison
prints a side-by-side table of assessor vs model COD and median
ratio per model group."
```

---

### Task 3: Integrate baseline comparison into `run_model()`

**Files:**
- Modify: `scripts/harness.py:491-568` (modify `run_model()` function)
- Test: `tests/test_harness.py`

The key challenge: `run_model()` currently has an early `return` at line 539 when IAAO passes. The comparison code must fire in both the early-return and loop-exhaustion cases. Solution: replace `return` with `break`, and put comparison code after the for loop.

- [ ] **Step 1: Write the integration test — comparison after IAAO pass**

Add a new test section at the end of `tests/test_harness.py`:

```python
# ---------------------------------------------------------------------------
# Assessor baseline integration in run_model
# ---------------------------------------------------------------------------

def test_run_model_prints_baseline_comparison(monkeypatch, tmp_path, capsys):
    """After model completes, assessor baseline comparison appears in output."""
    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)

    data_dir = tmp_path / "us-pa-berks"
    data_dir.mkdir()
    settings_path = data_dir / "in" / "settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(json.dumps({"modeling": {"model_groups": {"res": {}}}}))

    monkeypatch.setattr(harness, "_NOTEBOOKS_PIPELINE", tmp_path)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)
    monkeypatch.setattr(harness, "_settings_path", lambda loc: settings_path)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"jurisdiction_tier": "large_to_mid"},
    )

    # Create pred_test.parquet (model metrics) and pred_sales.parquet (assessor metrics)
    group_dir = data_dir / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    pd.DataFrame({"prediction_ratio": [0.95, 1.00, 1.05]}).to_parquet(
        group_dir / "pred_test.parquet", index=False
    )
    pd.DataFrame({"assr_ratio": [0.36, 0.40, 0.38]}).to_parquet(
        group_dir / "pred_sales.parquet", index=False
    )

    # Force IAAO pass so we hit the early exit path
    monkeypatch.setattr(harness, "_passes_iaao", lambda *a, **kw: True)

    harness.run_model("us-pa-berks", iteration_count=3, verbose=False)

    captured = capsys.readouterr()
    assert "ASSESSOR BASELINE COMPARISON" in captured.out
    assert "res" in captured.out
```

- [ ] **Step 2: Run integration test to verify it fails**

Run: `python -m pytest tests/test_harness.py::test_run_model_prints_baseline_comparison -v`
Expected: FAIL — `run_model()` does not yet call baseline comparison.

- [ ] **Step 3: Modify `run_model()` to add baseline comparison**

In `scripts/harness.py`, modify `run_model()` (lines 491-568). Three changes:

**Change 1:** Replace `return` on line 539 with `break`:

```python
        if _passes_iaao(metrics, jurisdiction_tier):
            print(f"[harness] All groups within IAAO range. Stopping after {i + 1} run(s).")
            break
```

**Change 2:** Add baseline comparison after the for loop. The current code after the for loop (lines 558-567) is:

```python
    if not metrics_history:
        print(f"\n[harness] No model iterations completed.", file=sys.stderr)
    else:
        best = _best_iteration(metrics_history, jurisdiction_tier)
        print(
            f"\n[harness] {iteration_count} iteration(s) exhausted. "
            f"Best results were in iteration {best}. "
            f"See out/settings_iter_{best - 1}.json for those settings.",
            file=sys.stderr,
        )
```

This `if/else` block only matters when the loop was NOT broken out of early (i.e., IAAO never passed). After a `break`, `metrics_history` will be non-empty but we don't want the "exhausted" message. Use the `for/else` pattern: the `else` clause of a for loop runs only if the loop was NOT broken out of.

Replace the code after the for loop with:

```python
    else:
        # for/else: runs only when the loop was NOT broken (IAAO never passed)
        if not metrics_history:
            print(f"\n[harness] No model iterations completed.", file=sys.stderr)
        else:
            best = _best_iteration(metrics_history, jurisdiction_tier)
            print(
                f"\n[harness] {iteration_count} iteration(s) exhausted. "
                f"Best results were in iteration {best}. "
                f"See out/settings_iter_{best - 1}.json for those settings.",
                file=sys.stderr,
            )

    # Assessor baseline comparison (runs regardless of how loop exited)
    model_metrics = _read_model_metrics(data_dir)
    assessor_metrics = _read_assessor_metrics(data_dir)

    if assessor_metrics:
        _print_baseline_comparison(model_metrics, assessor_metrics)
    else:
        print("[harness] No assessor baseline available (assr_market_value not mapped).")
```

The complete `run_model()` function after all changes:

```python
def run_model(locality: str, iteration_count: int, verbose: bool):
    print(f"[harness] === MODEL (up to {iteration_count} iterations) ===")
    from claude_settings import refine_after_model, ClaudeParseError
    from profile_data import build_data_profile

    data_dir = _locality_data_dir(locality)
    reasoning_log = data_dir / "out" / "claude_reasoning.jsonl"
    reasoning_log.parent.mkdir(parents=True, exist_ok=True)

    data_profile = build_data_profile(locality, data_base_dir=data_dir.parent)
    jurisdiction_tier = data_profile.get("jurisdiction_tier", "large_to_mid")

    metrics_history = []

    for i in range(iteration_count):
        print(f"[harness] --- Model run {i + 1}/{iteration_count} ---")

        _clear_model_checkpoints(data_dir)

        current_settings = _load_settings(locality)
        _save_settings_snapshot(data_dir, i, current_settings)

        rc = _run_subprocess(
            _SCRIPTS_DIR / "_run_model.py", locality, verbose,
        )
        if rc != 0:
            print(
                f"[harness] WARNING: model run {i + 1} failed (exit {rc}). "
                f"Reverting settings.",
                file=sys.stderr,
            )
            _save_settings(locality, current_settings)
            continue

        metrics = _read_model_metrics(data_dir)
        _save_metrics_snapshot(data_dir, i + 1, metrics)
        metrics_history.append({i + 1: metrics})

        if not metrics:
            print(f"[harness] WARNING: No model metrics found after run {i + 1}.", file=sys.stderr)
            if i < iteration_count - 1:
                print(f"[harness] Continuing to next iteration...", file=sys.stderr)
            continue

        _print_metrics_summary(metrics, jurisdiction_tier)

        if _passes_iaao(metrics, jurisdiction_tier):
            print(f"[harness] All groups within IAAO range. Stopping after {i + 1} run(s).")
            break

        if i < iteration_count - 1:
            print(f"[harness] Metrics outside IAAO range. Calling Claude for iteration {i + 2}...")
            try:
                delta = refine_after_model(
                    data_profile, current_settings, metrics,
                    iteration=i + 1, reasoning_log=reasoning_log,
                )
                merged = _merge_settings(current_settings, delta)
                _save_settings(locality, merged)
                print(f"[harness] settings.json updated for iteration {i + 2}.")
            except ClaudeParseError as e:
                print(f"[harness] WARNING: Claude refinement failed: {e}", file=sys.stderr)
                print(f"[harness] Continuing with current settings.", file=sys.stderr)
            except Exception as e:
                print(f"[harness] WARNING: Claude API call failed: {type(e).__name__}: {e}", file=sys.stderr)
                print(f"[harness] Continuing with current settings.", file=sys.stderr)
    else:
        # for/else: runs only when the loop was NOT broken (IAAO never passed)
        if not metrics_history:
            print(f"\n[harness] No model iterations completed.", file=sys.stderr)
        else:
            best = _best_iteration(metrics_history, jurisdiction_tier)
            print(
                f"\n[harness] {iteration_count} iteration(s) exhausted. "
                f"Best results were in iteration {best}. "
                f"See out/settings_iter_{best - 1}.json for those settings.",
                file=sys.stderr,
            )

    # Assessor baseline comparison (runs regardless of how loop exited)
    model_metrics = _read_model_metrics(data_dir)
    assessor_metrics = _read_assessor_metrics(data_dir)

    if assessor_metrics:
        _print_baseline_comparison(model_metrics, assessor_metrics)
    else:
        print("[harness] No assessor baseline available (assr_market_value not mapped).")
```

- [ ] **Step 4: Run integration test to verify it passes**

Run: `python -m pytest tests/test_harness.py::test_run_model_prints_baseline_comparison -v`
Expected: PASS

- [ ] **Step 5: Write test — baseline comparison after loop exhaustion**

```python
def test_run_model_prints_baseline_after_exhaustion(monkeypatch, tmp_path, capsys):
    """Baseline comparison also prints when iterations are exhausted (not just IAAO pass)."""
    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)

    data_dir = tmp_path / "us-pa-berks"
    data_dir.mkdir()
    settings_path = data_dir / "in" / "settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(json.dumps({"modeling": {"model_groups": {"res": {}}}}))

    monkeypatch.setattr(harness, "_NOTEBOOKS_PIPELINE", tmp_path)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)
    monkeypatch.setattr(harness, "_settings_path", lambda loc: settings_path)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"jurisdiction_tier": "large_to_mid"},
    )

    group_dir = data_dir / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    pd.DataFrame({"prediction_ratio": [0.95, 1.00, 1.05]}).to_parquet(
        group_dir / "pred_test.parquet", index=False
    )
    pd.DataFrame({"assr_ratio": [0.36, 0.40, 0.38]}).to_parquet(
        group_dir / "pred_sales.parquet", index=False
    )

    # Force IAAO to always fail so the loop exhausts
    monkeypatch.setattr(harness, "_passes_iaao", lambda *a, **kw: False)
    monkeypatch.setattr(
        "claude_settings.refine_after_model",
        lambda *a, **kw: {},
    )

    harness.run_model("us-pa-berks", iteration_count=2, verbose=False)

    captured = capsys.readouterr()
    assert "ASSESSOR BASELINE COMPARISON" in captured.out
    assert "exhausted" in captured.err  # loop exhaustion message still appears
```

- [ ] **Step 6: Run the exhaustion test**

Run: `python -m pytest tests/test_harness.py::test_run_model_prints_baseline_after_exhaustion -v`
Expected: PASS

- [ ] **Step 7: Write test — no assessor data prints fallback message**

```python
def test_run_model_no_assessor_data_prints_fallback(monkeypatch, tmp_path, capsys):
    """When no assessor data is available, prints fallback message."""
    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)

    data_dir = tmp_path / "us-pa-berks"
    data_dir.mkdir()
    settings_path = data_dir / "in" / "settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(json.dumps({"modeling": {"model_groups": {"res": {}}}}))

    monkeypatch.setattr(harness, "_NOTEBOOKS_PIPELINE", tmp_path)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)
    monkeypatch.setattr(harness, "_settings_path", lambda loc: settings_path)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"jurisdiction_tier": "large_to_mid"},
    )

    # Only create pred_test.parquet (no pred_sales.parquet)
    group_dir = data_dir / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    pd.DataFrame({"prediction_ratio": [0.95, 1.00, 1.05]}).to_parquet(
        group_dir / "pred_test.parquet", index=False
    )

    monkeypatch.setattr(harness, "_passes_iaao", lambda *a, **kw: True)

    harness.run_model("us-pa-berks", iteration_count=1, verbose=False)

    captured = capsys.readouterr()
    assert "No assessor baseline available" in captured.out
    assert "ASSESSOR BASELINE COMPARISON" not in captured.out
```

- [ ] **Step 8: Run the fallback test**

Run: `python -m pytest tests/test_harness.py::test_run_model_no_assessor_data_prints_fallback -v`
Expected: PASS

- [ ] **Step 9: Verify existing run_model tests still pass**

The `return` → `break` change could affect existing tests. Run them to verify:

Run: `python -m pytest tests/test_harness.py -k "test_model_recovery" -v`
Expected: All 3 recovery tests PASS. The recovery tests either:
- Force IAAO to fail (so `break` is never hit — same behavior as before)
- Never produce metrics (subprocess fails — `continue` path, not `return`/`break`)

- [ ] **Step 10: Run full test suite**

Run: `python -m pytest tests/test_harness.py tests/test_validate_field_mapping.py -v`
Expected: All tests pass

- [ ] **Step 11: Commit**

```bash
git add scripts/harness.py tests/test_harness.py
git commit -m "feat: integrate assessor baseline comparison into run_model

After the model iteration loop completes (whether by IAAO pass
or loop exhaustion), print a side-by-side comparison of assessor
vs model COD and median ratio per model group. Purely
informational — no impact on pass/fail logic.

Uses for/else pattern: 'iterations exhausted' message only prints
when IAAO never passed. Baseline comparison always prints."
```
