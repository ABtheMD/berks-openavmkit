"""Tests for scripts/harness.py — subprocess calls are mocked."""
import sys
import json
import pytest
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import harness


# ---------------------------------------------------------------------------
# Stage ordering
# ---------------------------------------------------------------------------

def test_stages_are_ordered():
    assert list(harness.STAGES.keys()) == ["download", "configure", "assemble", "clean", "model"]

def test_stage_index_download():
    assert harness._stage_index("download") == 0

def test_stage_index_model():
    assert harness._stage_index("model") == 4

def test_stage_index_invalid_raises():
    with pytest.raises(SystemExit):
        harness._stage_index("nonexistent")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def test_parse_args_defaults():
    args = harness._parse_args(["us-pa-berks"])
    assert args.locality == "us-pa-berks"
    assert args.from_stage == "download"
    assert args.to_stage == "model"
    assert args.iteration_count == 3
    assert args.verbose is False

def test_parse_args_custom_from_to():
    args = harness._parse_args(["us-pa-berks", "--from", "assemble", "--to", "clean"])
    assert args.from_stage == "assemble"
    assert args.to_stage == "clean"

def test_parse_args_iteration_count():
    args = harness._parse_args(["us-pa-berks", "--iteration-count", "5"])
    assert args.iteration_count == 5

def test_parse_args_verbose():
    args = harness._parse_args(["us-pa-berks", "--verbose"])
    assert args.verbose is True


# ---------------------------------------------------------------------------
# Seed file resolution
# ---------------------------------------------------------------------------

def test_seed_path_resolution():
    path = harness._seed_path("us-pa-berks")
    assert path.name == "seed_us-pa-berks.json"
    assert "seeds" in str(path)

def test_seed_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(harness, "_SEEDS_DIR", tmp_path)
    with pytest.raises(SystemExit):
        harness._check_seed("us-xx-fake")


# ---------------------------------------------------------------------------
# Stage range validation
# ---------------------------------------------------------------------------

def test_from_after_to_raises():
    with pytest.raises(SystemExit):
        harness._validate_stage_range("model", "download")

def test_equal_from_to_ok():
    harness._validate_stage_range("assemble", "assemble")


# ---------------------------------------------------------------------------
# Metric reading
# ---------------------------------------------------------------------------

def test_read_metrics_returns_dict(tmp_path):
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    df = pd.DataFrame({"prediction_ratio": [0.95, 1.05, 1.00, 1.10, 0.90]})
    df.to_parquet(group_dir / "pred_test.parquet", index=False)

    metrics = harness._read_model_metrics(tmp_path)
    assert "res" in metrics
    assert "median_ratio" in metrics["res"]
    assert "cod" in metrics["res"]
    assert "count" in metrics["res"]
    assert metrics["res"]["count"] == 5

def test_read_metrics_cod_uses_mean_not_median(tmp_path):
    """COD = mean of absolute deviations from median / median × 100 (IAAO standard)."""
    group_dir = tmp_path / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    # Ratios: [0.80, 1.00, 1.00, 1.00, 1.20] → median=1.00
    # Absolute deviations: [0.20, 0.00, 0.00, 0.00, 0.20]
    # Mean of deviations: 0.08, Median of deviations: 0.00
    # COD (correct, using mean) = 0.08 / 1.00 * 100 = 8.0
    # COD (wrong, using median) = 0.00 / 1.00 * 100 = 0.0
    df = pd.DataFrame({"prediction_ratio": [0.80, 1.00, 1.00, 1.00, 1.20]})
    df.to_parquet(group_dir / "pred_test.parquet", index=False)

    metrics = harness._read_model_metrics(tmp_path)
    assert metrics["res"]["cod"] == pytest.approx(8.0, abs=0.1)

def test_read_metrics_no_models_returns_empty(tmp_path):
    (tmp_path / "out" / "models").mkdir(parents=True)
    metrics = harness._read_model_metrics(tmp_path)
    assert metrics == {}


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


def test_print_baseline_comparison_no_common_groups(capsys):
    """No overlapping groups prints nothing."""
    model_metrics = {"res": {"median_ratio": 1.00, "cod": 10.0, "count": 500}}
    assessor_metrics = {"com": {"median_ratio": 0.40, "cod": 35.0, "count": 100}}

    harness._print_baseline_comparison(model_metrics, assessor_metrics)

    captured = capsys.readouterr()
    assert captured.out == ""


# ---------------------------------------------------------------------------
# _merge_settings
# ---------------------------------------------------------------------------

def test_merge_settings_adds_key():
    base = {"a": 1}
    delta = {"b": 2}
    assert harness._merge_settings(base, delta) == {"a": 1, "b": 2}

def test_merge_settings_overrides_key():
    base = {"a": 1}
    delta = {"a": 99}
    assert harness._merge_settings(base, delta) == {"a": 99}

def test_merge_settings_recursive():
    base = {"modeling": {"x": 1, "y": 2}}
    delta = {"modeling": {"y": 99, "z": 3}}
    result = harness._merge_settings(base, delta)
    assert result == {"modeling": {"x": 1, "y": 99, "z": 3}}

def test_merge_settings_preserves_base_nested():
    base = {"modeling": {"model_groups": {"res": {"name": "Res"}}}}
    delta = {"modeling": {"model_groups": {"com": {"name": "Com"}}}}
    result = harness._merge_settings(base, delta)
    assert "res" in result["modeling"]["model_groups"]
    assert "com" in result["modeling"]["model_groups"]

def test_merge_settings_null_deletes_key():
    base = {"modeling": {"skip": {"com": ["all"]}, "x": 1}}
    delta = {"modeling": {"skip": None}}
    result = harness._merge_settings(base, delta)
    assert "skip" not in result["modeling"]
    assert result["modeling"]["x"] == 1


# ---------------------------------------------------------------------------
# Checkpoint clearing
# ---------------------------------------------------------------------------

def test_clear_model_checkpoints_deletes_3_model_files(tmp_path):
    checkpoints = tmp_path / "out" / "checkpoints"
    checkpoints.mkdir(parents=True)
    (checkpoints / "3-model-00-enrich-spatial-lag.pickle").touch()
    (checkpoints / "3-model-02-finalize-models.pickle").touch()
    (checkpoints / "2-clean-01-process_sales.pickle").touch()

    harness._clear_model_checkpoints(tmp_path)

    remaining = list(checkpoints.iterdir())
    assert len(remaining) == 1
    assert remaining[0].name == "2-clean-01-process_sales.pickle"


# ---------------------------------------------------------------------------
# Settings snapshot
# ---------------------------------------------------------------------------

def test_save_settings_snapshot(tmp_path):
    settings = {"modeling": {"model_groups": {"res": {}}}}
    harness._save_settings_snapshot(tmp_path, 0, settings)
    snap = tmp_path / "out" / "settings_iter_0.json"
    assert snap.exists()
    assert json.loads(snap.read_text())["modeling"]["model_groups"]["res"] == {}


# ---------------------------------------------------------------------------
# Best iteration
# ---------------------------------------------------------------------------

def test_best_iteration_perfect_ratio():
    metrics_history = [
        {1: {"res": {"median_ratio": 1.10, "cod": 30.0, "count": 100}}},
        {2: {"res": {"median_ratio": 1.00, "cod": 12.0, "count": 100}}},
    ]
    assert harness._best_iteration(metrics_history) == 2

def test_best_iteration_single_run():
    metrics_history = [{1: {"res": {"median_ratio": 1.05, "cod": 20.0, "count": 100}}}]
    assert harness._best_iteration(metrics_history) == 1


# ---------------------------------------------------------------------------
# IAAO pass/fail
# ---------------------------------------------------------------------------

def test_passes_iaao_good_metrics():
    metrics = {"res": {"median_ratio": 1.00, "cod": 10.0, "count": 500}}
    assert harness._passes_iaao(metrics, jurisdiction_tier="large_to_mid") is True

def test_fails_iaao_high_cod():
    metrics = {"res": {"median_ratio": 1.00, "cod": 20.0, "count": 500}}
    assert harness._passes_iaao(metrics, jurisdiction_tier="large_to_mid") is False

def test_fails_iaao_high_ratio():
    metrics = {"res": {"median_ratio": 1.08, "cod": 10.0, "count": 500}}
    assert harness._passes_iaao(metrics, jurisdiction_tier="large_to_mid") is False

def test_skips_groups_with_low_count():
    metrics = {"res": {"median_ratio": 2.00, "cod": 99.0, "count": 5}}
    assert harness._passes_iaao(metrics, jurisdiction_tier="large_to_mid") is True


# ---------------------------------------------------------------------------
# run_stages (mocked subprocess)
# ---------------------------------------------------------------------------

def test_run_stages_calls_correct_scripts(monkeypatch, tmp_path):
    calls = []

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        calls.append(str(script))
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)
    monkeypatch.setattr(harness, "_SEEDS_DIR", tmp_path / "seeds")
    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "seed_us-pa-berks.json").write_text("{}")

    monkeypatch.setattr(harness, "run_configure", lambda *a, **kw: None)

    harness.run_stages("us-pa-berks", "assemble", "clean", 3, verbose=False)

    assert any("_run_assemble" in c for c in calls)
    assert any("_run_clean" in c for c in calls)
    assert not any("_run_model" in c for c in calls)

def test_run_stages_exits_on_subprocess_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 1)
    monkeypatch.setattr(harness, "_SEEDS_DIR", tmp_path / "seeds")
    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "seed_us-pa-berks.json").write_text("{}")

    with pytest.raises(SystemExit):
        harness.run_stages("us-pa-berks", "assemble", "assemble", 1, verbose=False)


def test_model_recovery_continues_after_subprocess_failure(monkeypatch, tmp_path):
    """When a model subprocess fails, the harness should continue to the next
    iteration instead of raising SystemExit."""
    call_count = {"n": 0}

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        if "_run_model" in str(script):
            call_count["n"] += 1
            # First model run succeeds, second fails, third succeeds
            return 1 if call_count["n"] == 2 else 0
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)

    # Set up locality data dir with required structure
    data_dir = tmp_path / "us-pa-berks"
    data_dir.mkdir()
    settings_path = data_dir / "in" / "settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(json.dumps({"modeling": {"model_groups": {"res": {}}}}))

    monkeypatch.setattr(harness, "_NOTEBOOKS_PIPELINE", tmp_path)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)
    monkeypatch.setattr(harness, "_settings_path", lambda loc: settings_path)

    # Stub out build_data_profile to avoid needing real parquets
    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"jurisdiction_tier": "large_to_mid"},
    )

    # Force IAAO to always report outside range so we never exit early
    monkeypatch.setattr(harness, "_passes_iaao", lambda *a, **kw: False)

    # Stub out Claude refinement to avoid API calls
    monkeypatch.setattr(
        "claude_settings.refine_after_model",
        lambda *a, **kw: {},
    )

    # Create a fake pred_test.parquet for the successful runs
    group_dir = data_dir / "out" / "models" / "res" / "main" / "ensemble"
    group_dir.mkdir(parents=True)
    df = pd.DataFrame({"prediction_ratio": [0.95, 1.05, 1.00, 1.10, 0.90]})
    df.to_parquet(group_dir / "pred_test.parquet", index=False)

    # Should NOT raise SystemExit — recovery should catch the failure
    harness.run_model("us-pa-berks", iteration_count=3, verbose=False)

    # All 3 iterations should have been attempted
    assert call_count["n"] == 3


def test_model_recovery_restores_settings_on_disk(monkeypatch, tmp_path):
    """After a model subprocess failure, settings.json on disk should be
    restored to the pre-iteration state."""
    original_settings = {"modeling": {"model_groups": {"res": {"name": "Residential"}}}}

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        if "_run_model" in str(script):
            # Simulate Claude having mutated settings.json before crash
            settings_path.write_text(json.dumps({"modeling": {"CORRUPTED": True}}))
            return 1  # crash
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)

    data_dir = tmp_path / "us-pa-berks"
    data_dir.mkdir()
    settings_path = data_dir / "in" / "settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(json.dumps(original_settings))

    monkeypatch.setattr(harness, "_NOTEBOOKS_PIPELINE", tmp_path)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)
    monkeypatch.setattr(harness, "_settings_path", lambda loc: settings_path)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"jurisdiction_tier": "large_to_mid"},
    )

    # Run with 1 iteration — it will fail and recover
    harness.run_model("us-pa-berks", iteration_count=1, verbose=False)

    # Settings on disk should be restored to original
    restored = json.loads(settings_path.read_text())
    assert restored == original_settings
    assert "CORRUPTED" not in str(restored)


def test_model_recovery_all_iterations_fail(monkeypatch, tmp_path, capsys):
    """When every model iteration fails, the harness should not crash and
    should report that no iterations completed."""

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        if "_run_model" in str(script):
            return 1  # every run fails
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

    # Should NOT raise SystemExit
    harness.run_model("us-pa-berks", iteration_count=3, verbose=False)

    captured = capsys.readouterr()
    assert "No model iterations completed" in captured.err


# ---------------------------------------------------------------------------
# FieldMappingError
# ---------------------------------------------------------------------------

def test_field_mapping_error_exists():
    """FieldMappingError is importable from harness."""
    assert hasattr(harness, "FieldMappingError")
    assert issubclass(harness.FieldMappingError, Exception)


# ---------------------------------------------------------------------------
# Field mapping integration in run_configure
# ---------------------------------------------------------------------------

def test_configure_skips_claude_when_mappings_valid(monkeypatch, tmp_path):
    """When fuzzy-matched mappings pass validation, Claude is NOT called for field mapping."""
    locality = "test-valid"

    valid_settings = {
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
                        "valid_sale": ["?", [">", "sale_price", 1000]],
                        "vacant_sale": ["?", ["isin", "class", ["A"]]],
                    },
                },
            }
        },
        "modeling": {"model_groups": {}},
    }

    profile = {
        "locality": locality,
        "total_parcels": 100,
        "total_sales": 50,
        "annual_sales_volume": 50,
        "class_distribution": {},
        "he_id_fill_rate_by_class": {},
        "land_he_id_fill_rate_by_class": {},
        "has_spatial_data": False,
        "column_profiles": {
            "cama_master": {
                "parid": {"dtype": "string", "non_null": 100, "unique": 100},
                "price": {"dtype": "float", "non_null": 50, "unique": 40},
                "saledt": {"dtype": "string", "non_null": 50, "unique": 45},
                "class": {"dtype": "string", "non_null": 100, "unique": 3},
            },
        },
        "jurisdiction_tier": "rural_small",
    }

    refine_field_mapping_called = {"called": False}

    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_load_settings", lambda loc: valid_settings)
    monkeypatch.setattr(harness, "_save_settings", lambda loc, s: None)
    monkeypatch.setattr(harness, "_seed_path", lambda loc: tmp_path / "seed.json")

    data_dir = tmp_path / "data" / locality
    (data_dir / "out").mkdir(parents=True)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: profile,
    )
    monkeypatch.setattr(
        "validate_field_mapping.validate_field_mapping",
        lambda settings, dp: {"errors": [], "warnings": []},
    )

    def fake_refine_field_mapping(*a, **kw):
        refine_field_mapping_called["called"] = True
        return None

    monkeypatch.setattr(
        "claude_settings.refine_field_mapping",
        fake_refine_field_mapping,
    )
    monkeypatch.setattr(
        "claude_settings.generate_initial",
        lambda *a, **kw: {"modeling": {"model_groups": {"res": {"name": "Res"}}}},
    )

    harness.run_configure(locality, verbose=False)

    # refine_field_mapping was NOT called (no errors)
    assert not refine_field_mapping_called["called"]


def test_configure_calls_claude_when_mappings_invalid(monkeypatch, tmp_path):
    """When fuzzy-matched mappings have errors, Claude is called to fix them."""
    locality = "test-invalid"

    profile = {
        "locality": locality,
        "total_parcels": 100,
        "total_sales": 50,
        "annual_sales_volume": 50,
        "class_distribution": {},
        "he_id_fill_rate_by_class": {},
        "land_he_id_fill_rate_by_class": {},
        "has_spatial_data": False,
        "column_profiles": {
            "cama_master": {
                "parid": {"dtype": "string", "non_null": 100, "unique": 100},
                "price": {"dtype": "float", "non_null": 50, "unique": 40},
            },
        },
        "jurisdiction_tier": "rural_small",
    }

    incomplete_settings = {
        "data": {
            "load": {
                "cama_master": {
                    "filename": "cama_master.parquet",
                    "load": {"key": "parid"},
                },
            }
        },
        "modeling": {"model_groups": {}},
    }

    fix_delta = {
        "data": {
            "load": {
                "cama_master": {
                    "load": {"sale_price": "price"},
                },
            }
        }
    }

    refine_field_mapping_called = {"called": False}

    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_load_settings", lambda loc: incomplete_settings)
    monkeypatch.setattr(harness, "_save_settings", lambda loc, s: None)
    monkeypatch.setattr(harness, "_seed_path", lambda loc: tmp_path / "seed.json")

    data_dir = tmp_path / "data" / locality
    (data_dir / "out").mkdir(parents=True)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: profile,
    )
    monkeypatch.setattr(
        "validate_field_mapping.validate_field_mapping",
        lambda settings, dp: {"errors": ["Missing critical field 'sale_price'"], "warnings": []},
    )

    def fake_refine_field_mapping(*a, **kw):
        refine_field_mapping_called["called"] = True
        return fix_delta

    monkeypatch.setattr(
        "claude_settings.refine_field_mapping",
        fake_refine_field_mapping,
    )
    monkeypatch.setattr(
        "claude_settings.generate_initial",
        lambda *a, **kw: {"modeling": {"model_groups": {}}},
    )

    harness.run_configure(locality, verbose=False)

    # refine_field_mapping WAS called
    assert refine_field_mapping_called["called"]


def test_configure_raises_field_mapping_error_when_unfixable(monkeypatch, tmp_path):
    """When Claude can't fix the mappings, FieldMappingError is raised."""
    locality = "test-unfixable"

    profile = {
        "locality": locality,
        "total_parcels": 100,
        "total_sales": 50,
        "annual_sales_volume": 50,
        "class_distribution": {},
        "he_id_fill_rate_by_class": {},
        "land_he_id_fill_rate_by_class": {},
        "has_spatial_data": False,
        "column_profiles": {},
        "jurisdiction_tier": "rural_small",
    }

    bad_settings = {
        "data": {"load": {}},
        "modeling": {"model_groups": {}},
    }

    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_load_settings", lambda loc: bad_settings)
    monkeypatch.setattr(harness, "_save_settings", lambda loc, s: None)
    monkeypatch.setattr(harness, "_seed_path", lambda loc: tmp_path / "seed.json")

    data_dir = tmp_path / "data" / locality
    (data_dir / "out").mkdir(parents=True)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: profile,
    )
    monkeypatch.setattr(
        "validate_field_mapping.validate_field_mapping",
        lambda settings, dp: {"errors": ["Missing critical field 'key'"], "warnings": []},
    )
    monkeypatch.setattr(
        "claude_settings.refine_field_mapping",
        lambda *a, **kw: None,
    )

    with pytest.raises(harness.FieldMappingError):
        harness.run_configure(locality, verbose=False)


# ---------------------------------------------------------------------------
# Sales qualification validation in run_assemble
# ---------------------------------------------------------------------------

def test_sales_qualification_error_exists():
    """SalesQualificationError is importable from harness."""
    assert hasattr(harness, "SalesQualificationError")
    assert issubclass(harness.SalesQualificationError, Exception)


def test_assemble_raises_sales_qualification_error(monkeypatch, tmp_path):
    """run_assemble raises SalesQualificationError when validation returns errors."""
    import pickle
    locality = "test-sq-fail"

    # Create a fake assembled pickle with zero valid sales
    data_dir = tmp_path / "notebooks" / "pipeline" / "data" / locality
    out_dir = data_dir / "out"
    out_dir.mkdir(parents=True)

    sales_df = pd.DataFrame({"sale_price": [100_000.0] * 10,
                              "valid_sale": [False] * 10,
                              "vacant_sale": [True] * 5 + [False] * 5})
    univ_df = sales_df.copy()
    with open(out_dir / "1-assemble-sup.pickle", "wb") as f:
        pickle.dump((sales_df, univ_df), f)

    # Mock subprocess to succeed
    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    with pytest.raises(harness.SalesQualificationError):
        harness.run_assemble(locality, verbose=False)


def test_assemble_warns_but_continues(monkeypatch, tmp_path, capsys):
    """run_assemble logs warnings but does not raise when no errors."""
    import pickle
    locality = "test-sq-warn"

    data_dir = tmp_path / "notebooks" / "pipeline" / "data" / locality
    out_dir = data_dir / "out"
    out_dir.mkdir(parents=True)

    # 98% valid → warning (too loose), but not an error
    sales_df = pd.DataFrame({"sale_price": [100_000.0] * 100,
                              "valid_sale": [True] * 98 + [False] * 2,
                              "vacant_sale": [True] * 20 + [False] * 80})
    univ_df = sales_df.copy()
    with open(out_dir / "1-assemble-sup.pickle", "wb") as f:
        pickle.dump((sales_df, univ_df), f)

    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    # Should NOT raise
    harness.run_assemble(locality, verbose=False)

    captured = capsys.readouterr()
    assert "WARNING" in captured.out or "warning" in captured.out.lower()


def test_assemble_passes_clean_data(monkeypatch, tmp_path, capsys):
    """run_assemble completes without warnings when data is healthy."""
    import pickle
    locality = "test-sq-clean"

    data_dir = tmp_path / "notebooks" / "pipeline" / "data" / locality
    out_dir = data_dir / "out"
    out_dir.mkdir(parents=True)

    # 50% valid, 20% vacant — healthy
    sales_df = pd.DataFrame({"sale_price": [100_000.0] * 100,
                              "valid_sale": [True] * 50 + [False] * 50,
                              "vacant_sale": [True] * 20 + [False] * 80})
    univ_df = sales_df.copy()
    with open(out_dir / "1-assemble-sup.pickle", "wb") as f:
        pickle.dump((sales_df, univ_df), f)

    monkeypatch.setattr(harness, "_run_subprocess", lambda *a, **kw: 0)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    # Should NOT raise
    harness.run_assemble(locality, verbose=False)

    captured = capsys.readouterr()
    assert "Sales qualification" in captured.out or "valid" in captured.out.lower()


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


# ---------------------------------------------------------------------------
# configure_settings.py integration in run_configure
# ---------------------------------------------------------------------------

def test_configure_calls_configure_settings_script(monkeypatch, tmp_path):
    """run_configure calls configure_settings.py after generate_settings.py."""
    locality = "test-configure"
    subprocess_calls = []

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        subprocess_calls.append(str(script))
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)
    monkeypatch.setattr(harness, "_load_settings", lambda loc: {
        "data": {"load": {"cama_master": {"load": {"key": "parid", "sale_price": "price", "sale_date": "saledt", "class": "class"}, "calc": {"valid_sale": ["?", True], "vacant_sale": ["?", False]}}}},
        "modeling": {"model_groups": {}},
    })
    monkeypatch.setattr(harness, "_save_settings", lambda loc, s: None)
    monkeypatch.setattr(harness, "_seed_path", lambda loc: tmp_path / "seed.json")

    data_dir = tmp_path / "data" / locality
    (data_dir / "out").mkdir(parents=True)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"column_profiles": {}, "jurisdiction_tier": "rural_small"},
    )
    monkeypatch.setattr(
        "validate_field_mapping.validate_field_mapping",
        lambda settings, dp: {"errors": [], "warnings": []},
    )
    monkeypatch.setattr(
        "claude_settings.generate_initial",
        lambda *a, **kw: {"modeling": {"model_groups": {}}},
    )

    harness.run_configure(locality, verbose=False)

    # Both generate_settings.py AND configure_settings.py should have been called
    script_names = [Path(s).name for s in subprocess_calls]
    assert "generate_settings.py" in script_names
    assert "configure_settings.py" in script_names


def test_configure_settings_runs_before_field_validation(monkeypatch, tmp_path):
    """configure_settings.py runs BEFORE field mapping validation."""
    locality = "test-order"
    call_order = []

    def fake_run_subprocess(script, locality, verbose, extra_args=None):
        call_order.append(("subprocess", Path(script).name))
        return 0

    monkeypatch.setattr(harness, "_run_subprocess", fake_run_subprocess)
    monkeypatch.setattr(harness, "_load_settings", lambda loc: {
        "data": {"load": {"cama_master": {"load": {"key": "parid", "sale_price": "price", "sale_date": "saledt", "class": "class"}, "calc": {"valid_sale": ["?", True], "vacant_sale": ["?", False]}}}},
        "modeling": {"model_groups": {}},
    })
    monkeypatch.setattr(harness, "_save_settings", lambda loc, s: None)
    monkeypatch.setattr(harness, "_seed_path", lambda loc: tmp_path / "seed.json")

    data_dir = tmp_path / "data" / locality
    (data_dir / "out").mkdir(parents=True)
    monkeypatch.setattr(harness, "_locality_data_dir", lambda loc: data_dir)

    monkeypatch.setattr(
        "profile_data.build_data_profile",
        lambda *a, **kw: {"column_profiles": {}, "jurisdiction_tier": "rural_small"},
    )

    def fake_validate(settings, dp):
        call_order.append(("validate", "validate_field_mapping"))
        return {"errors": [], "warnings": []}

    monkeypatch.setattr(
        "validate_field_mapping.validate_field_mapping",
        fake_validate,
    )
    monkeypatch.setattr(
        "claude_settings.generate_initial",
        lambda *a, **kw: {"modeling": {"model_groups": {}}},
    )

    harness.run_configure(locality, verbose=False)

    # Find positions
    configure_pos = next(i for i, (t, n) in enumerate(call_order) if n == "configure_settings.py")
    validate_pos = next(i for i, (t, n) in enumerate(call_order) if n == "validate_field_mapping")
    assert configure_pos < validate_pos, "configure_settings.py must run before field validation"
