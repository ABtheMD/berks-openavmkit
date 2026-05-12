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

def test_read_metrics_no_models_returns_empty(tmp_path):
    (tmp_path / "out" / "models").mkdir(parents=True)
    metrics = harness._read_model_metrics(tmp_path)
    assert metrics == {}


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
