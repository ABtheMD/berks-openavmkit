"""
harness.py

CLI orchestrator for the openavmkit pipeline. Runs any subset of the five
stages (download → configure → assemble → clean → model) for a given
locality slug, using Claude API calls to generate and iterate settings.json.

Usage:
    python scripts/harness.py <locality-slug> [options]

Options:
    --from STAGE           Start from this stage (default: download)
    --to STAGE             Stop after this stage (default: model)
    --iteration-count N    Max Claude iterations on model stage (default: 3)
    --verbose              Stream subprocess output to terminal
"""
import argparse
import json
import os
import sys
import subprocess
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_NOTEBOOKS_PIPELINE = _REPO_ROOT / "notebooks" / "pipeline"
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
_SEEDS_DIR = _REPO_ROOT / "seeds"

STAGES = {
    "download":  _SCRIPTS_DIR / "download_data.py",
    "configure": None,
    "assemble":  _SCRIPTS_DIR / "_run_assemble.py",
    "clean":     _SCRIPTS_DIR / "_run_clean.py",
    "model":     _SCRIPTS_DIR / "_run_model.py",
}

_STAGE_ORDER = list(STAGES.keys())

# IAAO COD upper bounds by (property_class_bucket, jurisdiction_tier)
_IAAO_COD_UPPER = {
    "residential":      {"very_large": 10.0, "large_to_mid": 15.0, "rural_small": 20.0},
    "income_producing": {"very_large": 15.0, "large_to_mid": 20.0, "rural_small": 25.0},
    "res_vacant":       {"very_large": 15.0, "large_to_mid": 20.0, "rural_small": 25.0},
    "other_vacant":     {"very_large": 20.0, "large_to_mid": 25.0, "rural_small": 30.0},
}

_GROUP_CLASS_BUCKET = {
    "res": "residential",
    "com": "income_producing",
    "ind": "income_producing",
    "ag":  "other_vacant",
    "farm": "other_vacant",
    "util": "income_producing",
    "exempt": "other_vacant",
}

_IAAO_RATIO_LO = 0.95
_IAAO_RATIO_HI = 1.05
_MIN_COUNT_FOR_IAAO = 30


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="harness.py",
        description="Run the openavmkit pipeline for a locality slug.",
    )
    parser.add_argument("locality", help="Locality slug, e.g. us-pa-berks")
    parser.add_argument(
        "--from", dest="from_stage", default="download",
        metavar="STAGE",
        help=f"Start from this stage. Options: {', '.join(_STAGE_ORDER)}",
    )
    parser.add_argument(
        "--to", dest="to_stage", default="model",
        metavar="STAGE",
        help=f"Stop after this stage. Options: {', '.join(_STAGE_ORDER)}",
    )
    parser.add_argument(
        "--iteration-count", type=int, default=3, choices=range(1, 100),
        metavar="N",
        help="Max Claude iterations on model stage (default: 3, min: 1)",
    )
    parser.add_argument("--verbose", action="store_true", help="Stream subprocess output")
    return parser.parse_args(argv)


def _stage_index(stage: str) -> int:
    if stage not in _STAGE_ORDER:
        print(
            f"Error: unknown stage '{stage}'. "
            f"Valid stages: {', '.join(_STAGE_ORDER)}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return _STAGE_ORDER.index(stage)


def _validate_stage_range(from_stage: str, to_stage: str):
    fi = _stage_index(from_stage)
    ti = _stage_index(to_stage)
    if fi > ti:
        print(
            f"Error: --from {from_stage} comes after --to {to_stage} in stage order.",
            file=sys.stderr,
        )
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Seed file helpers
# ---------------------------------------------------------------------------

def _seed_path(locality: str) -> Path:
    return _SEEDS_DIR / f"seed_{locality}.json"


def _check_seed(locality: str):
    path = _seed_path(locality)
    if not path.exists():
        print(
            f"Error: seed file not found for '{locality}'.\n"
            f"Expected: {path}",
            file=sys.stderr,
        )
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------

def _run_subprocess(script: Path, locality: str, verbose: bool, extra_args=None) -> int:
    env = {
        **os.environ,
        "LOCALITY": locality,
        "PYTHONPATH": os.pathsep.join([
            str(_NOTEBOOKS_PIPELINE),
            os.environ.get("PYTHONPATH", ""),
        ]),
    }
    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd,
        cwd=str(_NOTEBOOKS_PIPELINE),
        env=env,
        capture_output=not verbose,
        text=True,
    )

    if result.returncode != 0 and not verbose:
        lines = (result.stderr or result.stdout or "").splitlines()
        print(f"\n[harness] Stage failed. Last output:\n", file=sys.stderr)
        print("\n".join(lines[-50:]), file=sys.stderr)

    return result.returncode


# ---------------------------------------------------------------------------
# Metrics reading
# ---------------------------------------------------------------------------

def _read_model_metrics(locality_data_dir: Path) -> dict:
    models_dir = locality_data_dir / "out" / "models"
    if not models_dir.exists():
        return {}

    metrics = {}
    for group_dir in sorted(models_dir.iterdir()):
        if not group_dir.is_dir():
            continue
        pred_path = group_dir / "main" / "ensemble" / "pred_test.parquet"
        if not pred_path.exists():
            continue
        df = pd.read_parquet(pred_path, columns=["prediction_ratio"])
        ratios = df["prediction_ratio"].dropna()
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


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def _locality_data_dir(locality: str) -> Path:
    return _NOTEBOOKS_PIPELINE / "data" / locality


def _settings_path(locality: str) -> Path:
    return _locality_data_dir(locality) / "in" / "settings.json"


def _load_settings(locality: str) -> dict:
    p = _settings_path(locality)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def _save_settings(locality: str, settings: dict):
    p = _settings_path(locality)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(settings, fh, indent=2)


def _merge_settings(base: dict, delta: dict) -> dict:
    """
    JSON Merge Patch (RFC 7396): keys in delta override base; keys absent
    from delta are preserved. Null values in delta delete the key.
    Applied recursively for nested dicts.
    """
    result = dict(base)
    for k, v in delta.items():
        if v is None:
            result.pop(k, None)
        elif isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _merge_settings(result[k], v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Model iteration helpers
# ---------------------------------------------------------------------------

def _clear_model_checkpoints(locality_data_dir: Path):
    checkpoints_dir = locality_data_dir / "out" / "checkpoints"
    if not checkpoints_dir.exists():
        return
    for f in checkpoints_dir.glob("3-model-*.pickle"):
        f.unlink()
        print(f"[harness] Cleared checkpoint: {f.name}")


def _save_settings_snapshot(locality_data_dir: Path, iteration: int, settings: dict):
    out_dir = locality_data_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    snap = out_dir / f"settings_iter_{iteration}.json"
    with open(snap, "w", encoding="utf-8") as fh:
        json.dump(settings, fh, indent=2)


def _save_metrics_snapshot(locality_data_dir: Path, iteration: int, metrics: dict):
    out_dir = locality_data_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    snap = out_dir / f"metrics_iter_{iteration}.json"
    with open(snap, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)


def _passes_iaao(metrics: dict, jurisdiction_tier: str) -> bool:
    for group, m in metrics.items():
        if m.get("count", 0) < _MIN_COUNT_FOR_IAAO:
            continue
        ratio = m.get("median_ratio", 1.0)
        cod = m.get("cod", 0.0)

        if not (_IAAO_RATIO_LO <= ratio <= _IAAO_RATIO_HI):
            return False

        bucket = _GROUP_CLASS_BUCKET.get(group, "residential")
        cod_upper = _IAAO_COD_UPPER[bucket].get(jurisdiction_tier, 20.0)
        if cod > cod_upper:
            return False

    return True


def _best_iteration(metrics_history: list, jurisdiction_tier: str = "large_to_mid") -> int:
    best_iter = None
    best_score = float("inf")

    for entry in metrics_history:
        for iteration, metrics in entry.items():
            score = 0.0
            for group, m in metrics.items():
                if m.get("count", 0) < _MIN_COUNT_FOR_IAAO:
                    continue
                score += abs(m.get("median_ratio", 1.0) - 1.0)
                bucket = _GROUP_CLASS_BUCKET.get(group, "residential")
                cod_upper = _IAAO_COD_UPPER[bucket].get(jurisdiction_tier, 20.0)
                score += max(0.0, m.get("cod", 0.0) - cod_upper)
            if score < best_score:
                best_score = score
                best_iter = iteration

    return best_iter


def _print_metrics_summary(metrics: dict, jurisdiction_tier: str):
    print(f"[harness] Metrics summary:")
    for group, m in metrics.items():
        bucket = _GROUP_CLASS_BUCKET.get(group, "residential")
        cod_upper = _IAAO_COD_UPPER[bucket].get(jurisdiction_tier, 20.0)
        ratio = m.get("median_ratio", 0.0)
        cod = m.get("cod", 0.0)
        count = m.get("count", 0)
        ratio_ok = _IAAO_RATIO_LO <= ratio <= _IAAO_RATIO_HI
        cod_ok = cod <= cod_upper
        status = "PASS" if (ratio_ok and cod_ok) else "FAIL"
        print(
            f"  {status} {group:8s}  "
            f"ratio={ratio:.3f} (target 0.95-1.05)  "
            f"COD={cod:.1f} (target <={cod_upper})  "
            f"n={count}"
        )


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def run_download(locality: str, verbose: bool):
    print(f"[harness] === DOWNLOAD ===")
    seed = _seed_path(locality)
    rc = _run_subprocess(
        _SCRIPTS_DIR / "download_data.py", locality, verbose,
        extra_args=[str(seed), "--out-dir", str(_locality_data_dir(locality) / "in")],
    )
    if rc != 0:
        print(f"[harness] download failed (exit {rc})", file=sys.stderr)
        raise SystemExit(rc)


def run_configure(locality: str, verbose: bool):
    print(f"[harness] === CONFIGURE ===")

    seed = _seed_path(locality)
    out_settings = _settings_path(locality)
    rc = _run_subprocess(
        _SCRIPTS_DIR / "generate_settings.py", locality, verbose,
        extra_args=[str(seed), "--output", str(out_settings)],
    )
    if rc != 0:
        print(f"[harness] generate_settings failed (exit {rc})", file=sys.stderr)
        raise SystemExit(rc)

    sys.path.insert(0, str(_SCRIPTS_DIR))
    from profile_data import build_data_profile
    from claude_settings import generate_initial, ClaudeParseError

    data_dir = _locality_data_dir(locality)
    data_profile = build_data_profile(locality, data_base_dir=data_dir.parent)

    base_settings = _load_settings(locality)
    reasoning_log = data_dir / "out" / "claude_reasoning.jsonl"
    reasoning_log.parent.mkdir(parents=True, exist_ok=True)

    try:
        delta = generate_initial(data_profile, base_settings, reasoning_log=reasoning_log)
    except ClaudeParseError as e:
        print(f"[harness] WARNING: Claude settings generation failed: {e}", file=sys.stderr)
        print("[harness] Proceeding with base settings (no model_groups configured).", file=sys.stderr)
        return
    except Exception as e:
        print(f"[harness] WARNING: Claude API call failed: {type(e).__name__}: {e}", file=sys.stderr)
        print("[harness] Proceeding with base settings (no model_groups configured).", file=sys.stderr)
        return

    merged = _merge_settings(base_settings, delta)
    _save_settings(locality, merged)
    print(f"[harness] settings.json updated with Claude's model_groups decisions.")


def run_assemble(locality: str, verbose: bool):
    print(f"[harness] === ASSEMBLE ===")
    rc = _run_subprocess(_SCRIPTS_DIR / "_run_assemble.py", locality, verbose)
    if rc != 0:
        print(f"[harness] assemble failed (exit {rc}). Re-run with --from assemble.", file=sys.stderr)
        raise SystemExit(rc)


def run_clean(locality: str, verbose: bool):
    print(f"[harness] === CLEAN ===")
    rc = _run_subprocess(_SCRIPTS_DIR / "_run_clean.py", locality, verbose)
    if rc != 0:
        print(f"[harness] clean failed (exit {rc}). Re-run with --from clean.", file=sys.stderr)
        raise SystemExit(rc)


def run_model(locality: str, iteration_count: int, verbose: bool):
    print(f"[harness] === MODEL (up to {iteration_count} iterations) ===")
    sys.path.insert(0, str(_SCRIPTS_DIR))
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
            return

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


def run_stages(
    locality: str,
    from_stage: str,
    to_stage: str,
    iteration_count: int,
    verbose: bool,
):
    fi = _stage_index(from_stage)
    ti = _stage_index(to_stage)
    active = _STAGE_ORDER[fi : ti + 1]

    dispatch = {
        "download":  lambda: run_download(locality, verbose),
        "configure": lambda: run_configure(locality, verbose),
        "assemble":  lambda: run_assemble(locality, verbose),
        "clean":     lambda: run_clean(locality, verbose),
        "model":     lambda: run_model(locality, iteration_count, verbose),
    }

    for stage in active:
        dispatch[stage]()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None):
    args = _parse_args(argv)
    _check_seed(args.locality)
    _validate_stage_range(args.from_stage, args.to_stage)
    print(f"[harness] locality={args.locality}  stages={args.from_stage}->{args.to_stage}")
    run_stages(
        args.locality,
        args.from_stage,
        args.to_stage,
        args.iteration_count,
        args.verbose,
    )
    print("[harness] Done.")


if __name__ == "__main__":
    main()
