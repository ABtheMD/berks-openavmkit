# Harness Design Spec
**Date:** 2026-05-11  
**Branch:** `feature/harness` (new, cut from master)  
**Status:** Approved for implementation

---

## 1. Purpose

`harness.py` is a locality-agnostic CLI orchestrator that runs the full openavmkit pipeline — from raw data download through final model evaluation — for any jurisdiction identified by a locality slug. It embeds Claude API calls at two decision points to intelligently generate and iterate on `settings.json`, replacing hand-authored per-jurisdiction workarounds with a repeatable, auditable agent loop.

---

## 2. Architecture

### 2.1 File Layout

All harness-facing scripts live in `scripts/`:

```
scripts/
├── harness.py              # CLI entry point + stage orchestrator (new)
├── claude_settings.py      # All Claude API interactions (new)
├── profile_data.py         # Deterministic data profiling (new)
├── generate_settings.py    # Existing: ArcGIS field mapping (unchanged)
├── download_data.py        # Existing: parquet downloads (unchanged)
├── _run_assemble.py        # Moved from notebooks/pipeline/
├── _run_clean.py           # Moved from notebooks/pipeline/
└── _run_model.py           # Moved from notebooks/pipeline/
```

**Migration note:** `_run_assemble.py`, `_run_clean.py`, and `_run_model.py` are moved from `notebooks/pipeline/` to `scripts/`. The `init_notebooks.py` path-resolution logic must be updated to reflect the new working directory. The harness sets `LOCALITY` via environment variable before spawning subprocesses, so scripts no longer need to call `check_for_different_locality()` directly — but the function is preserved for backward compatibility with direct invocation.

### 2.2 Component Responsibilities

| Component | Owns |
|---|---|
| `harness.py` | Stage sequencing, subprocess invocation, `--from`/`--to` logic, iteration loop, checkpoint clearing, settings merging, audit file writes |
| `claude_settings.py` | All `anthropic` SDK calls, system prompts, response parsing, settings delta validation |
| `profile_data.py` | Reading downloaded parquets, computing measurable facts (class value counts, sales counts, HE fill rates, parcel counts, jurisdiction size inference) |
| `generate_settings.py` | ArcGIS field metadata → field classification block (unchanged) |
| `download_data.py` | ArcGIS Feature Server → parquet files (unchanged) |
| `_run_assemble/clean/model.py` | Pipeline execution (moved, minimal changes) |

### 2.3 Design Principle

`harness.py` owns **what to measure** and **what schema to write**.  
`claude_settings.py` owns **what to decide**.  
These two concerns are never mixed.

---

## 3. CLI Interface

### 3.1 Invocation

```bash
python scripts/harness.py <locality-slug> [options]
```

Locality slug follows the existing convention (e.g. `us-pa-berks`). The harness resolves the seed file automatically: `seeds/seed_<locality-slug>.json`. Missing seed file → immediate exit with a clear error.

### 3.2 Options

| Flag | Default | Description |
|---|---|---|
| `--from STAGE` | `download` | Start from this stage (inclusive) |
| `--to STAGE` | `model` | Stop after this stage (inclusive) |
| `--iteration-count N` | `3` | Max Claude iterations on the model stage |
| `--verbose` | off | Stream subprocess stdout/stderr to terminal |

`--from` must be ≤ `--to` in stage order. Invalid stage names exit immediately with a list of valid options.

### 3.3 Stage Names & Order

| # | Stage | What runs |
|---|---|---|
| 1 | `download` | `download_data.py` |
| 2 | `configure` | `generate_settings.py` → `profile_data.py` → Claude Call #1 |
| 3 | `assemble` | `_run_assemble.py` |
| 4 | `clean` | `_run_clean.py` |
| 5 | `model` | `_run_model.py` × up to `--iteration-count`, with Claude Call #2 between runs |

### 3.4 Example Invocations

```bash
# Full run from scratch
python scripts/harness.py us-pa-berks

# Download and configure only — inspect settings before committing to a long run
python scripts/harness.py us-pa-berks --to configure

# Resume from assemble (data + settings already done)
python scripts/harness.py us-pa-berks --from assemble

# Re-run model only with a single Claude iteration
python scripts/harness.py us-pa-berks --from model --iteration-count 1
```

---

## 4. Stage Sequencing & Data Flow

### 4.1 Configure Stage (Claude Call #1)

**Purpose:** Fill in what `generate_settings.py` cannot — `model_groups` filters, skip rules, HE field exclusions — using actual data rather than ArcGIS metadata.

**Flow:**
1. `generate_settings.py` → `settings.json` with field classification, empty `model_groups`
2. `profile_data.py` → `data_profile` dict (see §5.1)
3. `claude_settings.generate_initial(data_profile, base_settings)` → settings delta
4. Harness merges delta into `settings.json` using JSON Merge Patch semantics (keys present in the delta override the base; keys absent from the delta are preserved unchanged) and writes it back

**Claude's output (example):**
```json
{
  "modeling": {
    "model_groups": {
      "res": { "name": "Residential", "filter": ["==", "class", "R"] },
      "com": { "name": "Commercial",  "filter": ["==", "class", "C"] }
    },
    "instructions": {
      "skip": { "com": ["all"], "ag": ["all"] },
      "exclude_features": { "com": ["he_id", "land_he_id"] }
    }
  }
}
```

Claude also returns a `reasoning` field (logged, not written to `settings.json`).

### 4.2 Model Stage — Iteration Loop

**Per-iteration flow:**
```
for i in 0 .. iteration_count-1:
    1. Clear model-stage checkpoints (files matching the "3-model" checkpoint prefix)
    2. Save current settings.json → out/settings_iter_{i}.json
    3. Run _run_model.py
    4. Read model metrics → out/metrics_iter_{i+1}.json
    5. If all active groups pass IAAO thresholds → stop early
    6. If i < iteration_count-1:
         Claude Call #2 → settings delta
         Harness merges delta → settings.json
```

**On exhaustion without passing:** Harness exits cleanly, reports which metrics remain outside range, and identifies which `settings_iter_N.json` produced the best results. "Best" is defined as the lowest composite deviation score: sum of `|median_ratio − 1.0|` and `max(0, COD − target_COD_upper)` across all active groups.

### 4.3 Audit Trail

Written to `data/<locality>/out/` after each model run:

```
settings_iter_0.json    # settings before first model run
settings_iter_1.json    # settings before second model run (if any)
metrics_iter_1.json     # metrics from run 1, which triggered run 2
metrics_iter_2.json     # metrics from run 2 (final, or triggered run 3)
claude_reasoning.jsonl  # append-only log of all Claude reasoning blocks
```

---

## 5. Claude Integration

### 5.1 Data Profile Schema

Computed by `profile_data.py` from the downloaded parquets:

```json
{
  "locality": "us-pa-berks",
  "total_parcels": 169000,
  "total_sales": 28000,
  "annual_sales_volume": 9300,
  "class_distribution": {
    "R": { "parcels": 140000, "sales": 26000 },
    "C": { "parcels": 8000,   "sales": 1500 },
    "A": { "parcels": 3000,   "sales": 30  }
  },
  "he_id_fill_rate_by_class": {
    "R": 0.98, "C": 0.0, "A": 0.0
  },
  "available_columns": ["class", "bldg_area_finished_sqft", "..."],
  "jurisdiction_tier": "large_to_mid"
}
```

`jurisdiction_tier` is inferred from `total_parcels` and `annual_sales_volume`:
- `very_large`: > 500k parcels or > 50k annual sales
- `large_to_mid`: 50k–500k parcels
- `rural_small`: < 50k parcels

### 5.2 IAAO Thresholds

The IAAO Standard on Ratio Studies COD ranges are embedded in the Claude system prompt. Claude selects the applicable range per group based on property class and `jurisdiction_tier`:

| Property Class | large_to_mid COD Target |
|---|---|
| Residential improved | 5.0–15.0 |
| Income-producing (com/ind) | 5.0–20.0 |
| Residential vacant | 5.0–20.0 |
| Other vacant | 5.0–25.0 |

Additional IAAO metrics Claude evaluates:

| Metric | Target Range |
|---|---|
| Median ratio | 0.95–1.05 |
| PRD | 0.98–1.03 |
| PRB | −0.05 to +0.05 |

Claude explicitly states which tier and COD range it applied in its `reasoning` field.

### 5.3 Claude Module Interface

```python
# claude_settings.py

def generate_initial(data_profile: dict, base_settings: dict) -> dict:
    """
    Configure stage call. Returns settings delta to merge into settings.json.
    Raises ClaudeParseError if response cannot be parsed after one retry.
    """

def refine_after_model(
    data_profile: dict,
    current_settings: dict,
    model_metrics: dict,
    iteration: int,
) -> dict:
    """
    Model iteration call. Returns settings delta.
    Raises ClaudeParseError if response cannot be parsed after one retry.
    """
```

Both functions return a plain dict (the settings delta) and write the `reasoning` block to `claude_reasoning.jsonl` as a side effect. Both raise `ClaudeParseError` (caught by the harness per the error handling policy in §6 — "Claude response unparseable") if Claude's response cannot be parsed after one retry.

---

## 6. Error Handling

| Failure | Policy |
|---|---|
| Missing seed file | Exit immediately with message listing expected path |
| Invalid `--from`/`--to` stage | Exit immediately listing valid stage names |
| Subprocess non-zero exit | Capture exit code + last 50 lines of stderr, print stage name, exit. Re-run with `--from <stage>` to resume after fix. |
| Claude response unparseable | Retry once with error appended to conversation. If retry fails, skip iteration, log warning, continue with current settings. |
| Iterations exhausted, metrics not passing | Exit cleanly. Report metrics delta from IAAO targets. Identify best-performing `settings_iter_N.json`. |

---

## 7. Out of Scope

- Interactive / REPL mode (future layer on top of this CLI)
- Non-ArcGIS data sources (flat files, databases)
- Multi-locality batch runs (e.g. `harness.py --all`)
- Automatic PR creation after successful run
- Changes to `openavmkit/` library code
