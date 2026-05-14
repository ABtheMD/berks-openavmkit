"""
claude_settings.py

All Claude API interactions for the harness. Has no knowledge of file paths
or pipeline stage sequencing — it takes dicts and returns dicts.

Two public functions:
  generate_initial()     — called during the configure stage
  refine_after_model()   — called between model iteration runs

Both write reasoning to a JSONL log file and raise ClaudeParseError if
Claude's response cannot be parsed after one retry.
"""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import anthropic

# ---------------------------------------------------------------------------
# IAAO Standard on Ratio Studies — COD ranges by property class and tier
# Embedded in the system prompt so Claude can apply the right threshold.
# ---------------------------------------------------------------------------

_IAAO_COD_TABLE = """
IAAO Standard COD Ranges (from Standard on Ratio Studies):

Property Class                          | very_large | large_to_mid | rural_small
Residential improved (SFR, condo, MFR) | 5.0–10.0   | 5.0–15.0     | 5.0–20.0
Income-producing (commercial, ind)      | 5.0–15.0   | 5.0–20.0     | 5.0–25.0
Residential vacant land                 | 5.0–15.0   | 5.0–20.0     | 5.0–25.0
Other (non-agricultural) vacant land    | 5.0–20.0   | 5.0–25.0     | 5.0–30.0

Additional IAAO thresholds (all tiers):
  Median ratio  : 0.95–1.05 target
  PRD           : 0.98–1.03
  PRB           : −0.05 to +0.05
"""

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


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ClaudeParseError(Exception):
    """Raised when Claude's response cannot be parsed as valid settings JSON
    after one retry."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_json_block(text: str) -> dict:
    """
    Extract and parse the first JSON object from Claude's response text.

    Handles both fenced (```json ... ```) and bare JSON responses.
    Raises ClaudeParseError if no valid JSON object is found.
    """
    # Try fenced block first
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            pass

    # Try first { ... } span
    start = text.find("{")
    if start != -1:
        # Walk forward to find the matching closing brace
        depth = 0
        for i, ch in enumerate(text[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        break

    raise ClaudeParseError(f"No valid JSON object found in Claude response:\n{text[:500]}")


def _write_reasoning(reasoning: str, call_type: str, reasoning_log: Path):
    """Append a reasoning entry to the JSONL log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "call_type": call_type,
        "reasoning": reasoning,
    }
    with open(reasoning_log, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def _call_with_retry(client, messages: list, system: str) -> dict:
    """
    Call Claude, parse the response. On parse failure, retry once with the
    error appended. Raises ClaudeParseError if both attempts fail.
    """
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        system=system,
        messages=messages,
    )
    text = response.content[0].text

    try:
        return _extract_json_block(text)
    except ClaudeParseError as first_err:
        # Retry: append the error so Claude knows what went wrong
        retry_messages = messages + [
            {"role": "assistant", "content": text},
            {
                "role": "user",
                "content": (
                    f"Your response could not be parsed as JSON. Error: {first_err}\n"
                    "Please respond with ONLY a valid JSON object with keys "
                    '"settings" and "reasoning".'
                ),
            },
        ]
        retry_response = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            system=system,
            messages=retry_messages,
        )
        retry_text = retry_response.content[0].text
        try:
            return _extract_json_block(retry_text)
        except ClaudeParseError:
            raise ClaudeParseError(
                f"Claude failed to return valid JSON after two attempts.\n"
                f"First response: {text[:300]}\n"
                f"Second response: {retry_text[:300]}"
            )


# ---------------------------------------------------------------------------
# Settings validation
# ---------------------------------------------------------------------------

_VALID_FILTER_OPS = {"==", "!=", ">", "<", ">=", "<=", "isin", "and", "or"}
_VALID_SKIP_VALUES = {"all", "model", "report"}


def validate_settings_delta(
    delta: dict,
    data_profile: dict,
    current_settings: dict = None,
) -> dict:
    """
    Validate a settings delta against the data profile.

    Runs structural rules first (type normalization), then semantic rules
    (column validation), then warning rules.

    Returns a dict with:
      - "cleaned": deep copy of delta with invalid parts stripped
      - "violations": list of human-readable violation descriptions

    If violations is empty, the delta is valid.
    """
    violations = []
    cleaned = json.loads(json.dumps(delta))  # deep copy

    modeling = cleaned.get("modeling", {})
    model_groups = modeling.get("model_groups", {})
    models = modeling.get("models", {})

    # ==================================================================
    # STRUCTURAL RULES (run first to normalize types)
    # ==================================================================

    # ── Rule 3: model_groups structure ───────────────────────────────
    for group_key in list(model_groups.keys()):
        group_cfg = model_groups[group_key]
        if not isinstance(group_cfg, dict):
            violations.append(
                f"Removed group '{group_key}': value is "
                f"{type(group_cfg).__name__}, not a dict"
            )
            del model_groups[group_key]
            continue
        if "filter" in group_cfg:
            filt = group_cfg["filter"]
            if not isinstance(filt, list):
                violations.append(
                    f"Removed group '{group_key}': filter is "
                    f"{type(filt).__name__}, not a list"
                )
                del model_groups[group_key]
                continue
            if len(filt) >= 1 and filt[0] not in _VALID_FILTER_OPS:
                violations.append(
                    f"Removed group '{group_key}': filter operator "
                    f"'{filt[0]}' is not valid"
                )
                del model_groups[group_key]
                continue

    # ── Rule 4: skip values ──────────────────────────────────────────
    for group_key, group_cfg in model_groups.items():
        if not isinstance(group_cfg, dict) or "skip" not in group_cfg:
            continue
        skip = group_cfg["skip"]
        if not isinstance(skip, list):
            violations.append(
                f"Removed skip from group '{group_key}': value is "
                f"{type(skip).__name__}, not a list"
            )
            del group_cfg["skip"]
            continue
        invalid = [s for s in skip if s not in _VALID_SKIP_VALUES]
        if invalid:
            violations.append(
                f"Removed invalid skip values {invalid} from "
                f"group '{group_key}'"
            )
            group_cfg["skip"] = [s for s in skip if s in _VALID_SKIP_VALUES]

    # ── Rule 5: exclude_features type ────────────────────────────────
    for group_key, group_cfg in model_groups.items():
        if not isinstance(group_cfg, dict) or "exclude_features" not in group_cfg:
            continue
        ef = group_cfg["exclude_features"]
        if not isinstance(ef, list):
            violations.append(
                f"Removed exclude_features from group '{group_key}': "
                f"value is {type(ef).__name__}, not a list"
            )
            del group_cfg["exclude_features"]
            continue
        non_strings = [x for x in ef if not isinstance(x, str)]
        if non_strings:
            violations.append(
                f"Removed non-string elements {non_strings} from "
                f"exclude_features in group '{group_key}'"
            )
            group_cfg["exclude_features"] = [x for x in ef if isinstance(x, str)]

    # ── Rule 6: dep_vars type ────────────────────────────────────────
    for model_key, model_cfg in models.items():
        if not isinstance(model_cfg, dict) or "dep_vars" not in model_cfg:
            continue
        dv = model_cfg["dep_vars"]
        if not isinstance(dv, list):
            violations.append(
                f"Removed dep_vars from models.{model_key}: value is "
                f"{type(dv).__name__}, not a list"
            )
            del model_cfg["dep_vars"]
            continue
        non_strings = [x for x in dv if not isinstance(x, str)]
        if non_strings:
            violations.append(
                f"Removed non-string elements {non_strings} from "
                f"dep_vars in models.{model_key}"
            )
            model_cfg["dep_vars"] = [x for x in dv if isinstance(x, str)]

    # ==================================================================
    # SEMANTIC RULES (run after structural rules normalize types)
    # ==================================================================

    # Build known-column and string-column sets from data profile
    known_columns = set()
    string_columns = set()
    for _source, cols in data_profile.get("column_profiles", {}).items():
        for col_name, col_info in cols.items():
            known_columns.add(col_name)
            if col_info.get("dtype") == "string":
                string_columns.add(col_name)

    # Build categorical set from current_settings + delta
    categorical = set()
    if current_settings:
        fc = current_settings.get("field_classification", {})
        for col, ctype in fc.get("important", {}).items():
            if ctype == "categorical":
                categorical.add(col)
    fc_delta = cleaned.get("field_classification", {})
    for col, ctype in fc_delta.get("important", {}).items():
        if ctype == "categorical":
            categorical.add(col)

    # Strings that are dangerous as features (string but not categorical)
    dangerous_strings = string_columns - categorical

    # ── Rule 1: String features ──────────────────────────────────────
    # 1a) dep_vars — remove string columns
    for model_key, model_cfg in models.items():
        if not isinstance(model_cfg, dict) or "dep_vars" not in model_cfg:
            continue
        if not isinstance(model_cfg["dep_vars"], list):
            continue
        cleaned_deps = []
        for dv in model_cfg["dep_vars"]:
            if dv in dangerous_strings:
                violations.append(
                    f"Removed string column '{dv}' from dep_vars in "
                    f"models.{model_key} (would crash LightGBM)"
                )
            else:
                cleaned_deps.append(dv)
        model_cfg["dep_vars"] = cleaned_deps

    # 1b) model_groups — add dangerous strings to exclude_features
    for group_key, group_cfg in list(model_groups.items()):
        if not isinstance(group_cfg, dict):
            continue
        existing_exclude = set(group_cfg.get("exclude_features", []))
        to_add = sorted(dangerous_strings - existing_exclude)
        if to_add:
            group_cfg["exclude_features"] = sorted(existing_exclude | set(to_add))
            violations.append(
                f"Added string columns {to_add} to exclude_features for "
                f"group '{group_key}' (would crash LightGBM)"
            )

    # ── Rule 2: Nonexistent columns ──────────────────────────────────
    # Only enforce when column_profiles is present; skip if known_columns is
    # empty (old-format profiles without column_profiles have no basis for
    # determining whether a column exists).
    if known_columns:
        # 2a) exclude_features — remove unknown columns
        for group_key, group_cfg in list(model_groups.items()):
            if not isinstance(group_cfg, dict):
                continue
            if "exclude_features" in group_cfg and isinstance(
                group_cfg["exclude_features"], list
            ):
                valid = [c for c in group_cfg["exclude_features"] if c in known_columns]
                removed = [
                    c for c in group_cfg["exclude_features"] if c not in known_columns
                ]
                if removed:
                    violations.append(
                        f"Removed nonexistent columns {removed} from "
                        f"exclude_features in group '{group_key}'"
                    )
                    group_cfg["exclude_features"] = valid

        # 2b) dep_vars — remove unknown columns
        for model_key, model_cfg in models.items():
            if not isinstance(model_cfg, dict) or "dep_vars" not in model_cfg:
                continue
            if not isinstance(model_cfg["dep_vars"], list):
                continue
            valid = [c for c in model_cfg["dep_vars"] if c in known_columns]
            removed = [c for c in model_cfg["dep_vars"] if c not in known_columns]
            if removed:
                violations.append(
                    f"Removed nonexistent columns {removed} from "
                    f"dep_vars in models.{model_key}"
                )
                model_cfg["dep_vars"] = valid

        # 2c) filters — remove groups with nonexistent filter fields
        for group_key in list(model_groups.keys()):
            group_cfg = model_groups[group_key]
            if not isinstance(group_cfg, dict) or "filter" not in group_cfg:
                continue
            filt = group_cfg["filter"]
            if isinstance(filt, list) and len(filt) >= 2:
                op = filt[0]
                if op not in ("and", "or") and len(filt) >= 2:
                    field = filt[1]
                    if isinstance(field, str) and field not in known_columns:
                        violations.append(
                            f"Removed group '{group_key}': filter references "
                            f"nonexistent column '{field}'"
                        )
                        del model_groups[group_key]

    # ==================================================================
    # WARNING RULES
    # ==================================================================

    # ── Rule 7: Empty model_groups warning ───────────────────────────
    if model_groups:
        active_groups = [
            k for k, v in model_groups.items()
            if isinstance(v, dict) and "all" not in v.get("skip", [])
        ]
        if not active_groups:
            violations.append(
                "Warning: all model_groups are skipped — no active groups "
                "will be modeled"
            )

    return {"cleaned": cleaned, "violations": violations}


# ---------------------------------------------------------------------------
# Validation + re-prompt helper
# ---------------------------------------------------------------------------

def _validate_and_reprompt(
    client,
    messages: list,
    system: str,
    settings_delta: dict,
    data_profile: dict,
    current_settings: dict,
    reasoning_log: Path,
    call_type: str,
) -> dict:
    """
    Validate a settings delta. If violations found, re-prompt Claude once.

    Returns the cleaned delta from whichever pass is cleanest.
    """
    result = validate_settings_delta(settings_delta, data_profile, current_settings)

    if not result["violations"]:
        return result["cleaned"]

    # Log violations
    if reasoning_log is not None:
        _write_reasoning(
            "Validation violations:\n"
            + "\n".join(f"- {v}" for v in result["violations"]),
            "validation",
            reasoning_log,
        )

    # Build re-prompt with violations and cleaned delta
    violation_list = "\n".join(f"- {v}" for v in result["violations"])
    reprompt_content = (
        f"Your settings delta had validation errors:\n{violation_list}\n\n"
        f"The invalid parts have been removed. The cleaned delta is:\n"
        f"```json\n{json.dumps(result['cleaned'], indent=2)}\n```\n\n"
        f"Please provide a corrected settings delta that addresses "
        f"these issues.\n"
        f'Respond with ONLY a JSON object with keys "settings" and '
        f'"reasoning".'
    )

    reprompt_messages = messages + [
        {
            "role": "assistant",
            "content": json.dumps(
                {"settings": settings_delta, "reasoning": ""}
            ),
        },
        {"role": "user", "content": reprompt_content},
    ]

    try:
        reprompt_parsed = _call_with_retry(client, reprompt_messages, system)
        reprompt_delta = reprompt_parsed.get("settings", reprompt_parsed)
        reprompt_reasoning = reprompt_parsed.get("reasoning", "")

        if reasoning_log is not None:
            _write_reasoning(
                reprompt_reasoning, f"{call_type}_reprompt", reasoning_log
            )

        # Validate the re-prompt response
        reprompt_result = validate_settings_delta(
            reprompt_delta, data_profile, current_settings
        )

        if reprompt_result["violations"] and reasoning_log is not None:
            _write_reasoning(
                "Re-prompt still has violations:\n"
                + "\n".join(f"- {v}" for v in reprompt_result["violations"]),
                "validation",
                reasoning_log,
            )

        return reprompt_result["cleaned"]

    except ClaudeParseError:
        # If re-prompt fails to parse, return cleaned delta from first pass
        return result["cleaned"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_initial(
    data_profile: dict,
    base_settings: dict,
    reasoning_log: Path = None,
) -> dict:
    """
    Configure stage: generate model_groups, skip rules, and HE exclusions.

    Parameters
    ----------
    data_profile : dict
        Output of profile_data.build_data_profile()
    base_settings : dict
        Current settings.json (has field_classification, empty model_groups)
    reasoning_log : Path, optional
        File path to append Claude's reasoning (JSONL format)

    Returns
    -------
    dict : settings delta to merge into settings.json

    Raises
    ------
    ClaudeParseError : if Claude's response cannot be parsed after one retry
    """
    client = anthropic.Anthropic()
    messages = [
        {
            "role": "user",
            "content": (
                f"Data profile:\n```json\n{json.dumps(data_profile, indent=2)}\n```\n\n"
                f"Current settings.json (partial):\n```json\n{json.dumps(base_settings, indent=2)}\n```\n\n"
                "Generate the settings delta."
            ),
        }
    ]
    parsed = _call_with_retry(client, messages, _SYSTEM_CONFIGURE)
    settings_delta = parsed.get("settings", parsed)
    reasoning = parsed.get("reasoning", "")

    log_path = Path(reasoning_log) if reasoning_log is not None else None
    if log_path is not None:
        _write_reasoning(reasoning, "generate_initial", log_path)

    return _validate_and_reprompt(
        client, messages, _SYSTEM_CONFIGURE,
        settings_delta, data_profile, base_settings,
        log_path, "generate_initial",
    )


def refine_after_model(
    data_profile: dict,
    current_settings: dict,
    model_metrics: dict,
    iteration: int,
    reasoning_log: Path = None,
) -> dict:
    """
    Model iteration: propose settings changes to improve metrics.

    Parameters
    ----------
    data_profile : dict
        Output of profile_data.build_data_profile()
    current_settings : dict
        The settings.json that produced the current model run
    model_metrics : dict
        {group: {median_ratio, cod, count, prd, prb}} per active group
    iteration : int
        Which iteration number this is (1-based, for Claude's context)
    reasoning_log : Path, optional
        File path to append Claude's reasoning (JSONL format)

    Returns
    -------
    dict : settings delta to merge into settings.json

    Raises
    ------
    ClaudeParseError : if Claude's response cannot be parsed after one retry
    """
    client = anthropic.Anthropic()
    messages = [
        {
            "role": "user",
            "content": (
                f"Iteration: {iteration}\n\n"
                f"Data profile:\n```json\n{json.dumps(data_profile, indent=2)}\n```\n\n"
                f"Current settings.json:\n```json\n{json.dumps(current_settings, indent=2)}\n```\n\n"
                f"Model metrics:\n```json\n{json.dumps(model_metrics, indent=2)}\n```\n\n"
                "Propose settings changes to improve assessment quality."
            ),
        }
    ]
    parsed = _call_with_retry(client, messages, _SYSTEM_REFINE)
    settings_delta = parsed.get("settings", parsed)
    reasoning = parsed.get("reasoning", "")

    log_path = Path(reasoning_log) if reasoning_log is not None else None
    if log_path is not None:
        _write_reasoning(reasoning, f"refine_iter_{iteration}", log_path)

    return _validate_and_reprompt(
        client, messages, _SYSTEM_REFINE,
        settings_delta, data_profile, current_settings,
        log_path, f"refine_iter_{iteration}",
    )
