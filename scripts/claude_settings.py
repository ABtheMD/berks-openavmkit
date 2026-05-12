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
2. HE fields (he_id, land_he_id): If he_id_fill_rate_by_class for a group
   is <0.05, add those fields to that group's exclude_features list.
   If has_spatial_data is true for the locality, flag spatial_he_inheritance=true
   for those groups instead of excluding.
3. Respond with a JSON object with exactly two keys:
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
3. State which IAAO tier you assigned and which COD range you used.
4. Respond with a JSON object with exactly two keys:
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

    if reasoning_log is not None:
        _write_reasoning(reasoning, "generate_initial", Path(reasoning_log))

    return settings_delta


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

    if reasoning_log is not None:
        _write_reasoning(reasoning, f"refine_iter_{iteration}", Path(reasoning_log))

    return settings_delta
