"""Tests for scripts/claude_settings.py — Anthropic client is mocked."""
import sys
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from claude_settings import (
    ClaudeParseError,
    generate_initial,
    refine_after_model,
    _extract_json_block,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_PROFILE = {
    "locality": "us-pa-berks",
    "total_parcels": 169_000,
    "total_sales": 28_000,
    "annual_sales_volume": 9_300,
    "class_distribution": {
        "R": {"parcels": 140_000, "sales": 26_000},
        "C": {"parcels": 8_000, "sales": 1_500},
    },
    "he_id_fill_rate_by_class": {"R": 0.98, "C": 0.0},
    "land_he_id_fill_rate_by_class": {"R": 0.97, "C": 0.0},
    "available_columns": ["class", "sale_price", "bldg_area_finished_sqft"],
    "jurisdiction_tier": "large_to_mid",
}

SAMPLE_BASE_SETTINGS = {
    "modeling": {
        "model_groups": {},
        "instructions": {},
    }
}

VALID_DELTA = {
    "modeling": {
        "model_groups": {
            "res": {"name": "Residential", "filter": ["==", "class", "R"]},
            "com": {"name": "Commercial",  "filter": ["==", "class", "C"]},
        },
        "instructions": {
            "skip": {"com": ["all"]},
        },
    }
}

SAMPLE_METRICS = {
    "res": {"median_ratio": 1.03, "cod": 25.7, "count": 9045},
}

SAMPLE_CURRENT_SETTINGS = {
    "modeling": {
        "model_groups": {
            "res": {"name": "Residential", "filter": ["==", "class", "R"]},
        },
        "models": {
            "default": {"dep_vars": ["bldg_area_finished_sqft"]},
        },
    }
}


def _make_mock_response(content: str):
    """Build a mock Anthropic response object."""
    msg = MagicMock()
    msg.content = [MagicMock(text=content)]
    return msg


# ---------------------------------------------------------------------------
# _extract_json_block
# ---------------------------------------------------------------------------

def test_extract_json_block_fenced():
    text = 'Here is the config:\n```json\n{"a": 1}\n```\nDone.'
    assert _extract_json_block(text) == {"a": 1}

def test_extract_json_block_bare():
    text = 'Config: {"a": 1}'
    assert _extract_json_block(text) == {"a": 1}

def test_extract_json_block_invalid_raises():
    with pytest.raises(ClaudeParseError):
        _extract_json_block("No JSON here at all.")


# ---------------------------------------------------------------------------
# generate_initial
# ---------------------------------------------------------------------------

def test_generate_initial_returns_dict(tmp_path):
    response_text = json.dumps({"settings": VALID_DELTA, "reasoning": "R class maps to res."})
    with patch("claude_settings.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = _make_mock_response(response_text)
        result = generate_initial(SAMPLE_PROFILE, SAMPLE_BASE_SETTINGS, reasoning_log=tmp_path / "log.jsonl")
    assert isinstance(result, dict)
    assert "modeling" in result

def test_generate_initial_retries_on_bad_json(tmp_path):
    bad = "not json at all"
    good = json.dumps({"settings": VALID_DELTA, "reasoning": "ok"})
    with patch("claude_settings.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.side_effect = [
            _make_mock_response(bad),
            _make_mock_response(good),
        ]
        result = generate_initial(SAMPLE_PROFILE, SAMPLE_BASE_SETTINGS, reasoning_log=tmp_path / "log.jsonl")
    assert "modeling" in result

def test_generate_initial_raises_after_two_bad_responses(tmp_path):
    bad = "not json"
    with patch("claude_settings.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = _make_mock_response(bad)
        with pytest.raises(ClaudeParseError):
            generate_initial(SAMPLE_PROFILE, SAMPLE_BASE_SETTINGS, reasoning_log=tmp_path / "log.jsonl")

def test_generate_initial_writes_reasoning_log(tmp_path):
    log_path = tmp_path / "reasoning.jsonl"
    response_text = json.dumps({"settings": VALID_DELTA, "reasoning": "R maps to res."})
    with patch("claude_settings.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = _make_mock_response(response_text)
        generate_initial(SAMPLE_PROFILE, SAMPLE_BASE_SETTINGS, reasoning_log=log_path)
    assert log_path.exists()
    content = log_path.read_text()
    assert "R maps to res." in content


# ---------------------------------------------------------------------------
# refine_after_model
# ---------------------------------------------------------------------------

def test_refine_after_model_returns_dict(tmp_path):
    delta = {"modeling": {"models": {"default": {"dep_vars": ["bldg_area_finished_sqft", "bldg_age_years"]}}}}
    response_text = json.dumps({"settings": delta, "reasoning": "COD too high, adding age."})
    with patch("claude_settings.anthropic.Anthropic") as MockClient:
        MockClient.return_value.messages.create.return_value = _make_mock_response(response_text)
        result = refine_after_model(
            SAMPLE_PROFILE, SAMPLE_CURRENT_SETTINGS, SAMPLE_METRICS,
            iteration=1, reasoning_log=tmp_path / "log.jsonl"
        )
    assert isinstance(result, dict)
