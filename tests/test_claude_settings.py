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
    validate_settings_delta,
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


VALIDATION_PROFILE = {
    "locality": "us-pa-test",
    "total_parcels": 5000,
    "total_sales": 1000,
    "annual_sales_volume": 500,
    "class_distribution": {"R": {"parcels": 4000, "sales": 800}},
    "he_id_fill_rate_by_class": {"R": 0.98},
    "land_he_id_fill_rate_by_class": {"R": 0.97},
    "has_spatial_data": True,
    "column_profiles": {
        "cama_master": {
            "key": {"dtype": "string", "non_null": 5000, "unique": 5000},
            "class": {"dtype": "string", "non_null": 5000, "unique": 5},
            "sale_price": {"dtype": "float", "non_null": 3000, "unique": 2000},
            "he_id": {"dtype": "int", "non_null": 4800, "unique": 1200},
            "bldg_area": {"dtype": "float", "non_null": 4500, "unique": 3000},
        },
        "geo_parcels": {
            "key": {"dtype": "string", "non_null": 5000, "unique": 5000},
            "school": {"dtype": "string", "non_null": 5000, "unique": 15},
            "municipalname": {"dtype": "string", "non_null": 5000, "unique": 30},
            "acreage": {"dtype": "float", "non_null": 4900, "unique": 4000},
            "lat": {"dtype": "float", "non_null": 5000, "unique": 4999},
        },
    },
    "jurisdiction_tier": "large_to_mid",
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


# ---------------------------------------------------------------------------
# validate_settings_delta — Rule 3: model_groups structure
# ---------------------------------------------------------------------------

def test_validate_rule3_non_dict_group_removed():
    """model_group entry that is not a dict should be removed."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": "not a dict",
                "com": {"name": "Commercial", "filter": ["==", "class", "C"]},
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    assert "res" not in result["cleaned"]["modeling"]["model_groups"]
    assert "com" in result["cleaned"]["modeling"]["model_groups"]
    assert any("res" in v for v in result["violations"])

def test_validate_rule3_invalid_filter_operator():
    """Filter with invalid operator should cause group removal."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {"name": "Residential", "filter": ["LIKE", "class", "R"]}
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    assert "res" not in result["cleaned"]["modeling"]["model_groups"]
    assert any("LIKE" in v for v in result["violations"])

def test_validate_rule3_filter_not_list():
    """Filter that is not a list should cause group removal."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {"name": "Residential", "filter": "class == R"}
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    assert "res" not in result["cleaned"]["modeling"]["model_groups"]
    assert any("filter" in v.lower() for v in result["violations"])

def test_validate_rule3_group_without_filter_kept():
    """A group dict without a filter key is valid (filter is optional)."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {"name": "Residential"}
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    assert "res" in result["cleaned"]["modeling"]["model_groups"]


# ---------------------------------------------------------------------------
# validate_settings_delta — Rule 4: skip values
# ---------------------------------------------------------------------------

def test_validate_rule4_invalid_skip_value_removed():
    """Invalid skip values should be removed from the list."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {
                    "name": "Residential",
                    "filter": ["==", "class", "R"],
                    "skip": ["all", "invalid_stage"],
                }
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    skip = result["cleaned"]["modeling"]["model_groups"]["res"]["skip"]
    assert skip == ["all"]
    assert any("invalid_stage" in v for v in result["violations"])

def test_validate_rule4_skip_not_list_removed():
    """skip that is not a list should be removed."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {
                    "name": "Residential",
                    "filter": ["==", "class", "R"],
                    "skip": "all",
                }
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    assert "skip" not in result["cleaned"]["modeling"]["model_groups"]["res"]
    assert any("skip" in v.lower() for v in result["violations"])

def test_validate_rule4_valid_skip_unchanged():
    """Valid skip values should pass through unchanged."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {
                    "name": "Residential",
                    "filter": ["==", "class", "R"],
                    "skip": ["model", "report"],
                }
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    skip = result["cleaned"]["modeling"]["model_groups"]["res"]["skip"]
    assert skip == ["model", "report"]


# ---------------------------------------------------------------------------
# validate_settings_delta — Rule 5: exclude_features type
# ---------------------------------------------------------------------------

def test_validate_rule5_exclude_features_not_list():
    """exclude_features that is not a list should be removed."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {
                    "name": "Residential",
                    "filter": ["==", "class", "R"],
                    "exclude_features": "he_id",
                }
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    group = result["cleaned"]["modeling"]["model_groups"]["res"]
    assert not isinstance(group.get("exclude_features", []), str)
    assert any("exclude_features" in v.lower() for v in result["violations"])

def test_validate_rule5_non_string_elements_removed():
    """Non-string elements in exclude_features should be removed."""
    delta = {
        "modeling": {
            "model_groups": {
                "res": {
                    "name": "Residential",
                    "filter": ["==", "class", "R"],
                    "exclude_features": ["he_id", 42, True, "sale_price"],
                }
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    exclude = result["cleaned"]["modeling"]["model_groups"]["res"]["exclude_features"]
    assert 42 not in exclude
    assert True not in exclude
    assert "he_id" in exclude
    assert any("exclude_features" in v.lower() for v in result["violations"])


# ---------------------------------------------------------------------------
# validate_settings_delta — Rule 6: dep_vars type
# ---------------------------------------------------------------------------

def test_validate_rule6_dep_vars_not_list():
    """dep_vars that is not a list should be removed."""
    delta = {
        "modeling": {
            "models": {
                "default": {"dep_vars": "sale_price"}
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    model_cfg = result["cleaned"]["modeling"]["models"]["default"]
    assert "dep_vars" not in model_cfg
    assert any("dep_vars" in v.lower() for v in result["violations"])

def test_validate_rule6_non_string_dep_var_removed():
    """Non-string elements in dep_vars should be removed."""
    delta = {
        "modeling": {
            "models": {
                "default": {"dep_vars": ["sale_price", 42]}
            }
        }
    }
    result = validate_settings_delta(delta, VALIDATION_PROFILE)
    dep_vars = result["cleaned"]["modeling"]["models"]["default"]["dep_vars"]
    assert "sale_price" in dep_vars
    assert 42 not in dep_vars
