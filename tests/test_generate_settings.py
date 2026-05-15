"""Tests for scripts/generate_settings.py — isolated unit tests."""
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import generate_settings


def test_build_settings_fills_valuation_date():
    seed = {
        "locality": {"name": "Berks County", "slug": "us-pa-berks"},
        "sources": [],
    }
    classification = {
        "land": {}, "impr": {}, "other": {},
        "important": {"fields": {}, "locations": []},
    }
    settings = generate_settings.build_settings(seed, classification, [])
    vd = settings["modeling"]["metadata"]["valuation_date"]
    assert vd != "", "valuation_date must not be empty"
    assert vd == f"{datetime.now().year}-01-01"


def test_build_settings_fills_modeler_from_locality():
    seed = {
        "locality": {"name": "Berks County", "slug": "us-pa-berks"},
        "sources": [],
    }
    classification = {
        "land": {}, "impr": {}, "other": {},
        "important": {"fields": {}, "locations": []},
    }
    settings = generate_settings.build_settings(seed, classification, [])
    meta = settings["modeling"]["metadata"]
    assert meta["modeler"] == "Berks County"
    assert meta["modeler_nick"] == "Berks"


def test_build_settings_modeler_nick_single_word():
    seed = {
        "locality": {"name": "Guilford", "slug": "us-nc-guilford"},
        "sources": [],
    }
    classification = {
        "land": {}, "impr": {}, "other": {},
        "important": {"fields": {}, "locations": []},
    }
    settings = generate_settings.build_settings(seed, classification, [])
    assert settings["modeling"]["metadata"]["modeler_nick"] == "Guilford"
