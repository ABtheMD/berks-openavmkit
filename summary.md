# berks-openavmkit — Work Log

Personal record of work done on this fork of [openavmkit](https://github.com/openavmkit/openavmkit).

---

## Initial Setup

**Branch:** `master`
**Date:** 2026-05-02

### Git authentication
- Confirmed remote: `https://github.com/ABtheMD/berks-openavmkit.git`
- Git Credential Manager (GCM) was already installed at system level (`credential.helper = manager`)
- Cleared any stale GitHub credentials from Windows Credential Manager
- Re-authenticated via GCM OAuth browser flow — stored under `LegacyGeneric:target=git:https://github.com`
- Verified read access (`git ls-remote`) and write access (`git push --dry-run`)

### Git identity
Set global git identity (applies to all repos on this machine):
```
user.name  = ABtheMD
user.email = 20007868+ABtheMD@users.noreply.github.com
```
Stored in `C:\Users\Andre\.gitconfig`.

---

## feature/settings-generator

**Branch:** `feature/settings-generator`
**Date:** 2026-05-02
**Status:** In progress

### Goal
Build a general-purpose Python script that generates a `settings.json` file for
openavmkit from a jurisdiction seed file, without requiring the user to hand-edit
the large settings template.

### Files added
| File | Description |
|---|---|
| `scripts/generate_settings.py` | The generator script |
| `seeds/seed_us-pa-berks.json` | Seed file for Berks County, PA |
| `seeds/seed_us-pa-philadelphia.json` | Seed file for Philadelphia, PA |
| `.gitignore` | Added `.claude/` entry to exclude Claude Code metadata |

### How the script works
1. Reads a seed JSON file (`seeds/*.json`) containing locality metadata and ArcGIS Feature Server URLs
2. Queries each ArcGIS endpoint for field schema only (`?f=json`) — **no data is downloaded**
3. Maps raw ArcGIS column names to openavmkit canonical names via `data_dictionary.json` (exact + fuzzy matching)
4. Classifies fields into `land / impr / other` and `numeric / categorical / boolean` using:
   - ArcGIS field type (`esriFieldTypeDouble` → numeric, `esriFieldTypeString` → categorical, etc.)
   - Source role (`geo_parcels` → land, `cama_residential` → impr, etc.)
5. Writes a minimal, valid `settings.json` with `__` comment keys throughout as guidance

### Usage
```bash
# From inside the repo root
python scripts/generate_settings.py seeds/seed_us-pa-berks.json
# → writes to in/settings.json (default)

python scripts/generate_settings.py seeds/seed_us-pa-berks.json --output path/to/settings.json
python scripts/generate_settings.py seeds/seed_us-pa-berks.json --dry-run
```

### Results on Berks seed (live test)
- Sources: 4 Feature Servers (`geo_parcels`, `cama_master`, `cama_residential`, `cama_commercial`)
- Fields fetched: 323 total
- Matched to canonical names: 9
- Unmatched (kept as raw names): 314
- Note: low match rate is expected — Berks uses local column names (`propid`, `acreage`, `deedamount`, etc.)
  that don't directly correspond to openavmkit canonical names. User must complete the field mapping.

### What still needs manual review after running the script
- `modeling.metadata.modeler`, `modeler_nick`, `valuation_date` — left blank
- `modeling.modeling_groups` — highly jurisdiction-specific, left empty
- `field_classification.important.fields` — maps standard role names to local column names
- `models.default.dep_vars` — auto-suggested from matched numeric fields; needs trimming
- `data.load` filenames — assumes `{handle}.parquet`; adjust if download pipeline uses different names

---
