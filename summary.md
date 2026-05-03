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
**Status:** Merged into master (PR #1)

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

## feature/data-downloader

**Branch:** `feature/data-downloader`
**Date:** 2026-05-02
**Status:** Merged into master (PR #2)

### Goal
Build a data downloader that fetches actual parquet/geoparquet files from
the ArcGIS Feature Server URLs in a seed file, saving them to the correct
location for the openavmkit pipeline to consume.

### Files changed
| File | Change |
|---|---|
| `scripts/download_data.py` | New — the downloader script |
| `scripts/generate_settings.py` | Fix — `"dtypes": {}` → `"load": {}`, add `"geometry": true` for geo_parcels |
| `notebooks/pipeline/data/us-pa-berks/in/settings.json` | New — completed Berks County settings file |
| `.gitignore` | Updated — ignore `*.parquet` / `out/` per-file instead of whole data dir; settings.json now tracked |

### How the downloader works
1. Reads a seed file — same format used by `generate_settings.py`
2. For each `feature_server` source, paginates through all records via ArcGIS `/query` endpoint
3. **geo_parcels role**: fetches as GeoJSON (`f=geojson&outSR=4326`), saves as GeoParquet via geopandas
4. **All other roles**: fetches as JSON attributes only, saves as plain parquet via pandas
5. All column names are lowercased for consistency with openavmkit conventions
6. After downloading, patches any `settings.json` found in the output directory:
   - Adds `"geometry": true` to the `geo_parcels` load entry
   - Renames `"dtypes": {}` → `"load": {}` (pipeline expects this key)

### Usage
```bash
# Download all sources for Berks County
python scripts/download_data.py seeds/seed_us-pa-berks.json
# → saves to notebooks/pipeline/data/us-pa-berks/in/

# Download a single source (useful for testing)
python scripts/download_data.py seeds/seed_us-pa-berks.json --source geo_parcels

# Custom output directory
python scripts/download_data.py seeds/seed_us-pa-berks.json --out-dir path/to/in/

# Larger page size for faster servers
python scripts/download_data.py seeds/seed_us-pa-berks.json --page-size 2000
```

### Live test results — Berks County
| File | Size | Records | Notes |
|---|---|---|---|
| `geo_parcels.parquet` | 37 MB | — | GeoParquet, EPSG:4326, 35 columns |
| `cama_residential.parquet` | 18 MB | 169,484 | 196 columns |
| `cama_commercial.parquet` | 15 MB | 169,484 | 232 columns |
| `cama_master.parquet` | 12 MB | — | 49 columns |

### Berks County settings.json — what was filled in manually (Step 3)
After downloading, the following gaps were completed by hand for Berks:
- `modeler`: Berks County / BerksCo, `valuation_date`: 2025-01-01
- `modeling_groups`: res, com, ag, farm, ind, exempt, util (based on PA `class` field: R/C/A/F/I/E/UT)
- `dep_vars`: `sfla`, `yrblt`, `bedrooms`, `fullbaths`, `halfbaths`, `stories`, `acreage`, `finbsmtarea`, `location`, `phycond`
- `field_classification.important.fields`:
  - `impr_category` → `luc` (PA land use code: 101=SF residential, 102=duplex, etc.)
  - `land_category` → `class` (PA property class: R/C/A/F/I/E)
  - `loc_neighborhood` → `location` (CAMA 1–9 location quality code)
  - `loc_market_area` → `municipalname` (one of 73 Berks municipalities)
  - `loc_region` → `school` (school district)
- `important.locations`: `municipalname`, `muni`, `school`, `tax_dist_name`, `location`

### Pipeline input requirements (discovered during design)
- Files must live at `notebooks/pipeline/data/{slug}/in/`
- `geo_parcels` is **required** by the pipeline and must have a `geometry` column
- Non-geo sources (CAMA tables) are plain parquet, no geometry needed
- `data.load` entries in settings.json use `"load": {}` for column mapping (not `"dtypes"`)

---

## Roadmap / Future Work

The three-step pipeline to get from a seed file to a runnable openavmkit model:

| Step | Script | Status |
|---|---|---|
| **1 — Generate settings scaffold** | `scripts/generate_settings.py` | ✅ Done |
| **2 — Download data** | `scripts/download_data.py` | ✅ Done |
| **3 — Fill settings gaps** | *(no script yet — see note below)* | 🔲 Future |
| **4 — Run pipeline** | `notebooks/pipeline/01-assemble.ipynb` | 🔲 Next |

### Step 3: Settings configuration harness *(future work)*

After Steps 1 and 2, the `settings.json` still has jurisdiction-specific gaps that
cannot be filled by a simple generic script:

- `modeling.metadata.modeler`, `modeler_nick`, `valuation_date`
- `modeling.modeling_groups` — property type groupings driven by the jurisdiction's
  own classification system (PA uses `class` codes R/C/A/F/I; other states differ entirely)
- `field_classification.important.fields` — mapping of openavmkit's standard role names
  (`impr_category`, `land_category`, `loc_neighborhood`, etc.) to the actual column names
  in the downloaded data, which vary by jurisdiction
- `models.default.dep_vars` — meaningful predictors depend on which fields are actually
  populated and relevant for that jurisdiction

**Why a simple script can't do this:**
Every jurisdiction structures its CAMA data differently. Berks County uses `class` for
property type and `luc` for building subtype; another county might use a single numeric
code, or a completely different taxonomy. The field that means "neighborhood" in one
county might not exist at all in another.

**Proposed future approach — a configuration harness:**
Build an interactive or semi-automated Step 3 that:
1. Inspects the downloaded parquet files (unique values, null rates, cardinality)
2. Presents candidate fields for each gap with sample values
3. Lets the user confirm or override each mapping
4. Writes the completed `settings.json`

This harness would live at `scripts/configure_settings.py` and sit between the
downloader and the pipeline run. It is the highest-value next script to build after
the pipeline has been validated end-to-end for at least one jurisdiction.

---
