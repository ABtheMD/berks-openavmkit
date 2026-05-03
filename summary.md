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

## feature/run-pipeline

**Branch:** `feature/run-pipeline`
**Date:** 2026-05-03
**Status:** Open — partial fixes committed; pipeline still blocked on missing `data.process.merge` and sales-validation columns.

### Goal
Validate that `notebooks/pipeline/01-assemble.ipynb` actually runs end-to-end on the
Berks County data produced by Steps 1–2.

### What was changed
| File | Change |
|---|---|
| `notebooks/pipeline/data/us-pa-berks/in/settings.json` | Added `filter` clauses to all 7 `modeling_groups` (mapped from PA `class` field). Added minimal `load` mappings (`key` ← `propid`/`parid`, `class` ← `class`) to all 4 source files. |

#### `modeling_groups` filter mapping
```json
"res":    { "filter": ["==", "class", "R"]  }   // 140,906 parcels
"com":    { "filter": ["==", "class", "C"]  }   //   8,524
"ag":     { "filter": ["==", "class", "A"]  }   //     262
"farm":   { "filter": ["==", "class", "F"]  }   //   7,781
"ind":    { "filter": ["==", "class", "I"]  }   //   1,024
"exempt": { "filter": ["==", "class", "E"]  }   //   4,995
"util":   { "filter": ["==", "class", "UT"] }   //     242
```
Class distribution from `cama_master.parquet` (n=163,965):
also present but not mapped to any group: `UE` (131 rows), `FC` (100 rows). These will tag as `model_group = "UNKNOWN"` (~0.14% of total). Likely interpretation: UE = utility-exempt, FC = farm-commercial — left for future refinement.

### Critical discovery — settings.json is more under-specified than the previous PR realised

While trying to run 01-assemble, two large gaps surfaced that the Step-1 generator
and the manual Step-3 hand-fill did not address:

1. **`data.load.<source>.load: {}` is empty for all four sources.** Empty `load`
   means **only the `geometry` column gets loaded** (see [openavmkit/data.py:3618-3652](openavmkit/data.py)).
   Every other field — `class`, `acreage`, `valuland`, `deedamount`, etc. — that
   `field_classification` references was **silently being dropped at load time**.
   Fix in this branch: minimal load mapping (`key` + `class`) to unblock loading.
   A complete fix needs every column referenced by `field_classification` to be
   listed in `load`.

2. **`data.process.merge` does not exist.** The pipeline's `process_dataframes`
   step ([openavmkit/data.py:583-597](openavmkit/data.py)) requires
   `data.process.merge.universe` and `data.process.merge.sales` lists describing
   how to assemble the four parquets into a UNIVERSE and SALES dataframe. Without
   these, assembly raises:
   ```
   ValueError: No "universe" merge instructions found.
               data.process.merge must have exactly two keys: "universe", and "sales".
   ```

3. **Sales-validation columns are required but absent.** Even after merge is
   defined, `process_data` requires the SALES dataframe to contain `valid_sale`
   and `vacant_sale` boolean columns ([openavmkit/data.py:604-607](openavmkit/data.py)).
   These don't exist in the downloaded data — they need to be derived (e.g.
   `valid_sale = price > 0 AND price < $50M`, `vacant_sale = bldg_area == 0`).

### Run results
1. First run: crashed in `load_dataframes` with `IndexError: list index out of range`
   inside `get_dupes` — direct consequence of empty `load` (no non-geometry columns
   to use as a dedup key).
2. After load fix: all four parquets load successfully (geo_parcels: 156,430,
   cama_master: 163,964, cama_residential: 163,960, cama_commercial: 163,964).
   Then crashes in `process_dataframes` with the missing-merge ValueError above.

### Why the work stopped here
The fixes needed (full `load` mapping, `data.process.merge`, sales-validation
calc operations) are exactly what a **Step 3 settings-configuration harness**
(`scripts/configure_settings.py`) would generate. Continuing to fill them by
hand for one jurisdiction would re-implement the harness inline and risk
hard-coding Berks-specific assumptions into what should be generated rules.

The right next branch is `feature/configure-settings`: build the harness, then
re-attempt `feature/run-pipeline`.

---

## Roadmap / Future Work

The three-step pipeline to get from a seed file to a runnable openavmkit model:

| Step | Script | Status |
|---|---|---|
| **1 — Generate settings scaffold** | `scripts/generate_settings.py` | ✅ Done |
| **2 — Download data** | `scripts/download_data.py` | ✅ Done |
| **3 — Fill settings gaps** | `scripts/configure_settings.py` *(not yet built)* | 🔲 **Now blocking** Step 4 — see findings in `feature/run-pipeline` above |
| **4 — Run pipeline** | `notebooks/pipeline/01-assemble.ipynb` | 🟡 In progress on `feature/run-pipeline`; blocked on Step 3 |

### Step 3: Settings configuration harness *(now blocking — must build before Step 4 can run)*

After Steps 1 and 2, the `settings.json` still has jurisdiction-specific gaps that
cannot be filled by a simple generic script. **Updated 2026-05-03 with discoveries
from `feature/run-pipeline`:** the gaps are larger than previously catalogued. The
full list of settings-keys the pipeline needs and the generator does not currently
produce:

- `data.load.<source>.load` — column-rename map controlling **which fields actually
  get loaded** from each parquet. Empty `{}` means only `geometry` is loaded — every
  other field is silently dropped. Must list every column referenced anywhere
  downstream (field_classification, model dep_vars, calc operations, etc.).
- `data.process.merge.universe` / `data.process.merge.sales` — list of dataframe
  IDs (and optional join hints) describing how the four parquets get assembled into
  the UNIVERSE and SALES dataframes. Required by `process_dataframes`.
- Sales-validation calc operations producing `valid_sale` and `vacant_sale` boolean
  columns on the SALES dataframe. Required by `process_data`.
- `modeling.metadata.modeler`, `modeler_nick`, `valuation_date`
- `modeling.modeling_groups` — both the group definitions AND the per-group
  `filter` clauses. Property type groupings driven by the jurisdiction's
  own classification system (PA uses `class` codes R/C/A/F/I; other states differ entirely).
- `field_classification.important.fields` — mapping of openavmkit's standard role names
  (`impr_category`, `land_category`, `loc_neighborhood`, etc.) to the actual column names
  in the downloaded data, which vary by jurisdiction.
- `models.default.dep_vars` — meaningful predictors depend on which fields are actually
  populated and relevant for that jurisdiction.

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
downloader and the pipeline run. **Originally framed as future work; now the
critical-path blocker for Step 4** based on `feature/run-pipeline` discoveries.

---
