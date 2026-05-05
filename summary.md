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

## feature/configure-settings

**Branch:** `feature/configure-settings`
**Date:** 2026-05-03
**Status:** Complete — pipeline validated end-to-end for Berks County

### Goal
Build `scripts/configure_settings.py` to fill in the `data.load` and `data.process.merge`
sections that `generate_settings.py` leaves blank, and validate the full `01-assemble.ipynb`
pipeline on real Berks County data.

### Files changed
| File | Change |
|---|---|
| `scripts/configure_settings.py` | New — fills data.load mappings, calc ops, data.process.merge |
| `scripts/download_data.py` | Fix — convert ArcGIS Date fields (Unix ms) to datetime64 at download time |
| `scripts/generate_settings.py` | Fix — emit `model_groups` (correct key) not `modeling_groups` |
| `notebooks/pipeline/data/us-pa-berks/in/settings.json` | Updated — load mappings, merge, model_groups with filters |
| `notebooks/pipeline/data/us-pa-berks/in/*.parquet` | Patched — date columns converted from float ms to datetime64 |

### configure_settings.py — what it generates
Run: `python scripts/configure_settings.py seeds/seed_us-pa-berks.json`

1. **`data.load.<source>.load`** — maps raw column names to canonical names for every source:
   - parcel id → `key`
   - sale price → `sale_price`, sale date → `sale_date` (sales source only)
   - every `field_classification` field found in that parquet → itself
2. **`data.load.<source>.calc`** — on the sales source: `key_sale`, `valid_sale`, `vacant_sale`
3. **`data.load.geo_parcels.dupes`** — explicit dedup on `key` (prevents auto-pick of wrong column)
4. **`data.process.merge`** — universe (geo_parcels base + left-join each other source) and sales

One-time parquet patch: `python scripts/configure_settings.py seeds/... --patch-dates`
Converts float64 Unix-ms date columns to datetime64 in existing parquets.

### Bugs discovered and fixed during pipeline validation

| Error | Root cause | Fix (no library changes) |
|---|---|---|
| `IndexError: list index out of range` in `get_dupes` | Empty `"load": {}` → only geometry loaded; auto-dedup found no columns | `configure_settings.py` now fills all `data.load` mappings |
| `ValueError: No "universe" merge instructions` | `data.process.merge` section entirely absent | `configure_settings.py` now generates `data.process.merge` |
| `ValueError: Unknown operation: >` | Calc format used `>` directly; that's a filter operator, needs `["?", ...]` wrapper in calc context | Fixed in `build_sales_calcs()`: `"valid_sale": ["?", ["and", [">", ...], ...]]` |
| geo_parcels deduped on wrong column (`planbkpg` → 12k rows instead of 156k) | `get_dupes` auto mode picks first non-geometry column, which wasn't `key` | `configure_settings.py` adds explicit `"dupes": {"subset": ["key"], ...}` to geo_parcels entry |
| `ValueError: Date field 'sale_date' does not have a time format` | ArcGIS Date fields stored as float64 Unix ms; pipeline `enrich_time` needs datetime64 | `download_data.py` now converts ArcGIS Date fields at download time; `--patch-dates` flag patches existing parquets |
| `ValueError: You must define at least one model group` | Settings used key `modeling_groups`; pipeline reads `model_groups` | Fixed `generate_settings.py` to emit `model_groups`; manually set in Berks settings.json |
| `ValueError: Could not find field named "R"` | Filter string literals not prefixed with `str:` | Changed filters to `["==", "class", "str:R"]` etc. |

### openavmkit filter/calc syntax (key discoveries)
- **Calc operators**: `+`, `asstr`, `and`, `?`, `datetime`, `datetimestr`, etc. (in `calculations.py`)
- **Filter operators**: `==`, `>`, `<`, `>=`, `<=`, `isin`, `and`, `or`, etc. (in `filters.py`)
- **Bridge**: `["?", <filter_expr>]` in a calc context invokes `resolve_filter` → returns boolean Series
- **String literals in filters**: must use `str:` prefix — e.g. `["==", "class", "str:R"]`
- **`data.load.<source>.load`** semantics: empty `{}` → only geometry loaded (all other columns silently dropped)

### Berks County 01-assemble.ipynb results
| Model group | Parcels | Sales |
|---|---|---|
| Residential (R) | 133,913 | 102,558 |
| Commercial (C) | 8,209 | 6,410 |
| Farmland / Forest (F) | 7,768 | 4,694 |
| Tax Exempt (E) | 4,571 | 1,519 |
| Industrial (I) | 986 | 683 |
| Agricultural (A) | 254 | 190 |
| Utility (UT) | 210 | 67 |
| UNKNOWN (no class) | 519 | — |
| **Total** | **156,430** | **120,121** |

Output files written to `notebooks/pipeline/data/us-pa-berks/out/`:
- `1-assemble-sup.pickle`
- `look/1-assemble-universe.parquet` (156,430 × 343 columns)
- `look/1-assemble-sales.parquet`
- `look/1-assemble-sales-hydrated.parquet`

---

## fix: NA handling for calc-output boolean columns

**Date:** 2026-05-04
**Status:** Fix applied to `openavmkit/data.py`; kept local (branch deleted, intentionally set to the side)

### Background
After validating 01-assemble.ipynb, two `UserWarning` messages appeared on every run:
```
UserWarning: No NA handling specified for boolean field 'valid_sale'. Defaulting to 'na_false'.
UserWarning: No NA handling specified for boolean field 'vacant_sale'. Defaulting to 'na_false'.
```

### Root cause
`load_dataframe` in `openavmkit/data.py` fires a `UserWarning` for any boolean column that lacks an entry in `extra_map`. Columns produced by calc operations (`valid_sale`, `vacant_sale`) can never get such an entry through the load dict — they have no raw source column in the parquet file. So the warning fired on every pipeline run even though the default behavior (`na_false`) was always correct.

### Fix (`openavmkit/data.py`, inside `load_dataframe`)
Inserted a loop between the calc/tweak execution block and the dtype-enforcement loop. For any column that (a) appears in a `calc` operation, (b) has a boolean dtype, and (c) has no existing `extra_map` entry, the loop seeds `extra_map[col] = "na_false"` before the dtype-enforcement loop runs.

```python
for operation in operation_order:
    if operation["type"] == "calc":
        for calc_col in operation["operations"]:
            if calc_col in df.columns and pd.api.types.is_bool_dtype(df[calc_col]):
                if calc_col not in extra_map:
                    extra_map[calc_col] = "na_false"
```

**Key discovery:** calc output dtype is numpy `dtype('bool')`, not pandas `BooleanDtype`. Checking `== "boolean"` fails; `pd.api.types.is_bool_dtype()` is required to catch both.

### openavmkit editable install (required for fork's data.py to take effect)
The pipeline resolves `import openavmkit` from wherever pip installed it. To redirect it to the fork:
```bash
pip install -e . --no-deps   # run from berks-openavmkit root
```
`--no-deps` is needed because `requirements.txt` pins `pipreqs==0.5.0` which does not exist on PyPI (upstream uses `0.4.13`).

### Verification
- Cleared checkpoints at `out/checkpoints/`, re-ran 01-assemble on Berks County
- Zero `UserWarning` messages in pipeline output
- All output files and parcel/sales counts unchanged (156,430 parcels, 120,121 sales)

---

## fix: vacant_sale calc + class field in cama_master

**Date:** 2026-05-05
**Status:** Fix applied to `settings.json`; 02-clean validated end-to-end

### Background
After 01-assemble ran cleanly, 02-clean (`_run_clean.py`) was hitting a `TypeError: 'float' object cannot be interpreted as an integer` inside `_get_expected_periods`. Root cause traced to empty sales DataFrames for non-residential model groups.

### Root cause chain
1. `enrich_df_vacancy` sets `is_vacant = True` when `bldg_area_finished_sqft == 0`
2. Non-residential properties (A, C, F, I, E, UT) have no residential CAMA data → `bldg_area_finished_sqft = 0` → `is_vacant = True`
3. `_get_sales` (inside `_determine_value_driver`) sets `valid_sale = False` when `~vacant_sale & is_vacant` — so any property that is vacant but not flagged as a vacant sale gets invalidated
4. All non-res sales had `vacant_sale = False` (old calc only checked `price < 0`) → all invalidated → empty `df_sales`
5. Empty `df_sales` → `_determine_value_driver` returns "impr" → `_crunch_time_adjustment` gets empty `df_per` → `sale_date.min()` returns `NaT` → `.year` returns `nan` (float) → `range(nan, nan+1)` → `TypeError`

### Fixes applied to `settings.json`

**1. Added `class` field to `cama_master.load`:**
```json
"load": {
  "key": "parid", "sale_price": "price", "sale_date": "saledt",
  "livunit": "livunit",
  "class": "class"
}
```

**2. Expanded `vacant_sale` calc to mark non-residential PA classes as vacant sales:**
```json
"vacant_sale": ["?", ["or",
  ["<", "sale_price", 0],
  ["isin", "class", ["A", "C", "F", "I", "E", "UT"]]
]]
```

### Key discovery: `isin` list items don't use `str:` prefix

The `str:` prefix (needed for single-value `==` comparisons) is NOT used for `isin` list items. The filter engine checks `isinstance(value, str)` before stripping `str:` — for lists that branch is skipped, so items are passed as-is to `df[field].isin(value)`. Using `"str:A"` in an `isin` list never matches the actual column value `"A"`.

| Filter context | Correct syntax |
|---|---|
| `["==", "class", "str:R"]` | single string → needs `str:` prefix |
| `["isin", "class", ["A", "C", "F"]]` | list → NO `str:` prefix |

### Berks County 02-clean.ipynb results

| Model group | Parcels | Sales (pre-scrutiny) | After heuristics | Vacant | Improved |
|---|---|---|---|---|---|
| Residential (R) | 133,913 | 106,204 | 25,565 | 0 | 25,565 |
| Commercial (C) | 8,209 | 6,529 | 1,533 | 1,533 | 0 |
| Farmland / Forest (F) | 7,768 | 4,706 | 232 | 232 | 0 |
| Tax Exempt (E) | 4,571 | 1,704 | 99 | 99 | 0 |
| Industrial (I) | 986 | 708 | 109 | 109 | 0 |
| Agricultural (A) | 254 | 197 | 20 | 20 | 0 |
| Utility (UT) | 210 | 71 | — | — | — |
| **Total (res ratio study)** | — | **49,026** | — | **5,686** | **43,340** |

Time adjustment: calculated for all 7 model groups (period = Y). Heuristics dropped 18,031 invalid sales (17,307 duplicate date/price pairs, 1,471 false vacants).

Output files written to `notebooks/pipeline/data/us-pa-berks/out/`:
- `2-clean-sup.pickle`
- `look/2-clean-universe.parquet`
- `look/2-clean-sales.parquet`
- `look/2-clean-sales-hydrated.parquet`

Warnings (non-fatal, no action needed):
- `land equity clusters but no analysis.land_equity.location` — location field not yet wired up for land equity
- `no deed_id in analysis.sales_scrutiny.deed_id` — deed-based heuristic skipped (no deed field in Berks data)

---

## Roadmap / Future Work

The pipeline to get from a seed file to a runnable openavmkit model:

| Step | Script | Status |
|---|---|---|
| **1 — Generate settings scaffold** | `scripts/generate_settings.py` | ✅ Done |
| **2 — Download data** | `scripts/download_data.py` | ✅ Done |
| **3 — Fill settings gaps** | `scripts/configure_settings.py` | ✅ Done |
| **3b — Fill model_groups + important.fields** | Manual (jurisdiction-specific) | ✅ Done for Berks |
| **4a — Run 01-assemble.ipynb** | `notebooks/pipeline/01-assemble.ipynb` | ✅ Done |
| **4b — Run 02-clean.ipynb** | `notebooks/pipeline/02-clean.ipynb` | ✅ Done |
| **4c — Run 03-model.ipynb** | `notebooks/pipeline/03-model.ipynb` | 🔲 Future |

### Step 3b: model_groups (manual, jurisdiction-specific)

After `configure_settings.py`, set `modeling.model_groups` manually in `settings.json`:

```json
"model_groups": {
  "res": { "name": "Residential", "filter": ["==", "class", "str:R"] },
  "com": { "name": "Commercial",  "filter": ["==", "class", "str:C"] }
}
```

Key rules:
- The correct settings key is `model_groups` (NOT `modeling_groups`)
- String literals in filters require `str:` prefix
- Filter order matters — first matching group wins

### Step 3b: field_classification.important.fields (manual)

Maps openavmkit's standard role names to actual local column names. Already completed for Berks
(see the `field_classification.important.fields` section in settings.json).

---
