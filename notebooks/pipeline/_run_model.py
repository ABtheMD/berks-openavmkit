# headless 03-model execution

# === cell 1 ===
locality = "us-pa-berks"
verbose = True
clear_checkpoints = True

# === cell 3 ===
import init_notebooks
init_notebooks.setup_environment()
locality = init_notebooks.check_for_different_locality(locality)

# === cell 4 ===
from openavmkit.pipeline import (
    init_notebook,
    load_settings,
    load_cleaned_data_for_modeling,
    examine_sup,
    write_canonical_splits,
    try_variables,
    try_models,
    finalize_models,
    run_and_write_ratio_study_breakdowns,
    enrich_sup_spatial_lag,
    from_checkpoint,
    delete_checkpoints,
    identify_outliers,
    write_parquet
)

# === cell 5 ===
init_notebook(locality)

# === cell 6 ===
if clear_checkpoints:
    delete_checkpoints("3-model")

# === cell 7 ===
settings = load_settings()

# === cell 8 — load cleaned data ===
sales_univ_pair = load_cleaned_data_for_modeling(settings)

# === cell 9 ===
examine_sup(sales_univ_pair, load_settings())

# === cell 10 — write canonical train/test split ===
write_canonical_splits(
    sales_univ_pair,
    load_settings(),
    verbose=verbose
)

# === cell 11 — spatial lag enrichment (checkpointed) ===
sales_univ_pair = from_checkpoint("3-model-00-enrich-spatial-lag", enrich_sup_spatial_lag,
    {
        "sup": sales_univ_pair,
        "settings": load_settings(),
        "verbose": verbose
    }
)

# === cell 12 ===
write_parquet(sales_univ_pair.universe, "out/look/3-spatial-lag-universe.parquet")
write_parquet(sales_univ_pair.sales, "out/look/3-spatial-lag-sales.parquet")

# === cell 13 ===
examine_sup(sales_univ_pair, load_settings())

# === cell 14 — variable selection ===
try_variables(
    sales_univ_pair,
    load_settings(),
    verbose,
    plot=False
)

# === cell 15 — model experiments ===
try_models(
    sup=sales_univ_pair,
    settings=load_settings(),
    save_params=True,
    verbose=verbose,
    run_main=True,
    run_vacant=False,
    run_hedonic=False,
    run_ensemble=True,
    do_shaps=False,
    do_plots=True
)

# === cell 16 — outlier identification ===
identify_outliers(
    sup=sales_univ_pair,
    settings=load_settings()
)

# === cell 17 — finalize models (checkpointed) ===
results = from_checkpoint("3-model-02-finalize-models", finalize_models,
    {
        "sup": sales_univ_pair,
        "settings": load_settings(),
        "save_params": True,
        "use_saved_params": True,
        "verbose": verbose
    }
)

# === cell 18 — ratio study reports ===
run_and_write_ratio_study_breakdowns(load_settings())
