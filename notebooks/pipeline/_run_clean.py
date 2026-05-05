# headless 02-clean execution

# === cell 1 ===
locality = "us-pa-berks"
verbose = True
clear_checkpoints = True
sales_scrutiny_drop_outliers = False   # flag only, don't drop
sales_scrutiny_drop_heuristics = True  # drop heuristic outliers

# === cell 3 ===
import init_notebooks
init_notebooks.setup_environment()
locality = init_notebooks.check_for_different_locality(locality)

# === cell 4 ===
from openavmkit.pipeline import (
    init_notebook,
    from_checkpoint,
    delete_checkpoints,
    write_checkpoint,
    read_pickle,
    load_settings,
    examine_sup,
    fill_unknown_values_sup,
    process_sales,
    mark_ss_ids_per_model_group_sup,
    mark_horizontal_equity_clusters_per_model_group_sup,
    run_sales_scrutiny,
    write_notebook_output_sup
)

# === cell 5 ===
init_notebook(locality)

# === cell 6 ===
if clear_checkpoints:
    delete_checkpoints("2-clean")

# === cell 8 ===
settings = load_settings()

# === cell 9 ===
sales_univ_pair = read_pickle("out/1-assemble-sup")

# === cell 10 ===
examine_sup(sales_univ_pair, settings)

# === cell 12 ===
sales_univ_pair = fill_unknown_values_sup(sales_univ_pair, settings)

# === cell 14 ===
settings = load_settings()
sales_univ_pair = from_checkpoint("2-clean-00-horizontal-equity", mark_horizontal_equity_clusters_per_model_group_sup,
    {
        "sup": sales_univ_pair,
        "settings": settings,
        "verbose": verbose,
        "do_land_clusters": True,
        "do_impr_clusters": True
    }
)

# === cell 16 ===
sales_univ_pair = from_checkpoint("2-clean-01-process_sales", process_sales,
    {
        "sup": sales_univ_pair,
        "settings": load_settings(),
        "verbose": verbose
    }
)

# === cell 18 ===
sales_univ_pair = from_checkpoint("2-clean-02-sales-scrutiny", run_sales_scrutiny,
    {
        "sup": sales_univ_pair,
        "settings": load_settings(),
        "drop_cluster_outliers": sales_scrutiny_drop_outliers,
        "drop_heuristic_outliers": sales_scrutiny_drop_heuristics,
        "verbose": verbose
    }
)

# === cell 20 ===
write_notebook_output_sup(sales_univ_pair, "2-clean")
