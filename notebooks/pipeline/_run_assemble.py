# headless 01-assemble execution

# === cell 1 ===
# Change these as desired

# The slug of the locality you are currently working on
locality = "us-pa-berks"

# Whether to print out a lot of stuff (can help with debugging) or stay mostly quiet
verbose = True

# Clear previous state for this notebook and start fresh
clear_checkpoints = True

# === cell 3 ===
import init_notebooks
init_notebooks.setup_environment()
locality = init_notebooks.check_for_different_locality(locality)

# === cell 4 ===
# import OpenAVMkit:
from openavmkit.pipeline import ( 
    init_notebook,
    from_checkpoint,
    delete_checkpoints,
    examine_df,
    examine_df_in_ridiculous_detail,
    examine_sup,
    examine_sup_in_ridiculous_detail,
    cloud_sync,
    load_settings,
    load_dataframes,
    process_dataframes,
    process_sales,
    enrich_sup_streets,
    tag_model_groups_sup,
    write_notebook_output_sup
)

# === cell 5 ===
init_notebook(locality)

# === cell 6 ===
if clear_checkpoints:
    delete_checkpoints("1-assemble")

# === cell 9 ===
settings = load_settings()

# === cell 11 ===
# load all of our initial dataframes, but don't do anything with them just yet
dataframes = from_checkpoint("1-assemble-01-load_dataframes", load_dataframes,
    {
        "settings": load_settings(),
        "verbose": verbose
    }
)

# === cell 12 ===
# load all of our initial dataframes and assemble our data
sales_univ_pair = from_checkpoint("1-assemble-02-process_data", process_dataframes,
    {
        "dataframes": dataframes,
        "settings": load_settings(), 
        "verbose": verbose
    }
)

# === cell 13 ===
# calculate street frontages
sales_univ_pair = from_checkpoint("1-assemble-03-enrich_streets", enrich_sup_streets,
    {
        "sup": sales_univ_pair,
        "settings":load_settings(), 
        "verbose":verbose
    }
)

# === cell 20 ===
sales_univ_pair = from_checkpoint("1-assemble-04-tag_modeling_groups", tag_model_groups_sup,
    {
        "sup": sales_univ_pair, 
        "settings": load_settings(), 
        "verbose": verbose
    }
)

# === cell 22 ===
write_notebook_output_sup(
    sales_univ_pair, 
    "1-assemble", 
    parquet=True, 
    gpkg=False, 
    shp=False
)
