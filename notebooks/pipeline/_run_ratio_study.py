# Run ratio study only (finalize_models already checkpointed)
locality = "us-pa-berks"

import init_notebooks
init_notebooks.setup_environment()
locality = init_notebooks.check_for_different_locality(locality)

from openavmkit.pipeline import (
    init_notebook,
    load_settings,
    run_and_write_ratio_study_breakdowns,
)

init_notebook(locality)
run_and_write_ratio_study_breakdowns(load_settings())
print("RATIO STUDY COMPLETE")
