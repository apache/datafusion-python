"""
Common utilities for running TPC-H examples.
"""

import os
from pathlib import Path

USE_REF_DATA = os.environ.get("TPCH_USE_REF_DATA", "")

def get_data_path(filename: str) -> str:
    path = os.path.dirname(os.path.abspath(__file__))

    # IF the environment variable is set, always use reference data.
    # This allows users to generate a large scale data set and experiment
    # with the examples, but use the reference data when running pytest.
    if USE_REF_DATA != "":
        return os.path.join(path, "ref_data", filename)

    data_path = os.path.join(path, "data", filename)
    if not Path(data_path).is_file():
        data_path = os.path.join(path, "ref_data", filename)

    return data_path
