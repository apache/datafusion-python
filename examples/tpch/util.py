"""
Common utilities for running TPC-H examples.
"""

import os
from pathlib import Path

def get_data_path(filename: str) -> str:
    path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(path, "data", filename)

def get_answer_file(answer_file: str) -> str:
    path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(path, "../../benchmarks/tpch/data/answers", f"{answer_file}.out")
