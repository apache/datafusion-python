import subprocess
import sys
from pathlib import Path


def test_run_demo():
    script = Path(__file__).parent.parent.parent / "run_demo.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "1. logical plan" in result.stdout
    assert "2. physical plan returned" in result.stdout
    assert "3. effect of SET ffi_query_planner.max_rows" in result.stdout
    assert "4. two planners nesting" in result.stdout
