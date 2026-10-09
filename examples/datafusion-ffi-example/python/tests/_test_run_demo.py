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
    assert "1. table provider" in result.stdout
    assert "2. functions" in result.stdout
    assert "3. catalog provider" in result.stdout
    assert "4. config extension" in result.stdout
    assert "5. codec round-trip" in result.stdout
    assert "6. the same bytes decoded a second time" in result.stdout
