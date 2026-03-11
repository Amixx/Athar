#!/usr/bin/env python3
"""Generate SAMPLE_REPORT.md from the test fixtures."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
OUTPUT = ROOT / "docs" / "SAMPLE_REPORT.md"


def main():
    result = subprocess.run(
        ["python", "-m", "athar_layers", str(FIXTURES), "--report", str(OUTPUT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise SystemExit(1)
    print(f"Written to {OUTPUT}")


if __name__ == "__main__":
    main()
