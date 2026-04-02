from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_engine_modules_do_not_import_legacy_graph_or_diff():
    forbidden = ("from athar.diff", "import athar.diff", "from athar.graph", "import athar.graph")
    engine_files = [
        *sorted((ROOT / "athar" / "bottom").glob("*.py")),
        *sorted((ROOT / "athar" / "matcher").glob("*.py")),
        *sorted((ROOT / "athar" / "delta").glob("*.py")),
        ROOT / "athar" / "engine.py",
    ]
    for path in engine_files:
        content = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in content, f"{path} contains forbidden import: {token}"
