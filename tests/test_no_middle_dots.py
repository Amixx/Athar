from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN = "\u00b7\u2219\u22c5\u30fb"

TEXT_SUFFIXES = {".py", ".rs", ".md", ".ts", ".js", ".svelte", ".css", ".html", ".svg", ".yml", ".toml", ".txt", ".json"}
TEXT_NAMES = {"Makefile"}
SKIPPED_DIRS = ("tests/fixtures/", "corpus/", "corpus-data/", "real-world-test/", "data/")


def _tracked_text_files() -> list[Path]:
    listing = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    paths = []
    for name in filter(None, listing.split("\0")):
        path = ROOT / name
        if name.startswith(SKIPPED_DIRS) or not path.is_file():
            continue
        if path.suffix in TEXT_SUFFIXES or path.name in TEXT_NAMES:
            paths.append(path)
    return paths


def test_no_middle_dots_in_user_visible_text():
    hits = []
    for path in _tracked_text_files():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if any(char in line for char in FORBIDDEN):
                hits.append(f"{path.relative_to(ROOT)}:{number}")
    assert not hits, "middle dot or look-alike found:\n" + "\n".join(hits)
