"""Byte-offset STEP index for random access diagnostics."""

from __future__ import annotations

import re

_STEP_ID_PATTERN = re.compile(rb"#([0-9]+)\s*=")


def build_step_index(filepath: str) -> dict[int, int]:
    """Return STEP id -> byte-offset index from the raw IFC file."""
    with open(filepath, "rb") as f:
        payload = f.read()
    out: dict[int, int] = {}
    for match in _STEP_ID_PATTERN.finditer(payload):
        step_id = int(match.group(1))
        out.setdefault(step_id, match.start())
    return out

