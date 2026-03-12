"""Deterministic serialization and environment fingerprint helpers."""

from __future__ import annotations

import json
import platform
import sys
from typing import Any

import ifcopenshell


def canonical_json(payload: Any) -> str:
    """Serialize payload to a deterministic JSON string."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def environment_fingerprint() -> dict[str, Any]:
    """Runtime fingerprint used by determinism fixtures."""
    return {
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "version_info": {
                "major": sys.version_info.major,
                "minor": sys.version_info.minor,
                "micro": sys.version_info.micro,
            },
        },
        "ifcopenshell": {
            "version": getattr(ifcopenshell, "version", "unknown"),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
    }
