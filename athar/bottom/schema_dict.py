"""Pinned schema dictionary helpers for core parsing."""

from __future__ import annotations


def canonical_class_name(entity_type: str) -> str:
    """Collapse selected IFC concrete classes to canonical matcher classes."""
    if entity_type == "IfcWallStandardCase":
        return "IfcWall"
    return entity_type
