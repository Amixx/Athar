"""Pinned schema dictionary helpers for Phase 1 parsing."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from ifcopenshell.util import schema as schema_util


class SchemaDictionary:
    """Thin schema helper around IfcOpenShell declarations."""

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name

    @lru_cache(maxsize=4096)
    def attribute_names(self, entity: Any) -> tuple[str, ...]:
        declaration = schema_util.get_declaration(entity)
        names: list[str] = []
        for i in range(len(entity)):
            names.append(declaration.attribute_by_index(i).name())
        return tuple(names)


def canonical_class_name(entity_type: str) -> str:
    """Collapse selected IFC concrete classes to canonical matcher classes."""
    if entity_type == "IfcWallStandardCase":
        return "IfcWall"
    return entity_type
