"""Schema-derived class dictionary for the signature pipeline."""

from __future__ import annotations

from functools import lru_cache

from ifcopenshell import ifcopenshell_wrapper as _w

_CASE_SUFFIXES = ("StandardCase", "ElementedCase")


@lru_cache(maxsize=8)
def _case_supertypes(schema_name: str) -> dict[str, str]:
    """Every `*StandardCase` / `*ElementedCase` entity mapped to its supertype."""
    collapsed: dict[str, str] = {}
    for declaration in _w.schema_by_name(schema_name).declarations():
        name = declaration.name()
        if name.endswith(_CASE_SUFFIXES) and hasattr(declaration, "supertype"):
            collapsed[name] = declaration.supertype().name()
    return collapsed


def canonical_class_name(schema_name: str, entity_type: str) -> str:
    """Matcher class of `entity_type`: case subtypes collapse to their supertype."""
    return _case_supertypes(schema_name).get(entity_type, entity_type)
