"""Build the per-class schema descriptor map the native parser consumes.

Descriptors depend only on the schema name, so the result is cached per schema
and reused across files. Rust applies them to the STEP records; Python never
touches the file's entities.
"""

from __future__ import annotations

from functools import lru_cache

from ifcopenshell import ifcopenshell_wrapper as _w

from .parser import (
    _is_aggregation_type,
    _MAX_TYPE_CHAIN_HOPS,
    _is_select_type,
    _measure_type_from_attr_type,
    measure_unit_type,
)
from .schema_dict import canonical_class_name

_SHAPE_MODEL_ROOTS = frozenset(
    {
        "IfcProductRepresentation",
        "IfcRepresentation",
        "IfcRepresentationMap",
        "IfcRepresentationItem",
        "IfcProfileDef",
    }
)


def _measure_unit(schema, attr_type) -> str | None:
    """The unit type scaling an attribute's measure, for the native `Measure`.

    A real without one is a default-measure real; the `"DIRECTION" in
    attr_name` heuristic is carried separately as the per-attribute
    `direction` flag and applied Rust-side.
    """
    measure = _measure_type_from_attr_type(attr_type)
    return measure_unit_type(schema.name(), measure) if measure else None


def _shape_of(schema, attr_type) -> dict:
    """Recursive canonicalization shape for one attribute (or aggregate item)."""
    if _is_select_type(attr_type):
        return {"k": "select"}
    if _is_aggregation_type(attr_type):
        kind = attr_type.type_of_aggregation_string()
        return {
            "k": "agg",
            "sorted": kind in {"set", "bag"},
            "elem": _shape_of(schema, attr_type.type_of_element()),
        }
    return {"k": "leaf", "m": _measure_unit(schema, attr_type)}


def _is_text(attr_type) -> bool:
    """True when the attribute's underlying simple type is a string."""
    current = attr_type
    for _ in range(_MAX_TYPE_CHAIN_HOPS):
        if _is_select_type(current):
            return False
        if _is_aggregation_type(current):
            current = current.type_of_element()
            continue
        declared = current.declared_type() if hasattr(current, "declared_type") else None
        if isinstance(declared, str):
            return declared == "string"
        if declared is None:
            return False
        current = declared
    return False


def _supertype_names(decl) -> set[str]:
    names: set[str] = set()
    current = decl
    while current is not None:
        try:
            names.add(current.name())
        except Exception:
            pass
        try:
            current = current.supertype()
        except Exception:
            break
    return names


def _entity_declarations(schema):
    for decl in schema.declarations():
        # Entity declarations expose all_attributes(); defined/select/enum
        # types do not. Probe and skip non-entities.
        try:
            decl.all_attributes()
        except Exception:
            continue
        yield decl


def _build_descriptors(schema_name: str) -> dict:
    schema = _w.schema_by_name(schema_name)
    out: dict[str, dict] = {}
    for decl in _entity_declarations(schema):
        type_name = decl.name()
        attrs = []
        guid_index = name_index = None
        derived = decl.derived()
        for index, attr in enumerate(decl.all_attributes()):
            attr_name = attr.name()
            if attr_name == "GlobalId":
                guid_index = index
            elif attr_name == "Name":
                name_index = index
            attrs.append(
                {
                    "name": attr_name,
                    "direction": "DIRECTION" in attr_name.upper(),
                    "derived": bool(derived[index]),
                    "text": _is_text(attr.type_of_attribute()),
                    "shape": _shape_of(schema, attr.type_of_attribute()),
                }
            )
        supers = _supertype_names(decl)
        out[type_name.upper()] = {
            "class": canonical_class_name(schema_name, type_name),
            "product": "IfcProduct" in supers,
            "project": "IfcProject" in supers,
            "relationship": "IfcRelationship" in supers,
            "spatial": bool(
                supers & {"IfcSpatialElement", "IfcSpatialStructureElement"}
            ),
            "geometric": bool(supers & _SHAPE_MODEL_ROOTS),
            "guid": guid_index,
            "name": name_index,
            "attrs": attrs,
        }
    measures = {}
    for decl in schema.declarations():
        unit_type = measure_unit_type(schema_name, decl.name()) if decl.name().endswith("Measure") else None
        if unit_type:
            measures[decl.name().upper()] = unit_type
    return {"classes": out, "measures": measures}


@lru_cache(maxsize=8)
def schema_descriptors_json(schema_name: str) -> str:
    """JSON descriptors for `schema_name`: classes by UPPERCASE STEP keyword, measure unit types."""
    import json

    return json.dumps(_build_descriptors(schema_name), separators=(",", ":"))
