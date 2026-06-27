"""Build the per-class schema descriptor map the native parser consumes.

This is the one place ifcopenshell's EXPRESS schema is read for the native
path. It is *file-independent* — descriptors depend only on the schema name —
so the result is cached per schema and reused across files. Rust applies these
descriptors to the millions of STEP records; Python never touches the file's
entities.
"""

from __future__ import annotations

from functools import lru_cache

from ifcopenshell import ifcopenshell_wrapper as _w

from .parser import (
    _ANGLE_MEASURES,
    _AREA_MEASURES,
    _LENGTH_MEASURES,
    _VOLUME_MEASURES,
    _is_aggregation_type,
    _is_select_type,
    _measure_type_from_attr_type,
)
from .schema_dict import canonical_class_name


def _measure_kind(attr_type) -> str:
    """Map an attribute's measure to the native `Measure` enum tag.

    Returns one of length/area/volume/angle/default. The `"DIRECTION" in
    attr_name` heuristic is carried separately as the per-attribute
    `direction` flag and applied Rust-side for default-measure reals.
    """
    measure = _measure_type_from_attr_type(attr_type)
    if not measure:
        return "default"
    upper = measure.upper()
    if upper in _LENGTH_MEASURES:
        return "length"
    if upper in _AREA_MEASURES:
        return "area"
    if upper in _VOLUME_MEASURES:
        return "volume"
    if upper in _ANGLE_MEASURES:
        return "angle"
    return "default"


def _shape_of(attr_type) -> dict:
    """Recursive canonicalization shape for one attribute (or aggregate item)."""
    if _is_select_type(attr_type):
        return {"k": "select"}
    if _is_aggregation_type(attr_type):
        kind = attr_type.type_of_aggregation_string()
        return {
            "k": "agg",
            "sorted": kind in {"set", "bag"},
            "elem": _shape_of(attr_type.type_of_element()),
        }
    return {"k": "leaf", "m": _measure_kind(attr_type)}


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
                    "shape": _shape_of(attr.type_of_attribute()),
                }
            )
        supers = _supertype_names(decl)
        out[type_name.upper()] = {
            "class": canonical_class_name(type_name),
            "product": "IfcProduct" in supers,
            "spatial": bool(
                supers & {"IfcSpatialElement", "IfcSpatialStructureElement"}
            ),
            "guid": guid_index,
            "name": name_index,
            "attrs": attrs,
        }
    return out


@lru_cache(maxsize=8)
def schema_descriptors_json(schema_name: str) -> str:
    """JSON descriptor map for `schema_name`, keyed by UPPERCASE STEP keyword."""
    import json

    return json.dumps(_build_descriptors(schema_name), separators=(",", ":"))
