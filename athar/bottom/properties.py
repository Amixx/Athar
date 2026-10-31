"""Property and quantity value extraction from an IFC file.

Captures raw (non-canonicalized) property values for later use in
human-readable delta reports.  Uses ifcopenshell directly so values
preserve their original scale and unit — no quantization.
"""

from __future__ import annotations

import json

from .types import OrphanPropertySet, PropertyEntry


def extract_properties(ifc) -> dict[int, list[PropertyEntry]]:
    """Extract property and quantity values keyed by product step_id."""

    index: dict[int, list[PropertyEntry]] = {}

    _collect_direct_properties(ifc, index)
    _collect_type_properties(ifc, index)

    return index


def extract_orphan_property_sets(ifc) -> tuple[OrphanPropertySet, ...]:
    """Property set definitions that no relationship or type object references."""

    referenced: set[int] = set()
    for rel in ifc.by_type("IfcRelDefinesByProperties"):
        definition = rel.RelatingPropertyDefinition
        for item in definition if isinstance(definition, tuple) else (definition,):
            if item is not None:
                referenced.add(item.id())
    for type_obj in ifc.by_type("IfcTypeObject"):
        for pset in type_obj.HasPropertySets or ():
            referenced.add(pset.id())

    orphans: list[OrphanPropertySet] = []
    for definition in ifc.by_type("IfcPropertySetDefinition"):
        if definition.id() in referenced:
            continue
        orphans.append(
            OrphanPropertySet(
                step_id=definition.id(),
                guid=definition.GlobalId,
                name=definition.Name,
                entity_type=definition.is_a(),
                content_key=json.dumps(_content(definition), sort_keys=True),
            )
        )
    orphans.sort(key=lambda item: item.step_id)
    return tuple(orphans)


_UNORDERED_AGGREGATES = {"HasProperties", "Quantities", "HasQuantities"}
_IDENTITY_ATTRIBUTES = {"id", "GlobalId", "OwnerHistory"}


def _content(entity) -> dict:
    info = entity.get_info(recursive=False)
    out: dict = {}
    for attr, value in info.items():
        if attr in _IDENTITY_ATTRIBUTES:
            continue
        encoded = _content_value(value)
        if attr in _UNORDERED_AGGREGATES and isinstance(encoded, list):
            encoded = sorted(encoded, key=lambda item: json.dumps(item, sort_keys=True))
        out[attr] = encoded
    return out


def _content_value(value):
    if isinstance(value, tuple):
        return [_content_value(item) for item in value]
    if hasattr(value, "get_info"):
        return _content(value)
    return value


def _collect_direct_properties(ifc, index: dict[int, list[PropertyEntry]]) -> None:
    linked: set[tuple[int, int]] = set()
    for rel in ifc.by_type("IfcRelDefinesByProperties"):
        pset = rel.RelatingPropertyDefinition
        if pset is None:
            continue
        entries = _extract_from_pset_or_qto(pset)
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj) or (step_id, pset.id()) in linked:
                continue
            linked.add((step_id, pset.id()))
            index.setdefault(step_id, []).extend(entries)


def _collect_type_properties(ifc, index: dict[int, list[PropertyEntry]]) -> None:
    for rel in ifc.by_type("IfcRelDefinesByType"):
        type_obj = rel.RelatingType
        if type_obj is None:
            continue
        entries: list[PropertyEntry] = []
        for pset in type_obj.HasPropertySets or ():
            entries.extend(_extract_from_pset_or_qto(pset))
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj):
                continue
            index.setdefault(step_id, []).extend(entries)


def _extract_from_pset_or_qto(definition) -> list[PropertyEntry]:
    if hasattr(definition, "HasProperties"):
        return _extract_from_property_set(definition)
    if hasattr(definition, "Quantities"):
        return _extract_from_quantity_set(definition)
    return []


def _extract_from_property_set(pset) -> list[PropertyEntry]:
    entries: list[PropertyEntry] = []
    for prop in pset.HasProperties or ():
        if prop.is_a("IfcPropertySingleValue"):
            name = _safe_get(prop, "Name")
            if not name:
                continue
            nominal = _safe_get(prop, "NominalValue")
            if nominal is None:
                continue
            value = _safe_get(nominal, "wrappedValue")
            if value is not None:
                entries.append(PropertyEntry(name=str(name), value=value))
        elif prop.is_a("IfcPropertyEnumeratedValue"):
            name = _safe_get(prop, "Name")
            if not name:
                continue
            values = [_safe_get(v, "wrappedValue") for v in (prop.EnumerationValues or ())]
            entries.append(PropertyEntry(name=str(name), value=values))
    return entries


def _extract_from_quantity_set(qto) -> list[PropertyEntry]:
    value_attr = {
        "IfcQuantityLength": "LengthValue",
        "IfcQuantityArea": "AreaValue",
        "IfcQuantityVolume": "VolumeValue",
        "IfcQuantityCount": "CountValue",
        "IfcQuantityWeight": "WeightValue",
        "IfcQuantityTime": "TimeValue",
    }
    entries: list[PropertyEntry] = []
    for qty in qto.Quantities or ():
        name = _safe_get(qty, "Name")
        if not name:
            continue
        attr = value_attr.get(qty.is_a())
        if attr is None:
            continue
        value = _safe_get(qty, attr)
        if value is not None:
            entries.append(PropertyEntry(name=str(name), value=value))
    return entries


def _safe_get(obj, attr: str) -> str | int | float | bool | list | None:
    try:
        return getattr(obj, attr)
    except Exception:
        return None


_SPATIAL_TYPES = {
    "IfcProject",
    "IfcSite",
    "IfcBuilding",
    "IfcBuildingStorey",
    "IfcSpace",
    "IfcSpatialElement",
    "IfcSpatialStructureElement",
    "IfcSpatialZone",
}


def _is_signature_eligible(obj) -> bool:
    try:
        if obj.is_a("IfcProduct"):
            return True
    except Exception:
        return False
    try:
        if any(obj.is_a(t) for t in _SPATIAL_TYPES):
            return True
    except Exception:
        return False
    return False
