"""Property and quantity value extraction from an IFC file.

Captures property values for human-readable delta reports through
ifcopenshell, without quantization. Measures with an SI unit are converted
from the file's unit to that unit, which the entry names.
"""

from __future__ import annotations

import json

from .parser import unit_factor
from .types import OrphanPropertySet, PropertyEntry

# Measure class -> (unit type of the unit assignment, SI unit, factors of
# that unit type per SI unit).
_SI_UNITS = {
    "IfcLengthMeasure": ("LENGTHUNIT", "m", 1.0),
    "IfcPositiveLengthMeasure": ("LENGTHUNIT", "m", 1.0),
    "IfcNonNegativeLengthMeasure": ("LENGTHUNIT", "m", 1.0),
    "IfcAreaMeasure": ("AREAUNIT", "m2", 1.0),
    "IfcVolumeMeasure": ("VOLUMEUNIT", "m3", 1.0),
    "IfcPlaneAngleMeasure": ("PLANEANGLEUNIT", "rad", 1.0),
    "IfcPositivePlaneAngleMeasure": ("PLANEANGLEUNIT", "rad", 1.0),
    "IfcTimeMeasure": ("TIMEUNIT", "s", 1.0),
    # Unit factors of mass are relative to the gram.
    "IfcMassMeasure": ("MASSUNIT", "kg", 1000.0),
}
_QUANTITIES = {
    "IfcQuantityLength": ("LengthValue", "IfcLengthMeasure"),
    "IfcQuantityArea": ("AreaValue", "IfcAreaMeasure"),
    "IfcQuantityVolume": ("VolumeValue", "IfcVolumeMeasure"),
    "IfcQuantityCount": ("CountValue", None),
    "IfcQuantityWeight": ("WeightValue", "IfcMassMeasure"),
    "IfcQuantityTime": ("TimeValue", "IfcTimeMeasure"),
}


def extract_properties(ifc, unit_factors: dict[str, float]) -> dict[int, list[PropertyEntry]]:
    """Extract property and quantity values keyed by product step_id.

    ``unit_factors`` maps the unit types of the file's unit assignment to
    their SI factors.
    """

    index: dict[int, list[PropertyEntry]] = {}

    _collect_direct_properties(ifc, index, unit_factors)
    _collect_type_properties(ifc, index, unit_factors)

    return index


def _si_value(value, measure: str | None, own_unit, unit_factors: dict[str, float]):
    """``(value in the SI unit of ``measure``, that unit)``; the value as written and ``None`` without one.

    ``own_unit`` is the unit the property or quantity names itself, which
    takes the place of the file's unit assignment. Only an ``IfcSIUnit`` or
    an ``IfcConversionBasedUnit`` has a factor; with any other own unit, or
    when the file declares no unit of the measure's type, the value stays as
    written.
    """
    if measure not in _SI_UNITS or isinstance(value, bool) or not isinstance(value, (int, float)):
        return value, None
    unit_type, unit, per_si = _SI_UNITS[measure]
    if own_unit is None:
        factor = unit_factors.get(unit_type)
        if factor is None:
            return value, None
    elif own_unit.is_a("IfcSIUnit") or own_unit.is_a("IfcConversionBasedUnit"):
        factor = unit_factor(own_unit)
    else:
        return value, None
    if factor == per_si:
        return value, unit
    return float(f"{value * factor / per_si:.12g}"), unit


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


def _collect_direct_properties(ifc, index: dict[int, list[PropertyEntry]], unit_factors: dict[str, float]) -> None:
    linked: set[tuple[int, int]] = set()
    for rel in ifc.by_type("IfcRelDefinesByProperties"):
        pset = rel.RelatingPropertyDefinition
        if pset is None:
            continue
        entries = _extract_from_pset_or_qto(pset, unit_factors, inherited=False)
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj) or (step_id, pset.id()) in linked:
                continue
            linked.add((step_id, pset.id()))
            index.setdefault(step_id, []).extend(entries)


def _collect_type_properties(ifc, index: dict[int, list[PropertyEntry]], unit_factors: dict[str, float]) -> None:
    for rel in ifc.by_type("IfcRelDefinesByType"):
        type_obj = rel.RelatingType
        if type_obj is None:
            continue
        entries: list[PropertyEntry] = []
        for pset in type_obj.HasPropertySets or ():
            entries.extend(_extract_from_pset_or_qto(pset, unit_factors, inherited=True))
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj):
                continue
            index.setdefault(step_id, []).extend(entries)


def _extract_from_pset_or_qto(definition, unit_factors: dict[str, float], *, inherited: bool) -> list[PropertyEntry]:
    if hasattr(definition, "HasProperties"):
        return _extract_from_property_set(definition, unit_factors, inherited)
    if hasattr(definition, "Quantities"):
        return _extract_from_quantity_set(definition, unit_factors, inherited)
    return []


def _extract_from_property_set(pset, unit_factors: dict[str, float], inherited: bool) -> list[PropertyEntry]:
    pset_name = str(_safe_get(pset, "Name") or "")
    pset_guid = _safe_get(pset, "GlobalId")
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
                value, unit = _si_value(value, nominal.is_a(), _safe_get(prop, "Unit"), unit_factors)
                entries.append(
                    PropertyEntry(
                        pset=pset_name, name=str(name), value=value, inherited=inherited, pset_guid=pset_guid, unit=unit
                    )
                )
        elif prop.is_a("IfcPropertyEnumeratedValue"):
            name = _safe_get(prop, "Name")
            if not name:
                continue
            values = [_safe_get(v, "wrappedValue") for v in (prop.EnumerationValues or ())]
            entries.append(PropertyEntry(pset=pset_name, name=str(name), value=values, inherited=inherited, pset_guid=pset_guid))
    return entries


def _extract_from_quantity_set(qto, unit_factors: dict[str, float], inherited: bool) -> list[PropertyEntry]:
    qto_name = str(_safe_get(qto, "Name") or "")
    pset_guid = _safe_get(qto, "GlobalId")
    entries: list[PropertyEntry] = []
    for qty in qto.Quantities or ():
        name = _safe_get(qty, "Name")
        if not name:
            continue
        if qty.is_a() not in _QUANTITIES:
            continue
        attr, measure = _QUANTITIES[qty.is_a()]
        value = _safe_get(qty, attr)
        if value is not None:
            value, unit = _si_value(value, measure, _safe_get(qty, "Unit"), unit_factors)
            entries.append(
                PropertyEntry(
                    pset=qto_name, name=str(name), value=value, inherited=inherited, pset_guid=pset_guid, unit=unit
                )
            )
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
