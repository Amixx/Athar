"""Property and quantity value extraction from an IFC file.

Captures property values for human-readable delta reports through
ifcopenshell. A measure with a unit type is converted from the file's unit,
or the unit the property names, to SI, which the entry names; a converted
value is rounded to the resolution the native signatures quantize at, so the
report and the hashes agree on what is a change. The entry keeps the value as
written beside it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .parser import measure_unit_type, unit_factor
from .types import OrphanPropertySet, PropertyEntry

# Unit type -> symbol of its SI unit; any other unit type is labelled by its own name.
_SI_SYMBOLS = {
    "LENGTHUNIT": "m",
    "AREAUNIT": "m2",
    "VOLUMEUNIT": "m3",
    "PLANEANGLEUNIT": "rad",
    "TIMEUNIT": "s",
    "MASSUNIT": "kg",
}
# Steps per SI unit of the native quantization of a unit-bearing real (`Measure::scale` in canon.rs).
_NATIVE_RESOLUTION = 1_000_000.0
_QUANTITIES = {
    "IfcQuantityLength": ("LengthValue", "IfcLengthMeasure"),
    "IfcQuantityArea": ("AreaValue", "IfcAreaMeasure"),
    "IfcQuantityVolume": ("VolumeValue", "IfcVolumeMeasure"),
    "IfcQuantityCount": ("CountValue", None),
    "IfcQuantityWeight": ("WeightValue", "IfcMassMeasure"),
    "IfcQuantityTime": ("TimeValue", "IfcTimeMeasure"),
}


@dataclass(frozen=True)
class _Units:
    schema: str
    factors: dict[str, float]


def extract_properties(ifc, unit_factors: dict[str, float]) -> dict[int, list[PropertyEntry]]:
    """Extract property and quantity values keyed by product step_id.

    ``unit_factors`` maps the unit types of the file's unit assignment to
    their SI factors.
    """

    index: dict[int, list[PropertyEntry]] = {}
    units = _Units(ifc.schema_identifier, unit_factors)

    _collect_direct_properties(ifc, index, units)
    _collect_type_properties(ifc, index, units)

    return index


def _si_value(value, measure: str | None, own_unit, units: _Units):
    """``(value in the SI unit of the unit type of ``measure``, that unit's label)``; the value as written and ``None`` without one.

    ``own_unit`` is the unit the property or quantity names itself, which
    takes the place of the file's unit assignment. A unit without a multiplicative
    conversion factor, or an undeclared unit of the measure's type, leaves the
    value as written.
    """
    if measure is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return value, None
    unit_type = measure_unit_type(units.schema, measure)
    if unit_type is None:
        return value, None
    factor = units.factors.get(unit_type) if own_unit is None else unit_factor(own_unit)
    if factor is None:
        return value, None
    if factor != 1:
        value = round(value * factor * _NATIVE_RESOLUTION) / _NATIVE_RESOLUTION
    return value, _SI_SYMBOLS.get(unit_type, unit_type)


def _entry(pset: str, name: str, written, measure, own_unit, units: _Units, inherited: bool, pset_guid) -> PropertyEntry:
    """``measure`` is the measure of ``written``, or a list of the measure of each value when ``written`` is a list."""
    if isinstance(written, list):
        converted = [_si_value(item, item_measure, own_unit, units) for item, item_measure in zip(written, measure)]
        labels = {unit for _, unit in converted}
        if len(labels) == 1:
            value, unit = [item for item, _ in converted], labels.pop()
        else:
            value, unit = written, None
    else:
        value, unit = _si_value(written, measure, own_unit, units)
    return PropertyEntry(
        pset=pset,
        name=name,
        value=value,
        inherited=inherited,
        pset_guid=pset_guid,
        unit=unit,
        written=None if value == written else written,
    )


_VALUE_ATTRIBUTES = {
    "IfcPropertySingleValue": ("NominalValue",),
    "IfcPropertyBoundedValue": ("UpperBoundValue", "LowerBoundValue", "SetPointValue"),
    "IfcPropertyListValue": ("ListValues",),
    "IfcPropertyEnumeratedValue": ("EnumerationValues",),
    "IfcPropertyEnumeration": ("EnumerationValues",),
}


def _own_units(entity) -> dict[str, object]:
    """The unit a property, enumeration, quantity or measure with unit names, by the attribute whose values it applies to."""
    cls = entity.is_a()
    if cls == "IfcMeasureWithUnit":
        return {"ValueComponent": entity.UnitComponent}
    if cls == "IfcPropertyTableValue":
        return {"DefiningValues": entity.DefiningUnit, "DefinedValues": entity.DefinedUnit}
    if cls == "IfcPropertyEnumeratedValue":
        reference = entity.EnumerationReference
        return {"EnumerationValues": reference.Unit if reference is not None else None}
    if cls in _QUANTITIES:
        return {_QUANTITIES[cls][0]: entity.Unit}
    declared = set(entity.get_attribute_names())
    return {attr: entity.Unit for attr in _VALUE_ATTRIBUTES.get(cls, ()) if attr in declared}


def own_unit_factors(ifc) -> dict[int, dict[str, dict[str, float]]]:
    """Per property, enumeration, quantity or measure with unit naming its own unit: the factor of that unit by the
    attribute holding the values and the unit type of those values.

    The unit takes the place of the file's unit assignment for those values; a
    unit without a multiplicative factor leaves them as written (factor 1). An
    enumerated value takes the unit of the enumeration it references, and a
    table's defining and defined values each take their own unit.
    """
    schema = ifc.schema_identifier
    factors: dict[int, dict[str, dict[str, float]]] = {}
    records = (
        ifc.by_type("IfcSimpleProperty")
        + ifc.by_type("IfcPropertyEnumeration")
        + ifc.by_type("IfcPhysicalSimpleQuantity")
        + ifc.by_type("IfcMeasureWithUnit")
    )
    for entity in records:
        for attr, own_unit in _own_units(entity).items():
            if own_unit is None:
                continue
            if entity.is_a() in _QUANTITIES:
                measures = [_QUANTITIES[entity.is_a()][1]]
            else:
                value = getattr(entity, attr)
                measures = [item.is_a() for item in (value if isinstance(value, tuple) else (value,)) if item is not None]
            unit_types = {measure_unit_type(schema, measure) for measure in measures if measure} - {None}
            if unit_types:
                factor = unit_factor(own_unit)
                factors.setdefault(entity.id(), {})[attr] = {
                    unit_type: 1.0 if factor is None else factor for unit_type in unit_types
                }
    return factors


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


def _collect_direct_properties(ifc, index: dict[int, list[PropertyEntry]], units: _Units) -> None:
    linked: set[tuple[int, int]] = set()
    for rel in ifc.by_type("IfcRelDefinesByProperties"):
        pset = rel.RelatingPropertyDefinition
        if pset is None:
            continue
        entries = _extract_from_pset_or_qto(pset, units, inherited=False)
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj) or (step_id, pset.id()) in linked:
                continue
            linked.add((step_id, pset.id()))
            index.setdefault(step_id, []).extend(entries)


def _collect_type_properties(ifc, index: dict[int, list[PropertyEntry]], units: _Units) -> None:
    for rel in ifc.by_type("IfcRelDefinesByType"):
        type_obj = rel.RelatingType
        if type_obj is None:
            continue
        entries: list[PropertyEntry] = []
        for pset in type_obj.HasPropertySets or ():
            entries.extend(_extract_from_pset_or_qto(pset, units, inherited=True))
        if not entries:
            continue
        for obj in rel.RelatedObjects or ():
            step_id = obj.id()
            if not _is_signature_eligible(obj):
                continue
            index.setdefault(step_id, []).extend(entries)


def _extract_from_pset_or_qto(definition, units: _Units, *, inherited: bool) -> list[PropertyEntry]:
    if hasattr(definition, "HasProperties"):
        return _extract_from_property_set(definition, units, inherited)
    if hasattr(definition, "Quantities"):
        return _extract_from_quantity_set(definition, units, inherited)
    return []


def _extract_from_property_set(pset, units: _Units, inherited: bool) -> list[PropertyEntry]:
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
                entries.append(
                    _entry(pset_name, str(name), value, nominal.is_a(), prop.Unit, units, inherited, pset_guid)
                )
        elif prop.is_a("IfcPropertyEnumeratedValue"):
            name = _safe_get(prop, "Name")
            if not name:
                continue
            selected = prop.EnumerationValues or ()
            values = [_safe_get(v, "wrappedValue") for v in selected]
            measures = [v.is_a() for v in selected]
            own_unit = _own_units(prop)["EnumerationValues"]
            entries.append(_entry(pset_name, str(name), values, measures, own_unit, units, inherited, pset_guid))
    return entries


def _extract_from_quantity_set(qto, units: _Units, inherited: bool) -> list[PropertyEntry]:
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
            entries.append(_entry(qto_name, str(name), value, measure, qty.Unit, units, inherited, pset_guid))
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
