"""Unit-normalization metamorphic invariant: same building, different units.

The engine's headline claim is that lengths are canonicalized *through unit
handling* — two files describing the identical physical model must produce
identical signatures even when one is authored in metres and the other in
millimetres. This builds the same 1x1x1 m cube twice: once in METRE with
metre-valued literals, once in MILLIMETRE with every length scaled by 1000.
Same GlobalId, same placement, plus a length-valued property so both the
geometry and the data length paths are exercised. Truth: a perfect unit
conversion is not a change, so the diff must be empty.

This is a synthetic pair on purpose: a controlled two-unit authoring is a
correct, unambiguous metamorphic transform, where rescaling a real seed in
place would risk an incomplete conversion and a misleading test.
"""

from __future__ import annotations

import ifcopenshell
import pytest

from athar.engine import diff_files
from tests.corpus import assert_report_invariants, assert_zero_diff

_GUID = "0cubecubecubecubecube1"


def _build(path: str, scale: float, prefix: str | None) -> None:
    f = ifcopenshell.file(schema="IFC4")
    # Millimetres are METRE with an SI MILLI prefix; metres carry no prefix.
    length_unit = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Prefix=prefix, Name="METRE")
    units = f.create_entity("IfcUnitAssignment", Units=[length_unit])
    origin = f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0))
    context = f.create_entity(
        "IfcGeometricRepresentationContext",
        ContextType="Model",
        CoordinateSpaceDimension=3,
        Precision=1e-5,
        WorldCoordinateSystem=f.create_entity("IfcAxis2Placement3D", Location=origin),
    )
    f.create_entity(
        "IfcProject",
        GlobalId="0projectprojectprojec1",
        Name="unit-norm",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )

    # 1x1x1 m cube as an extrusion; every length literal scaled into `unit_name`.
    profile = f.create_entity(
        "IfcRectangleProfileDef",
        ProfileType="AREA",
        Position=f.create_entity(
            "IfcAxis2Placement2D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0)),
        ),
        XDim=1.0 * scale,
        YDim=1.0 * scale,
    )
    solid = f.create_entity(
        "IfcExtrudedAreaSolid",
        SweptArea=profile,
        Position=f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
        ),
        ExtrudedDirection=f.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)),
        Depth=1.0 * scale,
    )
    shape = f.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="SweptSolid",
        Items=[solid],
    )
    placement = f.create_entity(
        "IfcLocalPlacement",
        RelativePlacement=f.create_entity(
            "IfcAxis2Placement3D",
            # A non-trivial world position, in this file's length unit.
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(2.0 * scale, 3.0 * scale, 0.0)),
        ),
    )
    proxy = f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_GUID,
        Name="cube",
        ObjectPlacement=placement,
        Representation=f.create_entity("IfcProductDefinitionShape", Representations=[shape]),
    )

    # A length-valued property exercises the data-path length quantization too.
    pset = f.create_entity(
        "IfcPropertySet",
        GlobalId="0psetpsetpsetpsetpset1",
        Name="Pset_Test",
        HasProperties=[
            f.create_entity(
                "IfcPropertySingleValue",
                Name="NominalLength",
                NominalValue=f.create_entity("IfcLengthMeasure", 3.0 * scale),
            )
        ],
    )
    f.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId="0relrelrelrelrelrelre1",
        RelatedObjects=[proxy],
        RelatingPropertyDefinition=pset,
    )

    f.write(path)


def test_metre_and_millimetre_models_differ_only_in_the_project_units(tmp_path):
    metre = str(tmp_path / "cube_m.ifc")
    millimetre = str(tmp_path / "cube_mm.ifc")
    _build(metre, scale=1.0, prefix=None)
    _build(millimetre, scale=1000.0, prefix="MILLI")

    report = diff_files(metre, millimetre)
    assert_report_invariants(report)
    # The unit assignment itself is project content; nothing else changed.
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert [(item["new"]["class"], item["change_scope"]) for item in report["modified"]] == [
        ("IfcProject", "intrinsic")
    ]
    assert report["modified"][0]["aspects"]["data"] == "changed"
    assert report["stats"]["old_signatures"] == 2

    # The one product is matched by GlobalId with every aspect unchanged:
    # geometry and data length values both normalize across the unit switch.
    (item,) = report["unchanged"]
    assert item["match"]["reason"] == "guid"
    assert item["old"]["guid"] == _GUID
    for aspect in ("geometry", "data", "topology", "placement"):
        assert item["aspects"][aspect] == "unchanged", item


@pytest.mark.parametrize(("kind", "expected", "label"), [
    ("prefixed", (0.003, 0.006), "m"),
    ("foot", (0.9144, 1.8288), "m"),
    ("derived", (0.003, 0.006), "m"),
    ("context", (3.0, 6.0), None),
    ("conversion_context", (3.0, 6.0), None),
])
def test_property_unit_conversion_through_diff(tmp_path, kind, expected, label):
    old, new = tmp_path / "old.ifc", tmp_path / "new.ifc"
    _build(str(old), 1.0, None)
    model = ifcopenshell.open(str(old))
    metre = model.by_type("IfcSIUnit")[0]
    if kind == "prefixed":
        unit = model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Prefix="MILLI", Name="METRE")
    elif kind == "foot":
        dimensions = model.create_entity("IfcDimensionalExponents", 1, 0, 0, 0, 0, 0, 0)
        conversion = model.create_entity("IfcMeasureWithUnit", model.create_entity("IfcLengthMeasure", 0.3048), metre)
        unit = model.create_entity("IfcConversionBasedUnit", dimensions, "LENGTHUNIT", "foot", conversion)
    elif kind == "derived":
        millimetre = model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Prefix="MILLI", Name="METRE")
        element = model.create_entity("IfcDerivedUnitElement", millimetre, 1)
        unit = model.create_entity("IfcDerivedUnit", [element], "USERDEFINED", "length")
    else:
        dimensions = model.create_entity("IfcDimensionalExponents", 1, 0, 0, 0, 0, 0, 0)
        unit = model.create_entity("IfcContextDependentUnit", dimensions, "LENGTHUNIT", "context length")
        if kind == "conversion_context":
            conversion = model.create_entity("IfcMeasureWithUnit", model.create_entity("IfcLengthMeasure", 2.0), unit)
            unit = model.create_entity("IfcConversionBasedUnit", dimensions, "LENGTHUNIT", "context conversion", conversion)
    model.by_type("IfcUnitAssignment")[0].Units = [unit]
    prop = model.by_type("IfcPropertySingleValue")[0]
    prop.Unit = unit if kind in ("derived", "conversion_context") else None
    model.write(str(old))
    prop.NominalValue.wrappedValue = 6.0
    model.write(str(new))
    report = diff_files(str(old), str(new))
    assert_report_invariants(report)
    (item,) = report["modified"]
    (delta,) = item["property_deltas"]["changed"]
    assert delta["old_value"] == expected[0]
    assert delta["new_value"] == expected[1]
    assert delta.get("unit") == label


def _derived(model, unit_type, *elements):
    return model.create_entity("IfcDerivedUnit", UnitType=unit_type, Elements=[
        model.create_entity("IfcDerivedUnitElement", Unit=unit, Exponent=exponent) for unit, exponent in elements
    ])


def _with_measures(path, units, values, quantities=()):
    _build(str(path), 1.0, None)
    model = ifcopenshell.open(str(path))
    assignment = model.by_type("IfcUnitAssignment")[0]
    assignment.Units = list(assignment.Units) + [build(model) for build in units]
    proxy = model.by_type("IfcBuildingElementProxy")[0]
    definitions = [model.create_entity("IfcPropertySet", GlobalId="0measuresmeasuresmeas1", Name="Pset_Measures", HasProperties=[
        model.create_entity("IfcPropertySingleValue", Name=name, NominalValue=model.create_entity(measure, value),
                            Unit=unit(model) if unit else None)
        for name, measure, value, unit in values
    ])]
    if quantities:
        definitions.append(model.create_entity("IfcElementQuantity", GlobalId="0quantitiesquantitiesq1", Name="Qto_Measures", Quantities=[
            model.create_entity(cls, Name=name, **{attr: value}) for cls, name, attr, value in quantities
        ]))
    for index, definition in enumerate(definitions):
        model.create_entity("IfcRelDefinesByProperties", GlobalId=f"0measurerelmeasurerel{index:02d}",
                            RelatedObjects=[proxy], RelatingPropertyDefinition=definition)
    model.write(str(path))


def _thermal_units(prefix):
    def build(model):
        watt = model.create_entity("IfcSIUnit", UnitType="POWERUNIT", Name="WATT")
        metre = model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Prefix=prefix, Name="METRE")
        kelvin = model.create_entity("IfcSIUnit", UnitType="THERMODYNAMICTEMPERATUREUNIT", Name="KELVIN")
        return _derived(model, "THERMALTRANSMITTANCEUNIT", (watt, 1), (metre, -2), (kelvin, -1))
    return build


def _mass_unit(tonnes):
    def build(model):
        kilogram = model.create_entity("IfcSIUnit", UnitType="MASSUNIT", Prefix="KILO", Name="GRAM")
        if not tonnes:
            return kilogram
        dimensions = model.create_entity("IfcDimensionalExponents", 0, 1, 0, 0, 0, 0, 0)
        conversion = model.create_entity("IfcMeasureWithUnit", model.create_entity("IfcMassMeasure", 1000.0), kilogram)
        return model.create_entity("IfcConversionBasedUnit", dimensions, "MASSUNIT", "tonne", conversion)
    return build


def test_measures_of_any_unit_type_normalize_across_unit_declarations(tmp_path):
    si, scaled = tmp_path / "si.ifc", tmp_path / "scaled.ifc"
    _with_measures(si, [_thermal_units(None), _mass_unit(False)],
                   [("ThermalTransmittance", "IfcThermalTransmittanceMeasure", 0.25, None)],
                   [("IfcQuantityWeight", "GrossWeight", "WeightValue", 1200.0)])
    _with_measures(scaled, [_thermal_units("MILLI"), _mass_unit(True)],
                   [("ThermalTransmittance", "IfcThermalTransmittanceMeasure", 0.25e-6, None)],
                   [("IfcQuantityWeight", "GrossWeight", "WeightValue", 1.2)])

    report = diff_files(str(si), str(scaled))
    assert_report_invariants(report)
    assert [item["new"]["class"] for item in report["modified"]] == ["IfcProject"]
    (item,) = report["unchanged"]
    assert item["old"]["guid"] == _GUID


def test_a_property_unit_takes_the_place_of_the_file_unit(tmp_path):
    from athar.bottom.signatures import build_signature_bundle

    def nominal(path, value, unit):
        _build(str(path), 1000.0, "MILLI")
        model = ifcopenshell.open(str(path))
        prop = model.by_type("IfcPropertySingleValue")[0]
        prop.NominalValue.wrappedValue = value
        prop.Unit = model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE") if unit else None
        model.write(str(path))
        (signature,) = [s for s in build_signature_bundle(str(path)).signatures.values() if s.guid == _GUID]
        return [value for path, value in signature.data_facts if value.startswith("IFCLENGTHMEASURE")]

    assert nominal(tmp_path / "own.ifc", 3.0, unit=True) == nominal(tmp_path / "file.ifc", 3000.0, unit=False) == ["IFCLENGTHMEASURE: 3 m"]


def _year_unit(model):
    second = model.create_entity("IfcSIUnit", UnitType="TIMEUNIT", Name="SECOND")
    dimensions = model.create_entity("IfcDimensionalExponents", 0, 0, 1, 0, 0, 0, 0)
    conversion = model.create_entity("IfcMeasureWithUnit", model.create_entity("IfcTimeMeasure", 31556926.0), second)
    return model.create_entity("IfcConversionBasedUnit", dimensions, "TIMEUNIT", "year", conversion)


def test_a_measure_with_unit_takes_its_value_in_its_own_unit(tmp_path):
    from athar.bottom.signatures import build_signature_bundle

    path = tmp_path / "conversions.ifc"
    _with_measures(path, [_mass_unit(True), _year_unit], [])
    facts = {value for signature in build_signature_bundle(str(path)).signatures.values() for _, value in signature.data_facts}
    assert {"IFCMASSMEASURE: 1000 kg", "IFCTIMEMEASURE: 31556926 s"} <= facts


def _product_facts(path):
    from athar.bottom.signatures import build_signature_bundle

    (signature,) = [s for s in build_signature_bundle(str(path)).signatures.values() if s.guid == _GUID]
    return signature.data_facts


def _millimetre_model_with(path, build_properties):
    _build(str(path), 1000.0, "MILLI")
    model = ifcopenshell.open(str(path))
    pset = model.by_type("IfcPropertySet")[0]
    pset.HasProperties = list(pset.HasProperties) + build_properties(model)
    model.write(str(path))
    return _product_facts(path)


def _metre(model):
    return model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")


def test_an_enumeration_unit_applies_to_the_selected_and_enumeration_values(tmp_path):
    def properties(model):
        lengths = [model.create_entity("IfcLengthMeasure", value) for value in (3.0, 6.0)]
        enumeration = model.create_entity("IfcPropertyEnumeration", Name="Widths", EnumerationValues=lengths, Unit=_metre(model))
        return [model.create_entity("IfcPropertyEnumeratedValue", Name="Width", EnumerationReference=enumeration,
                                    EnumerationValues=[model.create_entity("IfcLengthMeasure", 3.0)])]

    facts = _millimetre_model_with(tmp_path / "enumerated.ifc", properties)
    assert sorted((path.rsplit("/", 1)[-1], value) for path, value in facts if path.endswith("EnumerationValues")) == [
        ("IfcPropertyEnumeratedValue.EnumerationValues", "[IFCLENGTHMEASURE: 3 m]"),
        ("IfcPropertyEnumeration.EnumerationValues", "[IFCLENGTHMEASURE: 3 m, IFCLENGTHMEASURE: 6 m]"),
    ]
