"""IfcMapConversion lengths are in the target CRS's map unit, not the project's."""

from __future__ import annotations

import ifcopenshell
import pytest

from athar.bottom.signatures import build_signature_bundle


def _build(path: str, map_unit: dict | None, schema: str = "IFC4") -> None:
    f = ifcopenshell.file(schema=schema)
    millimetre = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Prefix="MILLI", Name="METRE")
    units = f.create_entity("IfcUnitAssignment", Units=[millimetre])
    context = f.create_entity(
        "IfcGeometricRepresentationContext",
        ContextType="Model",
        CoordinateSpaceDimension=3,
        Precision=1e-5,
        WorldCoordinateSystem=f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
        ),
    )
    f.create_entity(
        "IfcProject",
        GlobalId="0projectprojectprojec1",
        Name="map-unit",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )
    if schema != "IFC2X3":
        crs = f.create_entity(
            "IfcProjectedCRS",
            Name="EPSG:25832",
            MapUnit=None if map_unit is None else f.create_entity("IfcSIUnit", **map_unit),
        )
        f.create_entity(
            "IfcMapConversion",
            SourceCRS=context,
            TargetCRS=crs,
            Eastings=659068.0,
            Northings=5299451.0,
            OrthogonalHeight=56.0,
        )
    f.write(path)


def _eastings(path: str) -> str:
    facts = [
        value
        for sig in build_signature_bundle(path).signatures.values()
        for fact_path, value in sig.data_facts
        if fact_path.endswith("IfcMapConversion.Eastings")
    ]
    assert len(facts) == 1, facts
    return facts[0]


def test_map_conversion_lengths_use_the_target_crs_map_unit(tmp_path):
    path = str(tmp_path / "metre_map_unit.ifc")
    _build(path, {"UnitType": "LENGTHUNIT", "Name": "METRE"})
    assert _eastings(path) == "659068 m"


def test_map_conversion_lengths_use_the_project_unit_without_a_map_unit(tmp_path):
    path = str(tmp_path / "no_map_unit.ifc")
    _build(path, None)
    assert _eastings(path) == "659.068 m"


def test_map_unit_that_is_not_a_length_unit_is_an_error(tmp_path):
    path = str(tmp_path / "area_map_unit.ifc")
    _build(path, {"UnitType": "AREAUNIT", "Name": "SQUARE_METRE"})
    with pytest.raises(ValueError, match=r"#\d+ IfcMapConversion"):
        build_signature_bundle(path)


def test_ifc2x3_file_without_map_conversion_builds(tmp_path):
    path = str(tmp_path / "ifc2x3.ifc")
    _build(path, None, schema="IFC2X3")
    assert build_signature_bundle(path).signatures
