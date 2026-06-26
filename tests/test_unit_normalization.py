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


def test_metre_and_millimetre_models_are_semantically_identical(tmp_path):
    metre = str(tmp_path / "cube_m.ifc")
    millimetre = str(tmp_path / "cube_mm.ifc")
    _build(metre, scale=1.0, prefix=None)
    _build(millimetre, scale=1000.0, prefix="MILLI")

    report = diff_files(metre, millimetre)
    assert_report_invariants(report)
    # A perfect unit conversion changed nothing semantically.
    assert_zero_diff(report)
    assert report["stats"]["old_signatures"] == 1

    # And the one product is matched by GlobalId with every aspect unchanged —
    # geometry and data length values both normalized across the unit switch.
    item = report["unchanged"][0]
    assert item["match"]["reason"] == "guid"
    assert item["old"]["guid"] == _GUID
    for aspect in ("geometry", "data", "topology", "placement"):
        assert item["aspects"][aspect] == "unchanged", item
