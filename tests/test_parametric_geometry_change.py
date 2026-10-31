"""Parametric shape edits are geometry changes.

Each variant edits one value inside a product's shape subgraph that is not a
point coordinate or direction: an extrusion Depth, a profile dimension, a
profile or solid Position, a mapped item's transformation Scale. Truth: exactly
one product modified, geometry changed, placement unchanged. Moving the
product's ObjectPlacement stays a placement-only change.
"""

from __future__ import annotations

import ifcopenshell
import pytest

from athar.engine import diff_files
from tests.corpus import assert_report_invariants


def _a2p3d(f, xyz=(0.0, 0.0, 0.0)):
    return f.createIfcAxis2Placement3D(f.createIfcCartesianPoint(xyz), None, None)


def _build(path: str, edit=None) -> None:
    f = ifcopenshell.file(schema="IFC4")
    units = f.createIfcUnitAssignment([f.createIfcSIUnit(None, "LENGTHUNIT", None, "METRE")])
    context = f.createIfcGeometricRepresentationContext(None, "Model", 3, 1e-5, _a2p3d(f), None)
    f.createIfcProject("0projectprojectprojec3", None, "P", None, None, None, None, [context], units)

    profile = f.createIfcRectangleProfileDef(
        "AREA", None, f.createIfcAxis2Placement2D(f.createIfcCartesianPoint((0.0, 0.0)), None), 0.04, 2.0
    )
    solid = f.createIfcExtrudedAreaSolid(profile, _a2p3d(f), f.createIfcDirection((0.0, 0.0, 1.0)), 3.0)
    body = f.createIfcShapeRepresentation(context, "Body", "SweptSolid", [solid])
    f.createIfcBuildingElementProxy(
        "0extrusionextrusionex1", None, "Extruded", None, None,
        f.createIfcLocalPlacement(None, _a2p3d(f)),
        f.createIfcProductDefinitionShape(None, None, [body]),
    )

    block = f.createIfcExtrudedAreaSolid(
        f.createIfcRectangleProfileDef("AREA", None, None, 1.0, 1.0),
        _a2p3d(f), f.createIfcDirection((0.0, 0.0, 1.0)), 1.0,
    )
    rep_map = f.createIfcRepresentationMap(
        _a2p3d(f), f.createIfcShapeRepresentation(context, "Body", "SweptSolid", [block])
    )
    operator = f.createIfcCartesianTransformationOperator3D(
        None, None, f.createIfcCartesianPoint((0.0, 0.0, 0.0)), 1.0, None
    )
    mapped = f.createIfcShapeRepresentation(context, "Body", "MappedRepresentation", [f.createIfcMappedItem(rep_map, operator)])
    f.createIfcBuildingElementProxy(
        "0mappedmappedmappedma1", None, "Mapped", None, None,
        f.createIfcLocalPlacement(None, _a2p3d(f, (5.0, 0.0, 0.0))),
        f.createIfcProductDefinitionShape(None, None, [mapped]),
    )
    if edit is not None:
        edit(f, solid, operator)
    f.write(path)


def _depth(f, solid, operator):
    solid.Depth = 3.5


def _symmetric_thicken(f, solid, operator):
    solid.SweptArea.XDim = 0.06


def _profile_position(f, solid, operator):
    solid.SweptArea.Position.Location.Coordinates = (0.01, 0.0)


def _solid_position(f, solid, operator):
    solid.Position.Location.Coordinates = (0.0, 0.0, 0.5)


def _mapped_scale(f, solid, operator):
    operator.Scale = 2.0


@pytest.mark.parametrize(
    ("edit", "victim"),
    [
        (_depth, "Extruded"),
        (_symmetric_thicken, "Extruded"),
        (_profile_position, "Extruded"),
        (_solid_position, "Extruded"),
        (_mapped_scale, "Mapped"),
    ],
)
def test_parametric_edit_is_one_geometry_change(tmp_path, edit, victim):
    old, new = str(tmp_path / "old.ifc"), str(tmp_path / "new.ifc")
    _build(old)
    _build(new, edit)
    report = diff_files(old, new)
    assert_report_invariants(report)
    assert [item["new"]["name"] for item in report["modified"]] == [victim]
    aspects = report["modified"][0]["aspects"]
    assert aspects["geometry"] == "changed"
    assert aspects["placement"] == "unchanged"
    assert aspects["data"] == "unchanged"


def test_object_placement_move_is_not_a_geometry_change(tmp_path):
    def move(f, solid, operator):
        proxy = next(p for p in f.by_type("IfcBuildingElementProxy") if p.Name == "Extruded")
        proxy.ObjectPlacement.RelativePlacement.Location.Coordinates = (0.0, 0.0, 1.0)

    old, new = str(tmp_path / "old.ifc"), str(tmp_path / "new.ifc")
    _build(old)
    _build(new, move)
    report = diff_files(old, new)
    assert [item["new"]["name"] for item in report["modified"]] == ["Extruded"]
    aspects = report["modified"][0]["aspects"]
    assert aspects["geometry"] == "unchanged"
    assert aspects["placement"] == "changed"
