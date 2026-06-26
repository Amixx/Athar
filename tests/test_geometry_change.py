"""Geometry-only edit: one moved vertex is a clean geometry change.

The mutation harness covers data, placement, add, and delete edits, but a pure
*geometry* edit had no positive test — yet `geometry: changed` (with data and
placement clean) is a first-class report state. The engine hashes explicit
geometry data (coordinates, points, directions) into `vh_geometry`, not
parametric scalars like an extrusion Depth, so this proves the contract through
the path that actually feeds the hash: an explicit mesh vertex.

Both files are the same tessellated cube at the same GlobalId, name, and
placement, with no properties; they differ only in one vertex coordinate. Truth:
exactly one product, modified, geometry changed, data and placement unchanged.
And because the WL topology self-seed hashes `vh_geometry`, the victim's
topology necessarily ripples too — so this is also the positive proof of the
`mixed` change_scope (intrinsic geometry + transitive topology).
"""

from __future__ import annotations

import ifcopenshell

from athar.engine import diff_files
from tests.corpus import assert_report_invariants

_GUID = "0meshmeshmeshmeshmesh01"

_BASE_COORDS = (
    (-0.5, -0.5, 0.0),
    (0.5, -0.5, 0.0),
    (0.5, 0.5, 0.0),
    (-0.5, 0.5, 0.0),
    (-0.5, -0.5, 1.0),
    (0.5, -0.5, 1.0),
    (0.5, 0.5, 1.0),
    (-0.5, 0.5, 1.0),
)
_CUBE_TRIANGLES = (
    (1, 2, 3), (1, 3, 4),
    (5, 7, 6), (5, 8, 7),
    (1, 2, 6), (1, 6, 5),
    (2, 3, 7), (2, 7, 6),
    (3, 4, 8), (3, 8, 7),
    (4, 1, 5), (4, 5, 8),
)


def _build(path: str, coords) -> None:
    f = ifcopenshell.file(schema="IFC4")
    length_unit = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")
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
        GlobalId="0projectprojectprojec2",
        Name="geom-edit",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )
    faceset = f.create_entity(
        "IfcTriangulatedFaceSet",
        Coordinates=f.create_entity("IfcCartesianPointList3D", CoordList=coords),
        Closed=True,
        CoordIndex=_CUBE_TRIANGLES,
    )
    shape = f.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="Tessellation",
        Items=[faceset],
    )
    placement = f.create_entity(
        "IfcLocalPlacement",
        RelativePlacement=f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(2.0, 3.0, 0.0)),
        ),
    )
    f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_GUID,
        Name="cube",
        ObjectPlacement=placement,
        Representation=f.create_entity("IfcProductDefinitionShape", Representations=[shape]),
    )
    f.write(path)


def test_single_vertex_move_is_a_clean_geometry_change(tmp_path):
    base = str(tmp_path / "cube.ifc")
    moved = str(tmp_path / "cube_moved.ifc")
    _build(base, _BASE_COORDS)
    # Move one vertex 250 mm in X; everything else identical.
    edited = ((-0.25, -0.5, 0.0),) + _BASE_COORDS[1:]
    _build(moved, edited)

    report = diff_files(base, moved)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 1

    item = report["modified"][0]
    assert item["match"]["reason"] == "guid"
    assert item["old"]["guid"] == _GUID
    assert item["aspects"]["geometry"] == "changed"
    assert item["aspects"]["data"] == "unchanged"
    assert item["aspects"]["placement"] == "unchanged"
    # The geometry change feeds the WL self-seed, so topology ripples too.
    assert item["aspects"]["topology"] == "changed"
    assert item["change_scope"] == "mixed"
    # Data untouched: the data hash must be stable through a geometry edit.
    assert item["data_hash"]["old"] == item["data_hash"]["new"]
