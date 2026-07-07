"""Canon-v6 tessellated-mesh contracts.

Revit re-serializes `IfcTriangulatedFaceSet` meshes with permuted
`CoordList`/`CoordIndex` order while the coordinate values stay byte-identical
(2026-07-06 round-trip corpus, FINDINGS.md). Vertex order is only reachable
through the index lists and the triangle list is semantically a set, so both
orderings are serialization noise, not geometry. The engine therefore hashes
triangulated meshes as order-canonical expanded triangles: permutations must
produce a zero diff, while a reversed winding (flipped normal) and any real
coordinate change stay visible.

The same corpus scoping exposed the converse gap for `IfcPolygonalFaceSet`:
its `Faces` references never entered the geometry Merkle, so editing a face's
`CoordIndex` was invisible. That edge now exists, and face-list order stays
neutral through the Merkle's sorted child hashing.
"""

from __future__ import annotations

import ifcopenshell

from athar.engine import diff_files
from tests.corpus import assert_report_invariants, assert_zero_diff

_GUID = "0meshcanonmeshcanon001"

_CUBE_COORDS = (
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
_QUAD_FACES = (
    (1, 2, 3, 4),
    (5, 8, 7, 6),
    (1, 2, 6, 5),
    (2, 3, 7, 6),
    (3, 4, 8, 7),
    (4, 1, 5, 8),
)


def _base_file():
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
        GlobalId="0projectprojectprojec4",
        Name="mesh-canon",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )
    return f, context


def _finish(f, context, item, path: str) -> None:
    shape = f.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType="Tessellation",
        Items=[item],
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


def _build_triangulated(path: str, coords, triangles) -> None:
    f, context = _base_file()
    faceset = f.create_entity(
        "IfcTriangulatedFaceSet",
        Coordinates=f.create_entity("IfcCartesianPointList3D", CoordList=coords),
        Closed=True,
        CoordIndex=triangles,
    )
    _finish(f, context, faceset, path)


def _build_polygonal(path: str, coords, faces) -> None:
    f, context = _base_file()
    faceset = f.create_entity(
        "IfcPolygonalFaceSet",
        Coordinates=f.create_entity("IfcCartesianPointList3D", CoordList=coords),
        Closed=True,
        Faces=[f.create_entity("IfcIndexedPolygonalFace", CoordIndex=face) for face in faces],
    )
    _finish(f, context, faceset, path)


def _permuted_cube():
    order = [3, 7, 1, 5, 8, 2, 6, 4]
    coords = tuple(_CUBE_COORDS[v - 1] for v in order)
    remap = {old: new + 1 for new, old in enumerate(order)}
    triangles = tuple((remap[b], remap[c], remap[a]) for a, b, c in _CUBE_TRIANGLES)
    triangles = tuple(reversed(triangles))
    return coords, triangles


def test_permuted_mesh_serialization_is_zero_diff(tmp_path):
    base = str(tmp_path / "cube.ifc")
    permuted = str(tmp_path / "cube_permuted.ifc")
    _build_triangulated(base, _CUBE_COORDS, _CUBE_TRIANGLES)
    coords, triangles = _permuted_cube()
    _build_triangulated(permuted, coords, triangles)

    report = diff_files(base, permuted)
    assert_report_invariants(report)
    assert_zero_diff(report)


def test_reversed_winding_is_a_geometry_change(tmp_path):
    base = str(tmp_path / "cube.ifc")
    flipped = str(tmp_path / "cube_flipped.ifc")
    _build_triangulated(base, _CUBE_COORDS, _CUBE_TRIANGLES)
    a, b, c = _CUBE_TRIANGLES[0]
    _build_triangulated(flipped, _CUBE_COORDS, ((a, c, b),) + _CUBE_TRIANGLES[1:])

    report = diff_files(base, flipped)
    assert_report_invariants(report)
    assert report["stats"]["modified"] == 1
    item = report["modified"][0]
    assert item["old"]["guid"] == _GUID
    assert item["aspects"]["geometry"] == "changed"


def test_polygonal_face_index_edit_is_a_geometry_change(tmp_path):
    base = str(tmp_path / "quad.ifc")
    edited = str(tmp_path / "quad_edited.ifc")
    _build_polygonal(base, _CUBE_COORDS, _QUAD_FACES)
    a, b, c, d = _QUAD_FACES[0]
    _build_polygonal(edited, _CUBE_COORDS, ((b, a, c, d),) + _QUAD_FACES[1:])

    report = diff_files(base, edited)
    assert_report_invariants(report)
    assert report["stats"]["modified"] == 1
    item = report["modified"][0]
    assert item["old"]["guid"] == _GUID
    assert item["aspects"]["geometry"] == "changed"


def test_polygonal_face_list_order_is_neutral(tmp_path):
    base = str(tmp_path / "quad.ifc")
    reordered = str(tmp_path / "quad_reordered.ifc")
    _build_polygonal(base, _CUBE_COORDS, _QUAD_FACES)
    _build_polygonal(reordered, _CUBE_COORDS, tuple(reversed(_QUAD_FACES)))

    report = diff_files(base, reordered)
    assert_report_invariants(report)
    assert_zero_diff(report)
