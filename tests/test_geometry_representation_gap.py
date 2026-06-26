"""Representation-equivalence gap: extrusion vs mesh of the same solid.

A documented IFC-diff failure mode: the *same* shape stored as an extruded
solid in one file and as a tessellated mesh in another resolves to an identical
BRep, yet Athar reports it as geometry-changed. `vh_geometry` hashes the
geometry-domain subgraph (representation items, profiles, points), not a
resolved solid, so a representation-kind swap produces a different hash even
when the realized geometry is the same.

This is a deliberate limitation, not a bug to silently fix here: the engine
contract is "No deep B-rep comparison" (see CLAUDE.md / athar/bottom/AGENTS.md).
Closing the gap would require resolving each representation to a canonical solid
through a geometry kernel, which the engine intentionally does not do.

The pair below is a genuine equivalence case: both proxies are the same
1x1x1 m cube (centered in X/Y, 0..1 in Z), share one GlobalId, name, and
placement, and differ only in how the body is encoded. Identity is preserved
(the proxy is matched by GUID, never added+deleted); the geometry aspect is the
only thing that flips. The `xfail(strict=True)` marker asserts the *desired*
behavior (geometry unchanged): if the engine ever learns representation
equivalence, this test will XPASS and the marker must be removed.
"""

from __future__ import annotations

import ifcopenshell
import ifcopenshell.guid
import pytest

from athar.engine import diff_files

# Same physical cube, two encodings. Centered in X/Y like IfcRectangleProfileDef,
# spanning 0..1 in Z like the extrusion depth.
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
# 1-based triangle indices (IfcTriangulatedFaceSet.CoordIndex) for the 12 faces.
_CUBE_TRIANGLES = (
    (1, 2, 3), (1, 3, 4),  # bottom
    (5, 7, 6), (5, 8, 7),  # top
    (1, 2, 6), (1, 6, 5),
    (2, 3, 7), (2, 7, 6),
    (3, 4, 8), (3, 8, 7),
    (4, 1, 5), (4, 5, 8),
)

_SHARED_GUID = ifcopenshell.guid.new()


def _new_model() -> tuple[ifcopenshell.file, object]:
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
        GlobalId=ifcopenshell.guid.new(),
        Name="rep-gap",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )
    return f, context


def _add_cube_proxy(f: ifcopenshell.file, context, item, rep_type: str) -> None:
    placement = f.create_entity(
        "IfcLocalPlacement",
        RelativePlacement=f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
        ),
    )
    shape = f.create_entity(
        "IfcShapeRepresentation",
        ContextOfItems=context,
        RepresentationIdentifier="Body",
        RepresentationType=rep_type,
        Items=[item],
    )
    f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_SHARED_GUID,
        Name="cube",
        ObjectPlacement=placement,
        Representation=f.create_entity("IfcProductDefinitionShape", Representations=[shape]),
    )


def _extrusion_item(f: ifcopenshell.file):
    profile = f.create_entity(
        "IfcRectangleProfileDef",
        ProfileType="AREA",
        Position=f.create_entity(
            "IfcAxis2Placement2D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0)),
        ),
        XDim=1.0,
        YDim=1.0,
    )
    return f.create_entity(
        "IfcExtrudedAreaSolid",
        SweptArea=profile,
        Position=f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
        ),
        ExtrudedDirection=f.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)),
        Depth=1.0,
    )


def _tessellation_item(f: ifcopenshell.file):
    return f.create_entity(
        "IfcTriangulatedFaceSet",
        Coordinates=f.create_entity("IfcCartesianPointList3D", CoordList=_CUBE_COORDS),
        Closed=True,
        CoordIndex=_CUBE_TRIANGLES,
    )


def _write_pair(tmp_path) -> tuple[str, str]:
    extruded, ctx = _new_model()
    _add_cube_proxy(extruded, ctx, _extrusion_item(extruded), "SweptSolid")
    old_path = str(tmp_path / "cube_extrusion.ifc")
    extruded.write(old_path)

    meshed, ctx2 = _new_model()
    _add_cube_proxy(meshed, ctx2, _tessellation_item(meshed), "Tessellation")
    new_path = str(tmp_path / "cube_mesh.ifc")
    meshed.write(new_path)
    return old_path, new_path


def test_representation_swap_preserves_identity(tmp_path):
    """Identity must survive an encoding swap: matched by GUID, never churned."""
    old_path, new_path = _write_pair(tmp_path)
    report = diff_files(old_path, new_path)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    # Exactly the one proxy, matched by its shared GlobalId.
    matched = report["modified"] + report["unchanged"]
    assert len(matched) == 1
    assert matched[0]["match"]["reason"] == "guid"
    assert matched[0]["new"]["guid"] == _SHARED_GUID


@pytest.mark.xfail(
    strict=True,
    reason=(
        "representation-equivalence gap: an extrusion and a mesh of the same "
        "solid hash differently because vh_geometry hashes the geometry-domain "
        "subgraph, not a resolved BRep. The engine intentionally does no deep "
        "B-rep comparison. Remove this marker if/when the gap is closed."
    ),
)
def test_equivalent_representations_report_geometry_unchanged(tmp_path):
    """Desired behavior: same realized solid -> geometry unchanged.

    Currently fails (xfail) because the two encodings produce different
    geometry subgraph hashes. The proxy lands in `modified` with the geometry
    aspect flipped to `changed`.
    """
    old_path, new_path = _write_pair(tmp_path)
    report = diff_files(old_path, new_path)
    geometry_states = [item["aspects"]["geometry"] for item in report["modified"]]
    assert "changed" not in geometry_states
