"""Every edit to a small model is either reported on the entity it belongs to
or named in `stats.coverage.unexamined_changes`; no-op rewrites report nothing.
"""

from __future__ import annotations

import random
import re

import ifcopenshell
import ifcopenshell.util.element
import pytest

from athar.bottom.signatures import build_signature_bundle
from athar.bottom.types import ParseDiagnostics, SignatureBundle
from athar.engine import diff_files


def _guid(tag: str) -> str:
    return tag.ljust(22, "0")


def _build() -> ifcopenshell.file:
    f = ifcopenshell.file(schema="IFC4")
    c = f.create_entity

    person = c("IfcPerson", None, "Doe", "Jo")
    organization = c("IfcOrganization", None, "Org")
    application = c("IfcApplication", organization, "1.0", "Authoring", "authoring")
    history = c(
        "IfcOwnerHistory",
        c("IfcPersonAndOrganization", person, organization),
        application,
        None,
        "ADDED",
        None,
        None,
        None,
        1700000000,
    )

    origin = c("IfcCartesianPoint", (0.0, 0.0, 0.0))
    ctx = c(
        "IfcGeometricRepresentationContext",
        None,
        "Model",
        3,
        1e-5,
        c("IfcAxis2Placement3D", origin, None, None),
        c("IfcDirection", (0.0, 1.0)),
    )
    length = c("IfcSIUnit", None, "LENGTHUNIT", None, "METRE")
    project = c("IfcProject", _guid("project"), history, "P", None, None, None, None, [ctx], c("IfcUnitAssignment", [length]))
    crs = c("IfcProjectedCRS", "EPSG:25832", None, None, None, None, None, None)
    c("IfcMapConversion", ctx, crs, 500000.0, 5300000.0, 400.0, 1.0, 0.0, 1.0)

    def placement(rel, x, y, z):
        return c("IfcLocalPlacement", rel, c("IfcAxis2Placement3D", c("IfcCartesianPoint", (x, y, z)), None, None))

    site_pl = placement(None, 0.0, 0.0, 0.0)
    site = c("IfcSite", _guid("site"), None, "Site", None, None, site_pl, None, None, "ELEMENT", (48, 8, 0, 0), (11, 34, 0, 0), 500.0, None, None)
    bld_pl = placement(site_pl, 0.0, 0.0, 0.0)
    building = c("IfcBuilding", _guid("building"), None, "B", None, None, bld_pl, None, None, "ELEMENT", None, None, None)
    s1 = c("IfcBuildingStorey", _guid("storey1"), None, "L1", None, None, placement(bld_pl, 0.0, 0.0, 0.0), None, None, "ELEMENT", 0.0)
    s2 = c("IfcBuildingStorey", _guid("storey2"), None, "L2", None, None, placement(bld_pl, 0.0, 0.0, 3.0), None, None, "ELEMENT", 3.0)
    space = c("IfcSpace", _guid("space"), None, "SP1", None, None, placement(bld_pl, 0.0, 5.0, 0.0), None, None, "ELEMENT", "INTERNAL", None)
    c("IfcRelAggregates", _guid("agg1"), None, None, None, project, [site])
    c("IfcRelAggregates", _guid("agg2"), None, None, None, site, [building])
    c("IfcRelAggregates", _guid("agg3"), None, None, None, building, [s1, s2])
    c("IfcRelAggregates", _guid("agg4"), None, None, None, s1, [space])

    def extrusion(profile):
        return c(
            "IfcExtrudedAreaSolid",
            profile,
            c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None),
            c("IfcDirection", (0.0, 0.0, 1.0)),
            2.8,
        )

    def element(cls, name, x, item, representation_type, *tail):
        shape = c("IfcProductDefinitionShape", None, None, [c("IfcShapeRepresentation", ctx, "Body", representation_type, [item])])
        return c(cls, _guid(name), history, name, None, None, placement(bld_pl, x, 0.0, 0.0), shape, None, *tail)

    def rectangle():
        return c("IfcRectangleProfileDef", "AREA", None, c("IfcAxis2Placement2D", c("IfcCartesianPoint", (0.0, 0.0)), None), 4.0, 0.2)

    w1 = element("IfcWall", "W1", 0.0, extrusion(rectangle()), "SweptSolid", "STANDARD")
    w2 = element("IfcWall", "W2", 10.0, extrusion(rectangle()), "SweptSolid", "STANDARD")

    shading = c("IfcSurfaceStyleShading", c("IfcColourRgb", None, 0.5, 0.5, 0.5), None)
    c("IfcStyledItem", w2.Representation.Representations[0].Items[0], [c("IfcSurfaceStyle", "Plaster", "BOTH", [shading])], None)

    c("IfcPresentationLayerAssignment", "A-WALL", None, [w1.Representation.Representations[0], w2.Representation.Representations[0]], None)

    corners = [c("IfcCartesianPoint", p) for p in ((0.0, 0.0), (4.0, 0.0), (4.0, 0.2), (0.0, 0.2))]
    outline = c("IfcArbitraryClosedProfileDef", "AREA", None, c("IfcPolyline", corners + [corners[0]]))
    w3 = element("IfcWall", "W3", 20.0, extrusion(outline), "SweptSolid", "STANDARD")

    vertices = [c("IfcCartesianPoint", p) for p in ((0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (1.0, 2.0, 0.0), (0.0, 1.0, 0.0))]
    face = c("IfcFace", [c("IfcFaceOuterBound", c("IfcPolyLoop", vertices), True)])
    b1 = element("IfcBuildingElementProxy", "B1", 30.0, c("IfcFacetedBrep", c("IfcClosedShell", [face])), "Brep", None)

    c1 = element("IfcCovering", "C1", 40.0, extrusion(rectangle()), "SweptSolid", "CLADDING")
    c1_body = c1.Representation.Representations[0]
    source = c(
        "IfcRepresentationMap",
        c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None),
        c("IfcShapeRepresentation", ctx, "Body", "SweptSolid", list(c1_body.Items)),
    )
    c1_body.Items = [c("IfcMappedItem", source, c("IfcCartesianTransformationOperator3D", None, None, c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None))]
    c1_body.RepresentationType = "MappedRepresentation"

    c("IfcRelDefinesByType", _guid("reltype1"), None, None, None, [c1], c("IfcCoveringType", _guid("ctype"), None, "CT", None, None, None, [source], None, None, "CLADDING"))
    unused_map = c(
        "IfcRepresentationMap",
        c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None),
        c("IfcShapeRepresentation", ctx, "Body", "SweptSolid", [extrusion(rectangle())]),
    )
    c("IfcRelDefinesByType", _guid("reltype2"), None, None, None, [w2], c("IfcWallType", _guid("wtype"), None, "WT", None, None, None, [unused_map], None, None, "STANDARD"))

    o1 = element("IfcOpeningElement", "O1", 1.0, extrusion(rectangle()), "SweptSolid", "OPENING")
    c("IfcRelVoidsElement", _guid("void"), None, None, None, w1, o1)

    w2.ObjectPlacement.RelativePlacement.Axis = c("IfcDirection", (0.0, 0.0, 2.0))
    w2.ObjectPlacement.RelativePlacement.RefDirection = c("IfcDirection", (3.0, 4.0, 0.0))
    centre_line = c("IfcLine", c("IfcCartesianPoint", (0.0, 0.0)), c("IfcVector", c("IfcDirection", (1.0, 0.0)), 4.0))
    w3.Representation.Representations = list(w3.Representation.Representations) + [c("IfcShapeRepresentation", ctx, "Axis", "Curve2D", [centre_line])]

    def grid_axis(tag, start, direction):
        line = c("IfcLine", c("IfcCartesianPoint", start), c("IfcVector", c("IfcDirection", direction), 10.0))
        return c("IfcGridAxis", tag, line, True)

    axis_a, axis_b, axis_1 = grid_axis("A", (0.0, 0.0), (0.0, 1.0)), grid_axis("B", (5.0, 0.0), (0.0, 1.0)), grid_axis("1", (0.0, 0.0), (1.0, 0.0))
    grid = c("IfcGrid", GlobalId=_guid("grid"), Name="G1", ObjectPlacement=placement(bld_pl, 0.0, 0.0, 0.0), UAxes=[axis_a, axis_b], VAxes=[axis_1])
    on_grid = c("IfcGridPlacement", c("IfcVirtualGridIntersection", [axis_a, axis_1], (0.0, 0.0, 0.0)), None)
    def body():
        corner = c("IfcBoundingBox", c("IfcCartesianPoint", (0.5, 0.5, 0.5)), 1.0, 1.0, 1.0)
        return c("IfcProductDefinitionShape", None, None, [c("IfcShapeRepresentation", ctx, "Box", "BoundingBox", [corner])])

    gp1 = c("IfcBuildingElementProxy", GlobalId=_guid("GP1"), Name="GP1", ObjectPlacement=on_grid, Representation=body())
    below_grid = c("IfcLocalPlacement", on_grid, c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 1.0)), None, None))
    gp2 = c("IfcBuildingElementProxy", GlobalId=_guid("GP2"), Name="GP2", ObjectPlacement=below_grid, Representation=body())

    c("IfcRelContainedInSpatialStructure", _guid("contain"), None, None, None, [w1, w2, w3, b1, c1, grid, gp1, gp2], s1)

    concrete = c("IfcMaterial", "Concrete", None, None)
    insulation = c("IfcMaterial", "Insulation", None, None)
    layers = c(
        "IfcMaterialLayerSet",
        [
            c("IfcMaterialLayer", concrete, 0.15, None, None, None, None, None),
            c("IfcMaterialLayer", insulation, 0.05, None, None, None, None, None),
        ],
        "WallBuildUp",
        None,
    )
    c("IfcRelAssociatesMaterial", _guid("relmat"), None, None, None, [w1], c("IfcMaterialLayerSetUsage", layers, "AXIS2", "POSITIVE", 0.0, None))
    c("IfcMaterial", "Spare", None, None)

    classification = c("IfcClassification", "Uniclass", None, None, "Uniclass 2015", None, None, None)
    reference = c("IfcClassificationReference", None, "Ss_25_10", "Wall systems", classification, None, None)
    c("IfcRelAssociatesClassification", _guid("relcls"), None, None, None, [w1], reference)

    pset = c("IfcPropertySet", _guid("pset"), None, "Pset_WallCommon", None, [c("IfcPropertySingleValue", "FireRating", None, c("IfcLabel", "EI60"), None)])
    c("IfcRelDefinesByProperties", _guid("relpset"), None, None, None, [w1], pset)

    c("IfcRelAssignsToGroup", _guid("relgroup"), None, None, None, [w2], None, c("IfcGroup", _guid("group"), None, "Zone A", None, None))
    c("IfcRelSpaceBoundary", _guid("boundary"), None, None, None, space, w2, None, "PHYSICAL", "INTERNAL")
    return f


def _one(f, cls, **match):
    hits = [e for e in f.by_type(cls) if all(getattr(e, k) == v for k, v in match.items())]
    assert len(hits) == 1, (cls, match, len(hits))
    return hits[0]


def _report(tmp_path, edit) -> dict:
    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    _build().write(str(base))
    f = ifcopenshell.open(str(base))
    edit(f)
    f.write(str(edited))
    return diff_files(str(base), str(edited))


def _modified(report: dict) -> dict[str, set[str]]:
    assert report["added"] == [] and report["deleted"] == []
    return {
        item["new"]["name"]: {
            aspect
            for aspect in ("geometry", "data", "topology", "placement")
            if item["aspects"][aspect] == "changed"
        }
        for item in report["modified"]
    }


def _extrusion(f, wall: str):
    return _one(f, "IfcWall", Name=wall).Representation.Representations[0].Items[0]


def _layer_thickness(f):
    _one(f, "IfcMaterialLayer", LayerThickness=0.15).LayerThickness = 0.25


def _layer_material_name(f):
    _one(f, "IfcMaterial", Name="Insulation").Name = "Mineral wool"


def _classification_code(f):
    _one(f, "IfcClassificationReference").Identification = "Ss_25_13"


def _group_membership(f):
    _one(f, "IfcRelAssignsToGroup").RelatedObjects = [_one(f, "IfcWall", Name="W1")]


def _group_name(f):
    _one(f, "IfcGroup").Name = "Zone B"


def _map_conversion(f):
    _one(f, "IfcMapConversion").Eastings = 500012.5


def _true_north(f):
    _one(f, "IfcGeometricRepresentationContext").TrueNorth.DirectionRatios = (0.17, 0.98)


def _unit_added(f):
    units = _one(f, "IfcUnitAssignment")
    units.Units = list(units.Units) + [f.create_entity("IfcSIUnit", None, "TIMEUNIT", None, "SECOND")]


def _bowtie(f):
    polyline = _one(f, "IfcPolyline")
    a, b, c, d, _ = polyline.Points
    polyline.Points = [a, c, b, d, a]


def _unshare_closing_point(f):
    polyline = _one(f, "IfcPolyline")
    points = list(polyline.Points)
    points[-1] = f.create_entity("IfcCartesianPoint", points[0].Coordinates)
    polyline.Points = points


def _loop_start_rotated(f):
    loop = _one(f, "IfcPolyLoop")
    a, b, c, d = loop.Polygon
    loop.Polygon = [c, d, a, b]


def _loop_reversed(f):
    loop = _one(f, "IfcPolyLoop")
    loop.Polygon = list(reversed(loop.Polygon))


def _document_added(f):
    document = f.create_entity("IfcDocumentReference", "https://example.org/spec.pdf", "D-1", "Wall spec", None, None)
    f.create_entity("IfcRelAssociatesDocument", _guid("reldoc"), None, None, None, [_one(f, "IfcWall", Name="W1")], document)


def _covering_attached(f):
    f.create_entity("IfcRelCoversBldgElements", _guid("relcover"), None, None, None, _one(f, "IfcWall", Name="W2"), [_one(f, "IfcCovering")])


def _boundary_flipped(f):
    _one(f, "IfcRelSpaceBoundary").InternalOrExternalBoundary = "EXTERNAL"


def _wall_name(f):
    _one(f, "IfcWall", Name="W1").Name = "W1 renamed"


def _fire_rating(f):
    _one(f, "IfcPropertySingleValue", Name="FireRating").NominalValue = f.create_entity("IfcLabel", "EI30")


def _predefined_type(f):
    _one(f, "IfcWall", Name="W1").PredefinedType = "PARTITIONING"


def _extrusion_depth(f):
    _extrusion(f, "W1").Depth = 3.5


def _profile_xdim(f):
    _extrusion(f, "W1").SweptArea.XDim = 5.0


def _extrusion_position_rotated(f):
    _extrusion(f, "W1").Position.RefDirection = f.create_entity("IfcDirection", (0.0, 1.0, 0.0))


def _placement_rotated(f):
    _one(f, "IfcWall", Name="W2").ObjectPlacement.RelativePlacement.RefDirection.DirectionRatios = (4.0, 3.0, 0.0)


def _placement_axis_made_a_zero_direction(f):
    relative = _one(f, "IfcWall", Name="W3").ObjectPlacement.RelativePlacement
    assert relative.Axis is None
    relative.Axis = f.create_entity("IfcDirection", (0.0, 0.0, 0.0))


def _vector_magnitude(f):
    _one(f, "IfcVector", Magnitude=4.0).Magnitude = 8.0


def _operator_scale(f):
    _one(f, "IfcCartesianTransformationOperator3D").Scale = 2.0


def _operator_mirrored(f):
    _one(f, "IfcCartesianTransformationOperator3D").Axis2 = f.create_entity("IfcDirection", (0.0, -1.0, 0.0))


def _type_map_used_by_occurrence(f):
    _one(f, "IfcCoveringType").RepresentationMaps[0].MappedRepresentation.Items[0].Depth = 3.5


def _type_map_not_used_by_occurrence(f):
    _one(f, "IfcWallType").RepresentationMaps[0].MappedRepresentation.Items[0].Depth = 3.5


def _type_swapped_for_one_with_another_map(f):
    """The occurrence keeps mapping the map of the type it had."""
    c = f.create_entity
    other_map = c(
        "IfcRepresentationMap",
        c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None),
        c("IfcShapeRepresentation", _one(f, "IfcGeometricRepresentationContext"), "Body", "SweptSolid", [_extrusion(f, "W3")]),
    )
    old_type = _one(f, "IfcCoveringType")
    _one(f, "IfcRelDefinesByType", RelatingType=old_type).RelatingType = c(
        "IfcCoveringType", old_type.GlobalId, None, "CT", None, None, None, [other_map], None, None, "CLADDING"
    )
    f.remove(old_type)


def _type_without_shape_assigned(f):
    bare = f.create_entity("IfcWallType", _guid("bare"), None, "Bare", None, None, None, None, None, None, "STANDARD")
    f.create_entity("IfcRelDefinesByType", _guid("reltype3"), None, None, None, [_one(f, "IfcWall", Name="W3")], bare)


def _void_removed(f):
    f.remove(_one(f, "IfcRelVoidsElement"))


def _shift_x(product, dx):
    location = product.ObjectPlacement.RelativePlacement.Location
    x, y, z = location.Coordinates
    location.Coordinates = (x + dx, y, z)


def _opening_moved_within_its_wall(f):
    _shift_x(_one(f, "IfcOpeningElement"), 0.5)


def _wall_moved_with_its_opening(f):
    _shift_x(_one(f, "IfcWall", Name="W1"), 2.5)
    _shift_x(_one(f, "IfcOpeningElement"), 2.5)


def _grid_axis_moved(f):
    _one(f, "IfcGridAxis", AxisTag="A").AxisCurve.Pnt.Coordinates = (1.0, 0.0)


def _grid_intersection_switched(f):
    intersection = _one(f, "IfcVirtualGridIntersection")
    intersection.IntersectingAxes = [_one(f, "IfcGridAxis", AxisTag="B"), intersection.IntersectingAxes[1]]


def _grid_moved(f):
    _one(f, "IfcGrid").ObjectPlacement.RelativePlacement.Location.Coordinates = (0.0, 1.0, 0.0)


def _offset_below_grid_placement(f):
    _one(f, "IfcBuildingElementProxy", Name="GP2").ObjectPlacement.RelativePlacement.Location.Coordinates = (0.0, 0.0, 2.0)


def _layer_order(f):
    layers = _one(f, "IfcMaterialLayerSet")
    layers.MaterialLayers = list(reversed(layers.MaterialLayers))


def _style_colour(f):
    _one(f, "IfcColourRgb").Red = 0.9


def _representation_identifier(f):
    _one(f, "IfcShapeRepresentation", RepresentationIdentifier="Axis").RepresentationIdentifier = "Annotation"


def _layer_name(f):
    _one(f, "IfcPresentationLayerAssignment").Name = "A-WALL-EXT"


def _site_latitude(f):
    _one(f, "IfcSite").RefLatitude = (48, 9, 0, 0)


def _owner_history_timestamp(f):
    _one(f, "IfcOwnerHistory").CreationDate = 1800000000


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        (lambda f: None, {}),
        (_layer_thickness, {"W1": {"data"}}),
        (_layer_material_name, {"W1": {"data"}}),
        (_classification_code, {"W1": {"data"}}),
        (_group_membership, {"W1": {"data"}, "W2": {"data"}}),
        (_group_name, {"W2": {"data"}}),
        (_map_conversion, {"P": {"data"}}),
        (_true_north, {"P": {"data"}}),
        (_unit_added, {"P": {"data"}}),
        (_bowtie, {"W3": {"geometry"}}),
        (_unshare_closing_point, {}),
        (_loop_start_rotated, {}),
        (_loop_reversed, {"B1": {"geometry"}}),
        (_document_added, {"W1": {"data"}}),
        (_covering_attached, {"W2": {"topology"}, "C1": {"topology"}}),
        (_boundary_flipped, {"W2": {"data"}, "SP1": {"data"}}),
        (_fire_rating, {"W1": {"data"}}),
        (_predefined_type, {"W1": {"data"}}),
        (_extrusion_depth, {"W1": {"geometry"}}),
        (_profile_xdim, {"W1": {"geometry"}}),
        (_extrusion_position_rotated, {"W1": {"geometry"}}),
        (_placement_rotated, {"W2": {"placement"}}),
        (_placement_axis_made_a_zero_direction, {"W3": {"placement"}}),
        (_vector_magnitude, {"W3": {"geometry"}}),
        (_operator_scale, {"C1": {"geometry"}}),
        (_operator_mirrored, {"C1": {"geometry"}}),
        (_type_map_used_by_occurrence, {"C1": {"geometry", "data"}}),
        (_type_map_not_used_by_occurrence, {"W2": {"data"}}),
        (_type_swapped_for_one_with_another_map, {"C1": {"data"}}),
        (_type_without_shape_assigned, {"W3": {"data", "topology"}}),
        (_void_removed, {"W1": {"geometry", "topology"}, "O1": {"topology"}}),
        (_opening_moved_within_its_wall, {"W1": {"geometry"}, "O1": {"placement"}}),
        (_wall_moved_with_its_opening, {"W1": {"placement"}, "O1": {"placement"}}),
        (_grid_axis_moved, {"G1": {"data"}, "GP1": {"placement"}, "GP2": {"placement"}}),
        (_grid_intersection_switched, {"GP1": {"placement"}, "GP2": {"placement"}}),
        (_grid_moved, {"G1": {"placement"}, "GP1": {"placement"}, "GP2": {"placement"}}),
        (_offset_below_grid_placement, {"GP2": {"placement"}}),
        (_layer_order, {"W1": {"data"}}),
        (_style_colour, {"W2": {"data"}}),
        (_representation_identifier, {"W3": {"data"}}),
        (_layer_name, {"W1": {"data"}, "W2": {"data"}}),
        (_site_latitude, {"Site": {"data"}}),
        (_owner_history_timestamp, {}),
    ],
    ids=lambda value: getattr(value, "__name__", None),
)
def test_edit_is_reported_on_the_entity_it_belongs_to(tmp_path, edit, expected):
    report = _report(tmp_path, edit)
    assert _modified(report) == expected
    assert report["stats"]["coverage"]["unexamined_changes"] == []
    for item in report["modified"]:
        if item["aspects"]["data"] == "changed":
            assert item["data_delta"], item["new"]["name"]
            assert not any(delta["path"].endswith(".(structure)") for delta in item["data_delta"]), item["data_delta"]


@pytest.mark.parametrize(
    ("edit", "owner", "path", "old", "new"),
    [
        (_true_north, "P", "IfcDirection.DirectionRatios", "[r0,r100000]", "[r17092,r98529]"),
        (_style_colour, "W2", "IfcColourRgb.Red", "0.5", "0.9"),
        (_representation_identifier, "W3", "IfcShapeRepresentation.RepresentationIdentifier[1]", "Axis", "Annotation"),
        (_layer_order, "W1", "MaterialLayers[1]/IfcMaterialLayer.LayerThickness", "0.15 m", "0.05 m"),
        (_layer_order, "W1", "MaterialLayers[2]/IfcMaterial.Name", "Insulation", "Concrete"),
    ],
    ids=lambda value: getattr(value, "__name__", None),
)
def test_data_change_names_what_differs(tmp_path, edit, owner, path, old, new):
    report = _report(tmp_path, edit)
    [item] = [item for item in report["modified"] if item["new"]["name"] == owner]
    assert {"path": path, "old": old, "new": new} in item["data_delta"]


def test_type_map_change_is_named_by_a_shape_fact(tmp_path):
    report = _report(tmp_path, _type_map_not_used_by_occurrence)
    [item] = report["modified"]
    assert [delta["path"] for delta in item["data_delta"]] == [
        "IfcWallType[Name=WT]/RepresentationMaps[1]/IfcRepresentationMap.(shape)"
    ]


def _report_after(tmp_path, setup, edit) -> dict:
    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    for path, edits in ((base, (setup,)), (edited, (setup, edit))):
        f = _build()
        for step in edits:
            step(f)
        f.write(str(path))
    return diff_files(str(base), str(edited))


def _second_map_on_the_covering_type(f):
    c = f.create_entity
    solid = _extrusion(f, "W1")
    spare = c(
        "IfcRepresentationMap",
        c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None),
        c(
            "IfcShapeRepresentation",
            _one(f, "IfcGeometricRepresentationContext"),
            "Body",
            "SweptSolid",
            [c("IfcExtrudedAreaSolid", solid.SweptArea, solid.Position, solid.ExtrudedDirection, 1.0)],
        ),
    )
    covering_type = _one(f, "IfcCoveringType")
    covering_type.RepresentationMaps = list(covering_type.RepresentationMaps) + [spare]


@pytest.mark.parametrize("position", [0, 1], ids=["mapped", "not_mapped"])
def test_type_map_content_changes_data_and_only_mapped_geometry(tmp_path, position):
    def edit(f):
        _one(f, "IfcCoveringType").RepresentationMaps[position].MappedRepresentation.Items[0].Depth = 3.5

    report = _report_after(tmp_path, _second_map_on_the_covering_type, edit)
    assert _modified(report) == {"C1": {"geometry", "data"} if position == 0 else {"data"}}
    [item] = report["modified"]
    assert [delta["path"] for delta in item["data_delta"]] == [
        f"IfcCoveringType[Name=CT]/RepresentationMaps[{position + 1}]/IfcRepresentationMap.(shape)"
    ]
    for item in report["modified"]:
        assert (item["aspects"]["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"])


def _material_profile_of_the_swept_area(f, wall: str, profile):
    c = f.create_entity
    profiles = c("IfcMaterialProfileSet", None, None, [c("IfcMaterialProfile", None, None, c("IfcMaterial", "Steel", None, None), profile)])
    c("IfcRelAssociatesMaterial", _guid("relprof"), None, None, None, [_one(f, "IfcWall", Name=wall)], profiles)


@pytest.mark.parametrize("shared", [True, False], ids=["one_instance", "equal_copy"])
def test_swept_profile_edit_changes_data_only_when_material_profile_shares_it(tmp_path, shared):
    def setup(f):
        swept = _extrusion(f, "W3").SweptArea
        _material_profile_of_the_swept_area(f, "W3", swept if shared else ifcopenshell.util.element.copy_deep(f, swept))

    def edit(f):
        curve = _extrusion(f, "W3").SweptArea.OuterCurve
        curve.Points = list(curve.Points[:2]) + [f.create_entity("IfcCartesianPoint", (4.0, 0.5))] + list(curve.Points[3:])

    report = _report_after(tmp_path, setup, edit)
    assert _modified(report) == {"W3": {"geometry", "data"} if shared else {"geometry"}}
    for item in report["modified"]:
        assert (item["aspects"]["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"])


def test_edit_of_a_material_profile_copy_of_the_swept_area_is_data_only(tmp_path):
    def setup(f):
        _material_profile_of_the_swept_area(f, "W3", ifcopenshell.util.element.copy_deep(f, _extrusion(f, "W3").SweptArea))

    def edit(f):
        curve = _one(f, "IfcMaterialProfile").Profile.OuterCurve
        curve.Points = list(curve.Points[:2]) + [f.create_entity("IfcCartesianPoint", (4.0, 0.5))] + list(curve.Points[3:])

    report = _report_after(tmp_path, setup, edit)
    assert _modified(report) == {"W3": {"data"}}
    for item in report["modified"]:
        assert (item["aspects"]["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"])


def _covering_maps_both_type_maps(f):
    _second_map_on_the_covering_type(f)
    c = f.create_entity
    body = _one(f, "IfcCovering").Representation.Representations[0]
    spare = _one(f, "IfcCoveringType").RepresentationMaps[1]
    operator = c("IfcCartesianTransformationOperator3D", None, None, c("IfcCartesianPoint", (0.0, 0.0, 0.0)), None, None)
    body.Items = list(body.Items) + [c("IfcMappedItem", spare, operator)]


def test_mapped_type_maps_swapping_places_beside_scale_edit_changes_geometry_and_data(tmp_path):
    def edit(f):
        covering_type = _one(f, "IfcCoveringType")
        covering_type.RepresentationMaps = list(reversed(covering_type.RepresentationMaps))
        body = _one(f, "IfcCovering").Representation.Representations[0]
        body.Items[0].MappingTarget.Scale = 2.0

    report = _report_after(tmp_path, _covering_maps_both_type_maps, edit)
    assert _modified(report) == {"C1": {"geometry", "data"}}
    [item] = report["modified"]
    assert [delta["path"] for delta in item["data_delta"]] == [
        "IfcCoveringType[Name=CT]/RepresentationMaps[1]/IfcRepresentationMap.(shape)",
        "IfcCoveringType[Name=CT]/RepresentationMaps[2]/IfcRepresentationMap.(shape)",
    ]
    for item in report["modified"]:
        assert (item["aspects"]["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"])


def test_port_nested_under_a_type_reports_the_type_shape_as_data(tmp_path):
    def with_port(f):
        port = f.create_entity("IfcDistributionPort", GlobalId=_guid("port"), Name="PT1")
        f.create_entity("IfcRelNests", _guid("nest"), None, None, None, _one(f, "IfcWallType"), [port])

    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    for path, edits in ((base, (with_port,)), (edited, (with_port, _type_map_not_used_by_occurrence))):
        f = _build()
        for edit in edits:
            edit(f)
        f.write(str(path))
    assert _modified(diff_files(str(base), str(edited))) == {"W2": {"data"}, "PT1": {"data"}}


def test_large_aggregate_reached_as_data_is_a_bounded_fact(tmp_path):
    count = 60

    def coloured_mesh(f):
        c = f.create_entity
        mesh = c(
            "IfcTriangulatedFaceSet",
            Coordinates=c("IfcCartesianPointList3D", [(float(i), float(i % 7), 0.0) for i in range(count + 2)]),
            CoordIndex=[(i + 1, i + 2, i + 3) for i in range(count)],
        )
        colours = c("IfcColourRgbList", [(i / count, 0.5, 0.25) for i in range(count)])
        c("IfcIndexedColourMap", MappedTo=mesh, Colours=colours, ColourIndex=list(range(1, count + 1)))
        context = _one(f, "IfcGeometricRepresentationContext")
        for name in ("M1", "M2", "M3"):
            shape = c("IfcProductDefinitionShape", None, None, [c("IfcShapeRepresentation", context, "Body", "Tessellation", [mesh])])
            c("IfcBuildingElementProxy", GlobalId=_guid(name), Name=name, Representation=shape)

    def recoloured(f):
        coloured_mesh(f)
        colours = _one(f, "IfcColourRgbList")
        colours.ColourList = [(0.9, 0.9, 0.9)] + list(colours.ColourList[1:])

    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    for path, change in ((base, coloured_mesh), (edited, recoloured)):
        f = _build()
        change(f)
        f.write(str(path))
    report = diff_files(str(base), str(edited))

    assert _modified(report) == {name: {"data"} for name in ("M1", "M2", "M3")}
    for item in report["modified"]:
        [delta] = item["data_delta"]
        assert delta["path"].endswith("IfcColourRgbList.ColourList")
        assert delta["old"] != delta["new"]
        assert len(delta["old"]) <= 96 and len(delta["new"]) <= 96


def test_long_text_keeps_a_readable_start_in_data_delta(tmp_path):
    text = "".join(f"Clause {n}: fire stopping to be continuous. " for n in range(1, 9))[:300]
    assert len(text) == 300

    def described(f):
        _one(f, "IfcWall", Name="W1").Description = text

    def edit(f):
        _one(f, "IfcWall", Name="W1").Description = text[:250] + "#" + text[251:]

    report = _report_after(tmp_path, described, edit)
    assert _modified(report) == {"W1": {"data"}}
    [delta] = report["modified"][0]["data_delta"]
    assert delta["path"].endswith(".Description")
    assert delta["old"].startswith(text[:96]) and delta["new"].startswith(text[:96])
    assert "300 bytes" in delta["old"] and "300 bytes" in delta["new"]
    assert delta["old"] != delta["new"]
    assert len(delta["old"]) < 150


def test_shape_content_below_a_data_edge_is_named_by_a_shape_digest(tmp_path):
    def edit(f):
        _one(f, "IfcGeometricRepresentationContext").WorldCoordinateSystem.Location.Coordinates = (1.0, 0.0, 0.0)

    report = _report(tmp_path, edit)
    assert _modified(report) == {"P": {"data"}}
    [delta] = report["modified"][0]["data_delta"]
    assert delta["path"] == "IfcAxis2Placement3D.(shape)"
    assert delta["old"] != delta["new"]


def _member_moved_between_owners(tmp_path, owner):
    """Report for a `Status` property moved from one owner built by `owner` to a second."""

    def edit(f):
        c = f.create_entity
        wall = _one(f, "IfcWall", Name="W1")
        members = [
            [
                c("IfcPropertySingleValue", "Extra", None, c("IfcLabel", "X"), None),
                c("IfcPropertySingleValue", "Reference", None, c("IfcLabel", "A"), None),
            ],
            [
                c("IfcPropertySingleValue", "Reference", None, c("IfcLabel", "A"), None),
                c("IfcPropertySingleValue", "Status", None, c("IfcLabel", "New"), None),
            ],
        ]
        return owner(f, wall, members)

    def move(f):
        first, second = edit(f)
        status = _one(f, "IfcPropertySingleValue", Name="Status")
        second.HasProperties = [p for p in second.HasProperties if p != status]
        first.HasProperties = list(first.HasProperties) + [status]

    base = tmp_path / "base.ifc"
    moved = tmp_path / "moved.ifc"
    for path, change in ((base, edit), (moved, move)):
        f = _build()
        change(f)
        f.write(str(path))
    report = diff_files(str(base), str(moved))
    [item] = report["modified"]
    assert item["new"]["name"] == "W1" and item["aspects"]["data"] == "changed"
    return item


def test_member_moved_between_property_sets_is_named_under_each_set(tmp_path):
    def owner(f, wall, members):
        c = f.create_entity
        sets = [c("IfcPropertySet", _guid(f"moved{i}"), None, "Pset_Moved", None, props) for i, props in enumerate(members)]
        for i, pset in enumerate(sets):
            c("IfcRelDefinesByProperties", _guid(f"relmoved{i}"), None, None, None, [wall], pset)
        return sets

    item = _member_moved_between_owners(tmp_path, owner)
    first, second = (f"IfcPropertySet[Name=Pset_Moved,GlobalId={_guid(f'moved{i}')}]/IfcPropertySingleValue" for i in range(2))
    deltas = {(delta["path"], delta["old"], delta["new"]) for delta in item["data_delta"]}
    assert (f"{first}.Name[3]", None, "Status") in deltas
    assert (f"{second}.Name[2]", "Status", None) in deltas
    assert all(path.startswith((first, second)) for path, _, _ in deltas)


def test_member_moved_between_owners_without_identity_is_a_structure_change(tmp_path):
    def owner(f, wall, members):
        c = f.create_entity
        groups = [c("IfcComplexProperty", "Group", None, "usage", props) for props in members]
        pset = c("IfcPropertySet", _guid("grouped"), None, "Pset_Grouped", None, groups)
        c("IfcRelDefinesByProperties", _guid("relgrouped"), None, None, None, [wall], pset)
        return groups

    item = _member_moved_between_owners(tmp_path, owner)
    [delta] = item["data_delta"]
    assert delta["path"] == f"IfcWall[GlobalId={_guid('W1')}].(structure)"
    assert delta["old"] != delta["new"]


def test_grid_placed_element_is_marked_unresolved_and_carries_no_coordinates(tmp_path):
    report = _report(tmp_path, _grid_intersection_switched)
    by_name = {item["new"]["name"]: item for item in report["modified"] + report["unchanged"]}
    for name in ("GP1", "GP2"):
        item = by_name[name]
        assert item["old"]["placement_unresolved"] is True and item["new"]["placement_unresolved"] is True
        assert item["aspects"]["placement"] == "changed"
        assert item["aspects"]["placement_delta_mm"] is None
    assert "placement_unresolved" not in by_name["W1"]["new"]

    signatures = {sig.name: sig for sig in build_signature_bundle(str(tmp_path / "base.ifc")).signatures.values()}
    for name in ("GP1", "GP2"):
        assert signatures[name].placement is None
        assert signatures[name].centroid is None and signatures[name].aabb is None
    assert signatures["B1"].centroid is not None and signatures["B1"].aabb is not None


def test_opening_position_under_an_unresolved_placement_is_host_geometry(tmp_path):
    """GP1 hosts an opening placed relative to GP1's grid placement."""

    def with_opening(f):
        c = f.create_entity
        host = _one(f, "IfcBuildingElementProxy", Name="GP1")
        offset = c("IfcAxis2Placement3D", c("IfcCartesianPoint", (0.25, 0.0, 0.0)), None, None)
        opening = c(
            "IfcOpeningElement",
            GlobalId=_guid("GO1"),
            Name="GO1",
            ObjectPlacement=c("IfcLocalPlacement", host.ObjectPlacement, offset),
        )
        c("IfcRelVoidsElement", _guid("gridvoid"), None, None, None, host, opening)

    def opening_moved(f):
        with_opening(f)
        _shift_x(_one(f, "IfcOpeningElement", Name="GO1"), 0.5)

    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    for path, change in ((base, with_opening), (edited, opening_moved)):
        f = _build()
        change(f)
        f.write(str(path))

    assert _modified(diff_files(str(base), str(base))) == {}
    assert _modified(diff_files(str(base), str(edited))) == {"GP1": {"geometry"}, "GO1": {"placement"}}


def test_wall_rename_is_a_data_change(tmp_path):
    report = _report(tmp_path, _wall_name)
    assert _modified(report) == {"W1 renamed": {"data"}}


def test_length_unit_prefix_modifies_the_project(tmp_path):
    def edit(f):
        _one(f, "IfcSIUnit").Prefix = "MILLI"

    assert "data" in _modified(_report(tmp_path, edit))["P"]


def test_renumbered_and_shuffled_file_is_unchanged(tmp_path):
    base = tmp_path / "base.ifc"
    shuffled = tmp_path / "shuffled.ifc"
    _build().write(str(base))

    lines = base.read_text().splitlines()
    start = lines.index("DATA;") + 1
    end = lines.index("ENDSEC;", start)
    records = [re.sub(r"#(\d+)", lambda m: f"#{int(m.group(1)) * 7 + 1000}", line) for line in lines[start:end]]
    random.Random(7).shuffle(records)
    shuffled.write_text("\n".join(lines[:start] + records + lines[end:]) + "\n")
    assert len(ifcopenshell.open(str(shuffled)).by_type("IfcWall")) == 3

    report = diff_files(str(base), str(shuffled))
    assert _modified(report) == {}
    assert report["stats"]["coverage"]["unexamined_changes"] == []


def test_change_outside_every_signature_is_named_not_hidden(tmp_path):
    def edit(f):
        _one(f, "IfcMaterial", Name="Spare").Name = "Spare, renamed"

    report = _report(tmp_path, edit)
    assert _modified(report) == {}
    coverage = report["stats"]["coverage"]
    assert coverage["unexamined_changes"] == ["IfcMaterial"]
    assert coverage["old"]["unreached"]["IfcMaterial"]["count"] == 1
    assert coverage["old"]["unreached"]["IfcMaterial"]["digest"] != coverage["new"]["unreached"]["IfcMaterial"]["digest"]


def test_coverage_block_accounts_for_the_whole_base_model(tmp_path):
    coverage = _report(tmp_path, lambda f: None)["stats"]["coverage"]
    assert coverage["old"] == coverage["new"]
    assert coverage["old"]["unknown_relationships"] == {}
    assert coverage["old"]["unreached"].keys() == {"IfcMaterial"}
    assert coverage["old"]["ignored_by_design"] == {
        "IfcApplication": 1,
        "IfcOrganization": 1,
        "IfcOwnerHistory": 1,
        "IfcPerson": 1,
        "IfcPersonAndOrganization": 1,
    }


def test_a_bundle_cannot_be_built_without_its_coverage_account():
    with pytest.raises(TypeError, match="coverage"):
        SignatureBundle(
            filepath="model.ifc",
            schema="IFC4",
            canon_version="athar-canon-v14",
            signatures={},
            diagnostics=ParseDiagnostics(),
            edge_stats={},
        )


def test_object_placement_pointing_at_an_element_is_an_error_naming_the_records(tmp_path):
    base = tmp_path / "base.ifc"
    broken = tmp_path / "broken.ifc"
    _build().write(str(base))
    f = ifcopenshell.open(str(base))
    wall, proxy = _one(f, "IfcWall", Name="W1"), _one(f, "IfcBuildingElementProxy", Name="GP1")
    grid_placement = proxy.ObjectPlacement.id()
    text, count = re.subn(
        rf"(#{proxy.id()}=IFCBUILDINGELEMENTPROXY\(.*?)#{grid_placement}\b", rf"\g<1>#{wall.id()}", base.read_text()
    )
    assert count == 1
    broken.write_text(text)

    with pytest.raises(ValueError, match=rf"#{proxy.id()}\b.*#{wall.id()}\b.*IFCWALL"):
        diff_files(str(base), str(broken))


def test_relationship_that_links_nothing_is_counted_as_unreached(tmp_path):
    """A material association whose only related object is a dangling reference."""

    def write(path, name):
        f = _build()
        wall = _one(f, "IfcWall", Name="W3")
        f.create_entity("IfcRelAssociatesMaterial", _guid("loose"), None, name, None, [wall], _one(f, "IfcMaterial", Name="Spare"))
        f.write(str(path))
        text, count = re.subn(rf"(IFCRELASSOCIATESMATERIAL\('{_guid('loose')}',.*?)\(#{wall.id()}\)", r"\g<1>(#999999)", path.read_text())
        assert count == 1
        path.write_text(text)

    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    write(base, "Finish")
    write(edited, "Finish, revised")

    report = diff_files(str(base), str(edited))
    assert _modified(report) == {}
    coverage = report["stats"]["coverage"]
    assert coverage["old"]["unreached"]["IfcRelAssociatesMaterial"]["count"] == 1
    assert coverage["unexamined_changes"] == ["IfcRelAssociatesMaterial"]
