"""Identity and movement as the report states them.

Every test builds a small model twice with ifcopenshell, edits the second
build, and reads the result of ``athar.engine.diff_files``.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import ifcopenshell
import ifcopenshell.guid
import pytest

from athar.engine import diff_files


def guid(name: str) -> str:
    return ifcopenshell.guid.compress(hashlib.md5(name.encode()).hexdigest())


class Model:
    """Project, building, storeys L1 and L2, walls W1 and W2 in L1, W3 in L2.

    Placements chain wall -> storey -> building. ``length_scale`` is the
    number of file length units per metre.
    """

    def __init__(self, length_prefix: str | None = None) -> None:
        self.file = f = ifcopenshell.file(schema="IFC4")
        self.scale = {"MILLI": 1000.0, None: 1.0}[length_prefix]
        self.named: dict[str, object] = {}
        self.containment: dict[str, object] = {}

        length = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE", Prefix=length_prefix)
        area = f.create_entity("IfcSIUnit", UnitType="AREAUNIT", Name="SQUARE_METRE")
        units = f.create_entity("IfcUnitAssignment", Units=[length, area])
        origin = self.axes(0.0, 0.0, 0.0)
        self.context = f.create_entity(
            "IfcGeometricRepresentationContext",
            ContextType="Model",
            CoordinateSpaceDimension=3,
            Precision=1e-5,
            WorldCoordinateSystem=origin,
        )
        project = f.create_entity(
            "IfcProject",
            GlobalId=guid("project"),
            Name="Project",
            RepresentationContexts=[self.context],
            UnitsInContext=units,
        )
        self.named["project"] = project

        building = self.spatial("IfcBuilding", "B", None, 0.0)
        self.aggregate(project, [building])
        storeys = [self.spatial("IfcBuildingStorey", "L1", building, 0.0), self.spatial("IfcBuildingStorey", "L2", building, 3.0)]
        self.aggregate(building, storeys)
        self.contain("L1", [self.wall("W1", "L1", 0.0), self.wall("W2", "L1", 5.0)])
        self.contain("L2", [self.wall("W3", "L2", 0.0)])

    def __getitem__(self, name: str):
        return self.named[name]

    def axes(self, x: float, y: float, z: float):
        f = self.file
        return f.create_entity(
            "IfcAxis2Placement3D",
            Location=f.create_entity("IfcCartesianPoint", Coordinates=(x * self.scale, y * self.scale, z * self.scale)),
        )

    def placement(self, relative_to, x: float, y: float, z: float):
        return self.file.create_entity(
            "IfcLocalPlacement",
            PlacementRelTo=relative_to.ObjectPlacement if relative_to is not None else None,
            RelativePlacement=self.axes(x, y, z),
        )

    def spatial(self, ifc_class: str, name: str, parent, z: float):
        entity = self.file.create_entity(
            ifc_class, GlobalId=guid(name), Name=name, ObjectPlacement=self.placement(parent, 0.0, 0.0, z)
        )
        self.named[name] = entity
        return entity

    def body(self, length: float = 4.0):
        f = self.file
        profile = f.create_entity(
            "IfcRectangleProfileDef",
            ProfileType="AREA",
            Position=f.create_entity(
                "IfcAxis2Placement2D", Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0))
            ),
            XDim=length * self.scale,
            YDim=0.2 * self.scale,
        )
        solid = f.create_entity(
            "IfcExtrudedAreaSolid",
            SweptArea=profile,
            Position=self.axes(0.0, 0.0, 0.0),
            ExtrudedDirection=f.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)),
            Depth=2.8 * self.scale,
        )
        shape = f.create_entity(
            "IfcShapeRepresentation",
            ContextOfItems=self.context,
            RepresentationIdentifier="Body",
            RepresentationType="SweptSolid",
            Items=[solid],
        )
        return f.create_entity("IfcProductDefinitionShape", Representations=[shape])

    def element(self, ifc_class: str, name: str, relative_to, x: float, **attributes):
        entity = self.file.create_entity(
            ifc_class,
            GlobalId=guid(name),
            Name=name,
            ObjectPlacement=self.placement(relative_to, x, 0.0, 0.0),
            **attributes,
        )
        self.named[name] = entity
        return entity

    def wall(self, name: str, storey: str, x: float):
        return self.element("IfcWall", name, self.named[storey], x, Representation=self.body())

    def rel(self, ifc_class: str, **attributes):
        index = len(self.file.by_type(ifc_class))
        return self.file.create_entity(ifc_class, GlobalId=guid(f"{ifc_class}{index}"), **attributes)

    def aggregate(self, whole, parts) -> None:
        self.rel("IfcRelAggregates", RelatingObject=whole, RelatedObjects=parts)

    def contain(self, storey: str, elements) -> None:
        self.containment[storey] = self.rel(
            "IfcRelContainedInSpatialStructure", RelatingStructure=self.named[storey], RelatedElements=elements
        )

    def contain_more(self, storey: str, elements) -> None:
        relation = self.containment[storey]
        relation.RelatedElements = [*relation.RelatedElements, *elements]

    def pset(self, name: str, values: dict[str, object], *, tag: str):
        f = self.file
        return f.create_entity(
            "IfcPropertySet",
            GlobalId=guid(f"pset {tag}"),
            Name=name,
            HasProperties=[
                f.create_entity("IfcPropertySingleValue", Name=key, NominalValue=value) for key, value in values.items()
            ],
        )

    def define(self, element, definition) -> None:
        self.rel("IfcRelDefinesByProperties", RelatedObjects=[element], RelatingPropertyDefinition=definition)

    def text(self, value: str):
        return self.file.create_entity("IfcLabel", value)

    def write(self, path: Path) -> Path:
        self.file.write(str(path))
        return path


def diff(tmp_path: Path, edit, *, setup=None, **model_arguments) -> dict:
    """Report for the model as ``setup`` leaves it against the same model after ``edit``."""
    paths = []
    for name, steps in (("old.ifc", (setup,)), ("new.ifc", (setup, edit))):
        model = Model(**model_arguments)
        for step in steps:
            if step is not None:
                step(model)
        paths.append(model.write(tmp_path / name))
    return diff_files(*paths)


def modified(report: dict, name: str) -> dict:
    [item] = [item for item in report["modified"] if item["new"]["guid"] == guid(name)]
    return item


def names(items: list[dict]) -> set[str]:
    return {(item.get("new") or item)["name"] for item in items}


def test_diff_files_accepts_path_objects(tmp_path):
    def rename(model):
        model["W1"].Description = "edited"

    report = diff(tmp_path, rename)
    assert names(report["modified"]) == {"W1"}
    assert report["audit"]["inputs"]["old"]["path"] == str(tmp_path / "old.ifc")


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root reads any file")
@pytest.mark.parametrize("broken", ["unreadable", "directory", "missing"])
@pytest.mark.parametrize("side", [0, 1])
def test_cli_reports_an_unreadable_input_as_an_error(tmp_path, broken, side):
    good = Model().write(tmp_path / "good.ifc")
    bad = tmp_path / "bad.ifc"
    if broken == "unreadable":
        Model().write(bad)
        bad.chmod(0)
    elif broken == "directory":
        bad.mkdir()
    arguments = [str(good), str(good)]
    arguments[side] = str(bad)

    try:
        result = subprocess.run([sys.executable, "-m", "athar", *arguments], capture_output=True, text=True)
    finally:
        if broken == "unreadable":
            bad.chmod(0o600)

    assert result.returncode == 1
    assert result.stdout == ""
    [line] = result.stderr.splitlines()
    assert line.startswith("Error: ") and str(bad) in line


def relation(item: dict, name: str) -> dict:
    [change] = [change for change in item["relations"] if change["relation"] == name]
    return change


def targets(change: dict) -> tuple[list[str], list[str]]:
    return [t["name"] for t in change["old"]], [t["name"] for t in change["new"]]


def test_recontained_element_names_old_and_new_container(tmp_path):
    def move(model):
        w1 = model["W1"]
        model.containment["L1"].RelatedElements = [model["W2"]]
        model.containment["L2"].RelatedElements = [model["W3"], w1]

    report = diff(tmp_path, move)

    assert names(report["modified"]) == {"W1", "L1", "L2"}
    w1 = modified(report, "W1")
    assert w1["aspects"]["relations"] == "changed"
    assert w1["aspects"]["placement"] == "unchanged"
    assert w1["change_scope"] == "intrinsic"
    change = relation(w1, "IfcRelContainedInSpatialStructure")
    assert change["role"] == "RelatingStructure"
    assert [(t["guid"], t["class"], t["name"]) for t in change["old"]] == [(guid("L1"), "IfcBuildingStorey", "L1")]
    assert [(t["guid"], t["class"], t["name"]) for t in change["new"]] == [(guid("L2"), "IfcBuildingStorey", "L2")]
    for storey in ("L1", "L2"):
        item = modified(report, storey)
        assert item["aspects"]["topology"] == "changed"
        assert item["aspects"]["relations"] == "unchanged"
        assert item["change_scope"] == "transitive"


def test_reaggregated_part_names_old_and_new_whole(tmp_path):
    def parts(model):
        for name in ("A1", "A2"):
            model.element("IfcElementAssembly", name, model["L1"], 0.0)
        model.contain_more("L1", [model["A1"], model["A2"]])
        part = model.element("IfcMember", "M", model["A1"], 0.0)
        model.named["parts A"] = model.rel("IfcRelAggregates", RelatingObject=model["A1"], RelatedObjects=[part])

    def move(model):
        model.file.remove(model["parts A"])
        model.rel("IfcRelAggregates", RelatingObject=model["A2"], RelatedObjects=[model["M"]])

    report = diff(tmp_path, move, setup=parts)

    change = relation(modified(report, "M"), "IfcRelAggregates")
    assert (change["role"], *targets(change)) == ("RelatingObject", ["A1"], ["A2"])
    assert {name: modified(report, name)["change_scope"] for name in ("A1", "A2")} == {"A1": "transitive", "A2": "transitive"}


def test_retyped_element_names_old_and_new_type(tmp_path):
    def types(model):
        f = model.file
        for name in ("T1", "T2"):
            model.named[name] = f.create_entity("IfcWallType", GlobalId=guid(name), Name=name, PredefinedType="STANDARD")
        model.named["typing"] = model.rel("IfcRelDefinesByType", RelatingType=model["T1"], RelatedObjects=[model["W1"], model["W2"]])

    def retype(model):
        model["typing"].RelatedObjects = [model["W2"]]
        model.rel("IfcRelDefinesByType", RelatingType=model["T2"], RelatedObjects=[model["W1"]])

    report = diff(tmp_path, retype, setup=types)

    assert names(report["modified"]) == {"W1"}
    change = relation(modified(report, "W1"), "IfcRelDefinesByType")
    assert change["role"] == "RelatingType"
    assert [(t["guid"], t["class"]) for t in change["old"]] == [(guid("T1"), "IfcWallType")]
    assert [(t["guid"], t["class"]) for t in change["new"]] == [(guid("T2"), "IfcWallType")]


def test_retyping_between_two_types_sharing_a_guid_is_reported(tmp_path):
    def types(model):
        f = model.file
        for name in ("T1", "T2"):
            model.named[name] = f.create_entity("IfcWallType", GlobalId=guid("T"), Name=name, PredefinedType="STANDARD")
        model.named["typing"] = model.rel("IfcRelDefinesByType", RelatingType=model["T1"], RelatedObjects=[model["W1"]])

    def retype(model):
        model["typing"].RelatingType = model["T2"]

    report = diff(tmp_path, retype, setup=types)

    change = relation(modified(report, "W1"), "IfcRelDefinesByType")
    assert targets(change) == (["T1"], ["T2"])


def test_type_with_a_regenerated_guid_is_the_same_type(tmp_path):
    def typed(model):
        model.named["T"] = model.file.create_entity("IfcWallType", GlobalId=guid("T"), Name="T", PredefinedType="STANDARD")
        model.rel("IfcRelDefinesByType", RelatingType=model["T"], RelatedObjects=[model["W1"]])

    def regenerate(model):
        model["T"].GlobalId = guid("T again")

    assert diff(tmp_path, regenerate, setup=typed)["modified"] == []


def test_rehosted_opening_and_refilled_door_name_their_targets(tmp_path):
    def openings(model):
        for name, wall in (("O1", "W1"), ("O2", "W2")):
            opening = model.element("IfcOpeningElement", name, model[wall], 1.0)
            model.named[f"void {name}"] = model.rel(
                "IfcRelVoidsElement", RelatingBuildingElement=model[wall], RelatedOpeningElement=opening
            )
        door = model.element("IfcDoor", "D", model["L1"], 1.0)
        model.contain_more("L1", [door])
        model.named["fill"] = model.rel("IfcRelFillsElement", RelatingOpeningElement=model["O1"], RelatedBuildingElement=door)

    def swap(model):
        model["void O1"].RelatingBuildingElement = model["W2"]
        model["void O2"].RelatingBuildingElement = model["W1"]
        model["fill"].RelatingOpeningElement = model["O2"]

    report = diff(tmp_path, swap, setup=openings)

    void = relation(modified(report, "O1"), "IfcRelVoidsElement")
    assert (void["role"], *targets(void)) == ("RelatingBuildingElement", ["W1"], ["W2"])
    fill = relation(modified(report, "D"), "IfcRelFillsElement")
    assert (fill["role"], *targets(fill)) == ("RelatingOpeningElement", ["O1"], ["O2"])


def test_renested_part_names_old_and_new_host(tmp_path):
    def nest(model):
        part = model.element("IfcDiscreteAccessory", "N", model["W1"], 0.0)
        model.named["nesting"] = model.rel("IfcRelNests", RelatingObject=model["W1"], RelatedObjects=[part])

    def move(model):
        model["nesting"].RelatingObject = model["W2"]

    report = diff(tmp_path, move, setup=nest)

    change = relation(modified(report, "N"), "IfcRelNests")
    assert (change["role"], *targets(change)) == ("RelatingObject", ["W1"], ["W2"])
    for wall in ("W1", "W2"):
        assert modified(report, wall)["aspects"]["relations"] == "unchanged"
        assert modified(report, wall)["change_scope"] == "transitive"


def test_same_relations_between_matched_entities_report_nothing(tmp_path):
    def renumber(model):
        model.file.create_entity("IfcCartesianPoint", Coordinates=(9.0, 9.0, 9.0))
        w3 = model["W3"]
        model.file.remove(model.containment["L2"])
        model.contain("L2", [w3])

    report = diff(tmp_path, renumber)
    assert report["modified"] == []


def raise_building(model):
    location = model["B"].ObjectPlacement.RelativePlacement.Location
    location.Coordinates = (0.0, 0.0, 0.5 * model.scale)


def test_children_of_a_moved_ancestor_move_with_it(tmp_path):
    report = diff(tmp_path, raise_building)

    assert names(report["modified"]) == {"B", "L1", "L2", "W1", "W2", "W3"}
    building = modified(report, "B")
    assert building["change_scope"] == "intrinsic"
    assert "moved_with" not in building
    for name in ("L1", "L2", "W1", "W2", "W3"):
        item = modified(report, name)
        assert item["aspects"]["placement"] == "changed"
        assert item["aspects"]["placement_delta_mm"] == (0.0, 0.0, 500.0)
        assert item["change_scope"] == "transitive"
        assert (item["moved_with"]["guid"], item["moved_with"]["class"]) == (guid("B"), "IfcBuilding")
    assert report["stats"]["modified_change_scope"] == {"intrinsic": 1, "transitive": 5, "mixed": 0}

    [cohort] = report["stats"]["placement_cohorts"]
    assert cohort["delta_mm"] == [0.0, 0.0, 500.0]
    assert cohort["count"] == 6
    assert cohort["root_cause"]["guid"] == guid("B")


def test_rotated_parent_and_three_children_share_one_motion(tmp_path):
    def children(model):
        for i, name in enumerate(("W1", "W2", "W3")):
            wall = model[name]
            wall.ObjectPlacement = model.placement(model["B"], 2.0 + i * 3, 1.0 + i, 0.0)
        for name in ("L1", "L2"):
            model[name].ObjectPlacement.PlacementRelTo = None
        model["B"].ObjectPlacement.RelativePlacement.Location.Coordinates = (3.0, -2.0, 0.0)

    def move(model):
        axes = model["B"].ObjectPlacement.RelativePlacement
        axes.Location.Coordinates = (4.0, 0.0, 0.5)
        axes.RefDirection = model.file.create_entity("IfcDirection", DirectionRatios=(0.0, 1.0, 0.0))

    report = diff(tmp_path, move, setup=children)
    [cohort] = report["stats"]["placement_cohorts"]
    assert cohort["count"] == 4
    assert cohort["root_cause"]["guid"] == guid("B")
    assert cohort["rotation_deg"] == pytest.approx(90.0)
    assert cohort["delta_mm"] == [1000.0, 2000.0, 500.0]
    assert len({tuple(item["aspects"]["placement_delta_mm"]) for item in report["modified"]}) == 4
    from athar.check import evaluate_report
    [violation] = evaluate_report(report, {"max_placement_cohort_share": 0.5})["violations"]
    assert violation["share"] == pytest.approx(4 / 6)
    assert violation["root_cause"]["guid"] == guid("B")


@pytest.mark.parametrize("angle,pivot", [(30.0, (2.0, -1.0)), (180.0, (0.0, 0.0))])
def test_baked_rotation_without_a_parent_shares_one_motion(tmp_path, angle, pivot):
    import math

    c, s = math.cos(math.radians(angle)), math.sin(math.radians(angle))

    def independent(model):
        for name, point in (("W1", (3.0, 2.0, 0.0)), ("W2", (7.0, -1.0, 0.0))):
            model[name].ObjectPlacement = model.placement(None, *point)
        model["W2"].ObjectPlacement.RelativePlacement.RefDirection = model.file.create_entity(
            "IfcDirection", DirectionRatios=(0.0, 1.0, 0.0)
        )

    def rotate(model):
        for name in ("W1", "W2"):
            axes = model[name].ObjectPlacement.RelativePlacement
            x, y, z = axes.Location.Coordinates
            px, py = pivot
            axes.Location.Coordinates = (c * (x - px) - s * (y - py) + px, s * (x - px) + c * (y - py) + py, z)
            direction = (-s, c, 0.0) if name == "W2" else (c, s, 0.0)
            axes.RefDirection = model.file.create_entity("IfcDirection", DirectionRatios=direction)

    [cohort] = diff(tmp_path, rotate, setup=independent)["stats"]["placement_cohorts"]
    assert cohort["count"] == 2
    assert cohort["rotation_deg"] == pytest.approx(angle, abs=0.001)
    assert cohort["delta_mm"] == ([-232.0, -1134.0, 0.0] if angle == 30 else [0.0, 0.0, 0.0])
    assert "root_cause" not in cohort


@pytest.mark.parametrize("rotation", [False, True])
def test_distinct_motions_do_not_form_a_cohort(tmp_path, rotation):
    def move(model):
        for name in ("W1", "W2"):
            axes = model[name].ObjectPlacement.RelativePlacement
            x, y, z = axes.Location.Coordinates
            axes.Location.Coordinates = (x + 1.0, y + 2.0, z)
        if rotation:
            model["W2"].ObjectPlacement.RelativePlacement.RefDirection = model.file.create_entity(
                "IfcDirection", DirectionRatios=(0.0, 1.0, 0.0)
            )
        else:
            model["W2"].ObjectPlacement.RelativePlacement.Location.Coordinates = (6.002, 2.0, 0.0)

    assert diff(tmp_path, move)["stats"]["placement_cohorts"] == []


def test_nearest_moved_ancestor_is_named(tmp_path):
    def raise_both(model):
        raise_building(model)
        model["L1"].ObjectPlacement.RelativePlacement.Location.Coordinates = (1.0 * model.scale, 0.0, 0.0)

    report = diff(tmp_path, raise_both)

    assert "moved_with" not in modified(report, "L1")
    assert modified(report, "L1")["change_scope"] == "intrinsic"
    assert modified(report, "W1")["moved_with"]["guid"] == guid("L1")
    assert modified(report, "W3")["moved_with"]["guid"] == guid("B")
    cohorts = {tuple(cohort["delta_mm"]): cohort for cohort in report["stats"]["placement_cohorts"]}
    assert cohorts[(1000.0, 0.0, 500.0)]["root_cause"]["guid"] == guid("L1")
    assert cohorts[(0.0, 0.0, 500.0)]["root_cause"]["guid"] == guid("B")


def test_elements_moved_one_by_one_share_no_root_cause(tmp_path):
    def shift(model):
        for name in ("W1", "W2"):
            location = model[name].ObjectPlacement.RelativePlacement.Location
            location.Coordinates = (location.Coordinates[0], 2.0 * model.scale, 0.0)

    report = diff(tmp_path, shift)

    assert {name: modified(report, name)["change_scope"] for name in ("W1", "W2")} == {"W1": "intrinsic", "W2": "intrinsic"}
    [cohort] = report["stats"]["placement_cohorts"]
    assert cohort["count"] == 2 and "root_cause" not in cohort


def test_placement_rehung_under_another_parent_is_intrinsic(tmp_path):
    def rehang(model):
        model["W1"].ObjectPlacement.PlacementRelTo = model["L2"].ObjectPlacement

    report = diff(tmp_path, rehang)

    w1 = modified(report, "W1")
    assert w1["aspects"]["placement_delta_mm"] == (0.0, 0.0, 3000.0)
    assert w1["change_scope"] == "intrinsic" and "moved_with" not in w1


def test_placement_without_an_owner_belongs_to_the_element_below_it(tmp_path):
    def intermediate(model):
        w1 = model["W1"]
        between = model.file.create_entity(
            "IfcLocalPlacement", PlacementRelTo=model["L1"].ObjectPlacement, RelativePlacement=model.axes(0.0, 0.0, 0.0)
        )
        w1.ObjectPlacement.PlacementRelTo = between
        model.named["between"] = between

    def shift(model):
        model["between"].RelativePlacement.Location.Coordinates = (0.0, 1.0 * model.scale, 0.0)

    report = diff(tmp_path, shift, setup=intermediate)

    assert names(report["modified"]) == {"W1"}
    w1 = modified(report, "W1")
    assert w1["change_scope"] == "intrinsic" and "moved_with" not in w1


def test_regenerated_guid_is_flagged_on_the_content_match(tmp_path):
    def regenerate(model):
        model["W1"].GlobalId = guid("W1 again")

    report = diff(tmp_path, regenerate)

    assert report["modified"] == [] and report["added"] == [] and report["deleted"] == []
    [item] = [item for item in report["unchanged"] if item["new"]["name"] == "W1"]
    assert item["match"]["reason"] == "geometry_hash"
    assert item["identity"] == {"guid_changed": True, "old_guid": guid("W1"), "new_guid": guid("W1 again")}
    assert report["stats"]["guid_changed"] == 1
    [listed] = report["guid_changed"]
    assert (listed["old"]["guid"], listed["new"]["guid"]) == (guid("W1"), guid("W1 again"))
    assert all("identity" not in other for other in report["unchanged"] if other is not item)


def test_unchanged_guids_are_not_flagged(tmp_path):
    report = diff(tmp_path, lambda model: None)
    assert report["stats"]["guid_changed"] == 0 and report["guid_changed"] == []


def test_regenerated_guid_with_a_changed_relation_is_modified_and_flagged(tmp_path):
    def regenerate_and_move(model):
        w1 = model["W1"]
        w1.GlobalId = guid("W1 again")
        model.containment["L1"].RelatedElements = [model["W2"]]
        model.containment["L2"].RelatedElements = [model["W3"], w1]

    report = diff(tmp_path, regenerate_and_move)

    item = modified(report, "W1 again")
    assert item["identity"]["old_guid"] == guid("W1")
    assert item["aspects"]["relations"] == "changed"
    assert report["stats"]["guid_changed"] == 1


def typed_wall(model, values: dict[str, object] | None):
    """Give W1 a type that defines ``Pset_X`` with ``values`` (no set when ``None``)."""
    sets = [] if values is None else [model.pset("Pset_X", values, tag="type")]
    model.named["T"] = model.file.create_entity(
        "IfcWallType", GlobalId=guid("T"), Name="T", PredefinedType="STANDARD", HasPropertySets=sets
    )
    model.rel("IfcRelDefinesByType", RelatingType=model["T"], RelatedObjects=[model["W1"]])


def test_value_moving_from_type_to_occurrence_is_a_value_and_an_origin_change(tmp_path):
    def setup(model):
        typed_wall(model, {"Fire": model.text("EI30")})

    def override(model):
        model.define(model["W1"], model.pset("Pset_X", {"Fire": model.text("EI60")}, tag="own"))

    deltas = modified(diff(tmp_path, override, setup=setup), "W1")["property_deltas"]

    [changed] = deltas["changed"]
    assert (changed["pset"], changed["name"], changed["old_value"], changed["new_value"]) == ("Pset_X", "Fire", "EI30", "EI60")
    assert changed["inherited"] is False
    assert changed["old_definitions"] == [{"origin": "type", "pset_guid": guid("pset type"), "value": "EI30"}]
    assert changed["new_definitions"] == [
        {"origin": "occurrence", "pset_guid": guid("pset own"), "value": "EI60"},
        {"origin": "type", "pset_guid": guid("pset type"), "value": "EI30"},
    ]
    [origin] = deltas["origin_changed"]
    assert (origin["name"], origin["old_origin"], origin["new_origin"]) == ("Fire", "type", "occurrence")
    assert "added" not in deltas and "removed" not in deltas


def test_occurrence_taking_over_the_type_value_is_an_origin_change_only(tmp_path):
    def setup(model):
        typed_wall(model, {"Fire": model.text("EI30")})

    def override(model):
        model.define(model["W1"], model.pset("Pset_X", {"Fire": model.text("EI30")}, tag="own"))

    deltas = modified(diff(tmp_path, override, setup=setup), "W1")["property_deltas"]

    assert "changed" not in deltas
    [origin] = deltas["origin_changed"]
    assert (origin["old_origin"], origin["new_origin"]) == ("type", "occurrence")


def test_overridden_type_value_change_leaves_the_effective_value(tmp_path):
    def setup(model):
        typed_wall(model, {"Fire": model.text("EI30")})
        model.define(model["W1"], model.pset("Pset_X", {"Fire": model.text("EI60")}, tag="own"))

    def edit_type(model):
        model["T"].HasPropertySets[0].HasProperties[0].NominalValue = model.text("EI90")

    deltas = modified(diff(tmp_path, edit_type, setup=setup), "W1")["property_deltas"]

    assert set(deltas) == {"overridden_changed"}
    [overridden] = deltas["overridden_changed"]
    assert (overridden["inherited"], overridden["old_value"], overridden["new_value"]) == (True, "EI30", "EI90")


def test_same_key_in_two_sets_keeps_both_definitions(tmp_path):
    def setup(model):
        model.define(model["W1"], model.pset("Pset_X", {"Fire": model.text("EI30")}, tag="first"))
        model.define(model["W1"], model.pset("Pset_X", {"Fire": model.text("EI60")}, tag="second"))

    def edit_first(model):
        [first] = [p for p in model.file.by_type("IfcPropertySet") if p.GlobalId == guid("pset first")]
        first.HasProperties[0].NominalValue = model.text("EI90")

    item = modified(diff(tmp_path, edit_first, setup=setup), "W1")

    [changed] = item["property_deltas"]["changed"]
    assert (changed["old_value"], changed["new_value"]) == (["EI30", "EI60"], ["EI60", "EI90"])
    assert sorted(changed["old_definitions"], key=lambda definition: definition["value"]) == [
        {"origin": "occurrence", "pset_guid": guid("pset first"), "value": "EI30"},
        {"origin": "occurrence", "pset_guid": guid("pset second"), "value": "EI60"},
    ]
    first = f"IfcPropertySet[Name=Pset_X,GlobalId={guid('pset first')}]/IfcPropertySingleValue.NominalValue"
    assert {"path": first, "old": "IFCLABEL: EI30", "new": "IFCLABEL: EI90"} in item["data_delta"]


def test_data_facts_name_the_type_and_set_a_property_comes_from(tmp_path):
    def setup(model):
        typed_wall(model, {"Fire": model.text("EI30")})

    def edit_type(model):
        model["T"].HasPropertySets[0].HasProperties[0].NominalValue = model.text("EI90")

    item = modified(diff(tmp_path, edit_type, setup=setup), "W1")
    path = "IfcWallType[Name=T]/IfcPropertySet[Name=Pset_X]/IfcPropertySingleValue.NominalValue"
    assert item["data_delta"] == [{"path": path, "old": "IFCLABEL: EI30", "new": "IFCLABEL: EI90"}]


def test_changed_value_in_a_set_with_a_regenerated_guid_is_one_data_delta_entry(tmp_path):
    def setup(model):
        values = {"FireRating": model.text("EI30"), "Reference": model.text("W-01"), "Status": model.text("New")}
        model.define(model["W1"], model.pset("Pset_WallCommon", values, tag="common"))

    def regenerate_and_edit(model):
        [pset] = model.file.by_type("IfcPropertySet")
        pset.GlobalId = guid("pset common again")
        [rating] = [p for p in pset.HasProperties if p.Name == "FireRating"]
        rating.NominalValue = model.text("EI90")

    item = modified(diff(tmp_path, regenerate_and_edit, setup=setup), "W1")

    assert item["data_delta"] == [
        {
            "path": "IfcPropertySet[Name=Pset_WallCommon]/IfcPropertySingleValue.NominalValue[1]",
            "old": "IFCLABEL: EI30",
            "new": "IFCLABEL: EI90",
        }
    ]


def test_changed_value_on_a_type_with_a_regenerated_guid_is_one_data_delta_entry(tmp_path):
    def setup(model):
        typed_wall(model, {"Fire": model.text("EI30"), "Reference": model.text("W-01")})

    def regenerate_and_edit(model):
        model["T"].GlobalId = guid("T again")
        model["T"].HasPropertySets[0].HasProperties[0].NominalValue = model.text("EI90")

    item = modified(diff(tmp_path, regenerate_and_edit, setup=setup), "W1")

    assert [(delta["old"], delta["new"]) for delta in item["data_delta"]] == [("IFCLABEL: EI30", "IFCLABEL: EI90")]
    assert item["data_delta"][0]["path"].startswith("IfcWallType[Name=T]/IfcPropertySet[Name=Pset_X]/")


def measured_wall(model):
    f = model.file
    model.define(
        model["W1"],
        model.pset(
            "Pset_X",
            {
                "Clearance": f.create_entity("IfcLengthMeasure", 3000.0),
                "Face": f.create_entity("IfcAreaMeasure", 2.5),
                "Ratio": f.create_entity("IfcReal", 0.25),
            },
            tag="measures",
        ),
    )
    quantities = f.create_entity(
        "IfcElementQuantity",
        GlobalId=guid("qto"),
        Name="Qto_X",
        Quantities=[f.create_entity("IfcQuantityLength", Name="Length", LengthValue=4000.0)],
    )
    model.define(model["W1"], quantities)
    model.named["quantities"] = quantities


def test_property_and_quantity_values_are_si_numbers_with_a_unit(tmp_path):
    def edit(model):
        [pset] = [p for p in model.file.by_type("IfcPropertySet") if p.GlobalId == guid("pset measures")]
        values = {prop.Name: prop for prop in pset.HasProperties}
        values["Clearance"].NominalValue = model.file.create_entity("IfcLengthMeasure", 3250.0)
        values["Face"].NominalValue = model.file.create_entity("IfcAreaMeasure", 3.5)
        values["Ratio"].NominalValue = model.file.create_entity("IfcReal", 0.5)
        model["quantities"].Quantities[0].LengthValue = 4500.0

    item = modified(diff(tmp_path, edit, setup=measured_wall, length_prefix="MILLI"), "W1")

    changed = {entry["name"]: entry for entry in item["property_deltas"]["changed"]}
    assert (changed["Clearance"]["old_value"], changed["Clearance"]["new_value"], changed["Clearance"]["unit"]) == (3.0, 3.25, "m")
    assert (changed["Length"]["old_value"], changed["Length"]["new_value"], changed["Length"]["unit"]) == (4.0, 4.5, "m")
    assert (changed["Face"]["old_value"], changed["Face"]["new_value"], changed["Face"]["unit"]) == (2.5, 3.5, "m2")
    assert (changed["Ratio"]["old_value"], changed["Ratio"]["new_value"]) == (0.25, 0.5)
    assert "unit" not in changed["Ratio"]

    facts = {(delta["old"], delta["new"]) for delta in item["data_delta"]}
    assert facts == {
        ("IFCLENGTHMEASURE: 3 m", "IFCLENGTHMEASURE: 3.25 m"),
        ("IFCAREAMEASURE: 2.5 m2", "IFCAREAMEASURE: 3.5 m2"),
        ("IFCREAL: 0.25", "IFCREAL: 0.5"),
        ("4 m", "4.5 m"),
    }


def single_value(model, value, unit=None):
    """Give W1 ``Pset_X.Measure`` with ``value``, optionally naming its own unit."""
    f = model.file
    prop = f.create_entity("IfcPropertySingleValue", Name="Measure", NominalValue=value, Unit=unit)
    model.define(model["W1"], f.create_entity("IfcPropertySet", GlobalId=guid("pset single"), Name="Pset_X", HasProperties=[prop]))
    model.named["measure"] = prop


def test_measure_without_a_declared_unit_is_as_written_and_unlabelled(tmp_path):
    def edit(model):
        model["measure"].NominalValue = model.file.create_entity("IfcVolumeMeasure", 3.0)

    setup = lambda model: single_value(model, model.file.create_entity("IfcVolumeMeasure", 2.0))
    item = modified(diff(tmp_path, edit, setup=setup, length_prefix="MILLI"), "W1")

    assert item["property_deltas"]["changed"] == [
        {"pset": "Pset_X", "name": "Measure", "inherited": False, "old_value": 2.0, "new_value": 3.0}
    ]


def test_measure_in_a_context_dependent_unit_is_as_written_and_unlabelled(tmp_path):
    def setup(model):
        f = model.file
        ell = f.create_entity(
            "IfcContextDependentUnit",
            Dimensions=f.create_entity("IfcDimensionalExponents", 1, 0, 0, 0, 0, 0, 0),
            UnitType="LENGTHUNIT",
            Name="ELL",
        )
        single_value(model, f.create_entity("IfcLengthMeasure", 2.0), ell)

    def edit(model):
        model["measure"].NominalValue = model.file.create_entity("IfcLengthMeasure", 3.0)

    item = modified(diff(tmp_path, edit, setup=setup), "W1")

    assert item["property_deltas"]["changed"] == [
        {"pset": "Pset_X", "name": "Measure", "inherited": False, "old_value": 2.0, "new_value": 3.0}
    ]


def test_file_level_context_dependent_unit_leaves_a_measure_as_written_and_unlabelled(tmp_path):
    def setup(model):
        f = model.file
        ell = f.create_entity(
            "IfcContextDependentUnit",
            Dimensions=f.create_entity("IfcDimensionalExponents", 1, 0, 0, 0, 0, 0, 0),
            UnitType="LENGTHUNIT",
            Name="ELL",
        )
        [assignment] = f.by_type("IfcUnitAssignment")
        assignment.Units = [unit for unit in assignment.Units if unit.UnitType != "LENGTHUNIT"] + [ell]
        single_value(model, f.create_entity("IfcLengthMeasure", 2.0))

    def edit(model):
        model["measure"].NominalValue = model.file.create_entity("IfcLengthMeasure", 3.0)

    item = modified(diff(tmp_path, edit, setup=setup), "W1")

    assert item["property_deltas"]["changed"] == [
        {"pset": "Pset_X", "name": "Measure", "inherited": False, "old_value": 2.0, "new_value": 3.0}
    ]


def test_value_whose_unit_changed_is_a_change_carrying_both_units(tmp_path):
    def retag(model):
        model["measure"].NominalValue = model.file.create_entity("IfcAreaMeasure", 3.0)

    def untag(model):
        model["measure"].NominalValue = model.file.create_entity("IfcReal", 3.0)

    setup = lambda model: single_value(model, model.file.create_entity("IfcLengthMeasure", 3.0))
    expected = {"pset": "Pset_X", "name": "Measure", "inherited": False, "old_value": 3.0, "new_value": 3.0, "old_unit": "m"}

    [changed] = modified(diff(tmp_path, retag, setup=setup), "W1")["property_deltas"]["changed"]
    assert changed == {**expected, "unit": "m2"}
    [changed] = modified(diff(tmp_path, untag, setup=setup), "W1")["property_deltas"]["changed"]
    assert changed == {**expected, "unit": None}


def test_placement_delta_is_in_millimetres_whatever_the_file_unit(tmp_path):
    report = diff(tmp_path, raise_building, length_prefix="MILLI")
    assert modified(report, "B")["aspects"]["placement_delta_mm"] == (0.0, 0.0, 500.0)


def test_renested_port_names_old_and_new_element(tmp_path):
    def ports(model):
        for name in ("P1", "P2"):
            model.element("IfcFlowSegment", name, model["L1"], 0.0)
        model.contain_more("L1", [model["P1"], model["P2"]])
        port = model.element("IfcDistributionPort", "Port", model["P2"], 0.0)
        model.named["nesting"] = model.rel("IfcRelNests", RelatingObject=model["P2"], RelatedObjects=[port])

    def renest(model):
        model["nesting"].RelatingObject = model["P1"]

    report = diff(tmp_path, renest, setup=ports)

    assert names(report["modified"]) == {"Port", "P1", "P2"}
    change = relation(modified(report, "Port"), "IfcRelNests")
    assert (change["role"], *targets(change)) == ("RelatingObject", ["P2"], ["P1"])
    for name in ("P1", "P2"):
        aspects = modified(report, name)["aspects"]
        assert {aspect for aspect, state in aspects.items() if state == "changed"} == {"topology"}
        assert modified(report, name)["change_scope"] == "transitive"


def test_retargeted_space_boundary_is_reported_on_the_space(tmp_path):
    def boundary(model):
        space = model.spatial("IfcSpace", "S", model["L1"], 0.0)
        model.aggregate(model["L1"], [space])
        model.named["boundary"] = model.rel(
            "IfcRelSpaceBoundary",
            RelatingSpace=space,
            RelatedBuildingElement=model["W1"],
            PhysicalOrVirtualBoundary="PHYSICAL",
            InternalOrExternalBoundary="INTERNAL",
        )

    def retarget(model):
        model["boundary"].RelatedBuildingElement = model["W2"]

    report = diff(tmp_path, retarget, setup=boundary)

    change = relation(modified(report, "S"), "IfcRelSpaceBoundary")
    assert (change["role"], *targets(change)) == ("RelatedBuildingElement", ["W1"], ["W2"])
    lost = relation(modified(report, "W1"), "IfcRelSpaceBoundary")
    assert (lost["role"], *targets(lost)) == ("RelatingSpace", ["S"], [])
    gained = relation(modified(report, "W2"), "IfcRelSpaceBoundary")
    assert (gained["role"], *targets(gained)) == ("RelatingSpace", [], ["S"])


def test_covering_moved_to_another_element_is_reported_on_the_covering(tmp_path):
    def covering(model):
        cover = model.element("IfcCovering", "C", model["L1"], 0.0)
        model.contain_more("L1", [cover])
        model.named["covers"] = model.rel(
            "IfcRelCoversBldgElements", RelatingBuildingElement=model["W1"], RelatedCoverings=[cover]
        )

    def move(model):
        model["covers"].RelatingBuildingElement = model["W2"]

    report = diff(tmp_path, move, setup=covering)

    change = relation(modified(report, "C"), "IfcRelCoversBldgElements")
    assert (change["role"], *targets(change)) == ("RelatingBuildingElement", ["W1"], ["W2"])
    assert {modified(report, wall)["change_scope"] for wall in ("W1", "W2")} == {"transitive"}



def reclassify(model, name: str, ifc_class: str):
    """Replace the element ``name`` by one of ``ifc_class`` with the same GlobalId, placement and shape."""
    old = model[name]
    new = model.file.create_entity(
        ifc_class,
        GlobalId=old.GlobalId,
        Name=old.Name,
        ObjectPlacement=old.ObjectPlacement,
        Representation=old.Representation,
    )
    for relation in model.file.by_type("IfcRelContainedInSpatialStructure"):
        if old in relation.RelatedElements:
            relation.RelatedElements = [new if element == old else element for element in relation.RelatedElements]
    model.file.remove(old)
    model.named[name] = new


def test_reclassified_element_keeps_its_identity(tmp_path):
    def proxy_to_wall(model):
        reclassify(model, "W1", "IfcBuildingElementProxy")

    report = diff(tmp_path, proxy_to_wall)

    assert report["added"] == [] and report["deleted"] == []
    item = modified(report, "W1")
    assert item["match"] == {"score": 0.7, "reason": "guid_reclassified"}
    assert item["class"] == {"old_class": "IfcWall", "new_class": "IfcBuildingElementProxy"}
    assert item["aspects"]["class"] == "changed"
    assert item["aspects"]["geometry"] == "unchanged"
    assert item["aspects"]["placement"] == "unchanged"
    assert item["aspects"]["relations"] == "unchanged"
    label = f"IfcBuildingElementProxy[GlobalId={guid('W1')}]"
    assert item["data_delta"] == [{"path": f"{label}.(class)", "old": "IfcWall", "new": "IfcBuildingElementProxy"}]
    assert report["stats"]["matcher_diagnostics"]["matched_by_tier"]["guid_reclassified"] == 1
    assert modified(report, "L1")["change_scope"] == "transitive"
    assert all(other["aspects"]["class"] == "unchanged" for other in report["unchanged"])


def test_reclassified_element_reports_what_else_differs(tmp_path):
    def reclassify_and_edit(model):
        reclassify(model, "W1", "IfcBuildingElementProxy")
        model["W1"].Name = "W1 corrected"
        model["W1"].ObjectPlacement.RelativePlacement.Location.Coordinates = (0.0, 1.0, 0.0)

    report = diff(tmp_path, reclassify_and_edit)

    [item] = [item for item in report["modified"] if item["new"]["guid"] == guid("W1")]
    assert item["aspects"]["class"] == "changed"
    assert item["aspects"]["placement_delta_mm"] == (0.0, 1000.0, 0.0)
    label = f"IfcBuildingElementProxy[GlobalId={guid('W1')}]"
    assert {"path": f"{label}.Name", "old": "W1", "new": "W1 corrected"} in item["data_delta"]


def test_classes_never_pair_without_the_same_unique_guid(tmp_path):
    def replace(model):
        reclassify(model, "W1", "IfcBuildingElementProxy")
        model["W1"].GlobalId = guid("another element")

    report = diff(tmp_path, replace)

    assert [item["class"] for item in report["added"]] == ["IfcBuildingElementProxy"]
    assert [item["class"] for item in report["deleted"]] == ["IfcWall"]


def twin_ports(model, order: tuple[str, str], *, port_guids: dict[str, str]):
    """Pipes A and B, each with a nested port of one name at one world position."""
    for name, x in (("A", 0.0), ("B", 2.0)):
        model.element("IfcFlowSegment", name, model["L1"], x)
    model.contain_more("L1", [model["A"], model["B"]])
    for name in order:
        port = model.file.create_entity(
            "IfcDistributionPort",
            GlobalId=port_guids[name],
            Name="Port",
            ObjectPlacement=model.placement(model["L1"], 1.0, 0.0, 0.0),
        )
        model.file.create_entity(
            "IfcRelNests", GlobalId=guid(f"nests {name}"), RelatingObject=model[name], RelatedObjects=[port]
        )


def test_equal_ports_written_in_another_order_stay_with_their_pipes(tmp_path):
    port_guids = {"A": guid("PA"), "B": guid("PB")}
    old = Model()
    twin_ports(old, ("A", "B"), port_guids=port_guids)
    new = Model()
    twin_ports(new, ("B", "A"), port_guids=port_guids)
    for entity in new.file.by_type("IfcRoot"):
        entity.GlobalId = guid(f"{entity.GlobalId} again")

    report = diff_files(old.write(tmp_path / "old.ifc"), new.write(tmp_path / "new.ifc"))

    assert report["modified"] == [] and report["added"] == [] and report["deleted"] == []
    assert report["stats"]["guid_changed"] == len(report["unchanged"]) > 0


def test_equal_ports_sharing_a_guid_stay_with_their_pipes(tmp_path):
    port_guids = {"A": guid("P"), "B": guid("P")}
    old = Model()
    twin_ports(old, ("A", "B"), port_guids=port_guids)
    new = Model()
    twin_ports(new, ("B", "A"), port_guids=port_guids)

    report = diff_files(old.write(tmp_path / "old.ifc"), new.write(tmp_path / "new.ifc"))

    assert report["modified"] == [] and report["added"] == [] and report["deleted"] == []
    assert report["guid_changed"] == []


def test_neighbours_of_a_deleted_element_changed_transitively(tmp_path):
    def hosted(model):
        port = model.element("IfcDistributionPort", "Port", model["W1"], 0.0)
        opening = model.element("IfcOpeningElement", "Opening", model["W1"], 0.0)
        model.named["links"] = [
            model.rel("IfcRelNests", RelatingObject=model["W1"], RelatedObjects=[port]),
            model.rel("IfcRelVoidsElement", RelatingBuildingElement=model["W1"], RelatedOpeningElement=opening),
        ]

    def delete_wall(model):
        model.containment["L1"].RelatedElements = [model["W2"]]
        for link in model["links"]:
            model.file.remove(link)
        model.file.remove(model["W1"])

    report = diff(tmp_path, delete_wall, setup=hosted)

    assert names(report["deleted"]) == {"W1"}
    assert targets(relation(modified(report, "Port"), "IfcRelNests")) == (["W1"], [])
    assert targets(relation(modified(report, "Opening"), "IfcRelVoidsElement")) == (["W1"], [])
    for name in ("Port", "Opening"):
        item = modified(report, name)
        assert item["aspects"]["relations"] == "changed" and item["change_scope"] == "transitive"
    assert report["stats"]["modified_change_scope"] == {"intrinsic": 0, "transitive": 3, "mixed": 0}


def test_element_under_a_replaced_storey_changed_transitively(tmp_path):
    def replace_storey(model):
        model["L2"].GlobalId = guid("L3")
        model["L2"].Name = "L3"

    report = diff(tmp_path, replace_storey)

    assert names(report["deleted"]) == {"L2"} and names(report["added"]) == {"L3"}
    wall = modified(report, "W3")
    assert targets(relation(wall, "IfcRelContainedInSpatialStructure")) == (["L2"], ["L3"])
    assert wall["change_scope"] == "transitive"


def test_space_taken_out_of_its_present_storey_changed_intrinsically(tmp_path):
    def space(model):
        model.named["space in storey"] = model.rel(
            "IfcRelAggregates", RelatingObject=model["L1"], RelatedObjects=[model.spatial("IfcSpace", "S", model["L1"], 0.0)]
        )

    def detach(model):
        model.file.remove(model["space in storey"])

    report = diff(tmp_path, detach, setup=space)

    space = modified(report, "S")
    assert targets(relation(space, "IfcRelAggregates")) == (["L1"], [])
    assert space["change_scope"] == "mixed" and space["aspects"]["topology"] == "changed"


def test_element_moved_from_a_deleted_storey_to_a_present_one_changed_intrinsically(tmp_path):
    def move_and_delete(model):
        w3 = model["W3"]
        w3.ObjectPlacement = model.placement(model["L1"], 0.0, 0.0, 3.0)
        model.file.remove(model.containment["L2"])
        model.contain_more("L1", [w3])
        building_parts = next(r for r in model.file.by_type("IfcRelAggregates") if r.RelatingObject == model["B"])
        building_parts.RelatedObjects = [model["L1"]]
        model.file.remove(model["L2"])

    report = diff(tmp_path, move_and_delete)

    assert names(report["deleted"]) == {"L2"}
    wall = modified(report, "W3")
    assert targets(relation(wall, "IfcRelContainedInSpatialStructure")) == (["L2"], ["L1"])
    assert wall["aspects"]["placement"] == "unchanged"
    assert wall["change_scope"] == "intrinsic"


def test_micro_difference_in_an_si_value_is_a_property_change(tmp_path):
    def edit(model):
        model["measure"].NominalValue = model.file.create_entity("IfcLengthMeasure", 1000000.000002)

    setup = lambda model: single_value(model, model.file.create_entity("IfcLengthMeasure", 1000000.000001))
    item = modified(diff(tmp_path, edit, setup=setup), "W1")

    assert item["property_deltas"]["changed"] == [
        {
            "pset": "Pset_X",
            "name": "Measure",
            "inherited": False,
            "old_value": 1000000.000001,
            "new_value": 1000000.000002,
            "unit": "m",
        }
    ]


def _ifc2x3_bounded_value(path, upper: float):
    f = ifcopenshell.file(schema="IFC2X3")
    metre = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")
    f.create_entity(
        "IfcProject",
        GlobalId=guid("project"),
        Name="Project",
        UnitsInContext=f.create_entity("IfcUnitAssignment", Units=[metre]),
    )
    wall = f.create_entity("IfcWall", GlobalId=guid("W1"), Name="W1")
    bounded = f.create_entity(
        "IfcPropertyBoundedValue",
        Name="Clearance",
        UpperBoundValue=f.create_entity("IfcLengthMeasure", upper),
        LowerBoundValue=f.create_entity("IfcLengthMeasure", 1.0),
        Unit=metre,
    )
    pset = f.create_entity("IfcPropertySet", GlobalId=guid("pset"), Name="Pset_X", HasProperties=[bounded])
    f.create_entity(
        "IfcRelDefinesByProperties", GlobalId=guid("rel"), RelatedObjects=[wall], RelatingPropertyDefinition=pset
    )
    f.write(str(path))
    return path


def test_ifc2x3_bounded_value_with_a_unit_diffs_and_compares_its_bounds(tmp_path):
    old = _ifc2x3_bounded_value(tmp_path / "old.ifc", 2.0)
    new = _ifc2x3_bounded_value(tmp_path / "new.ifc", 3.0)

    report = diff_files(old, new)

    assert names(report["modified"]) == {"W1"}
    assert diff_files(old, old)["modified"] == []
