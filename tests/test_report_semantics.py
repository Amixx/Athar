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


