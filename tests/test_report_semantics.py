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


