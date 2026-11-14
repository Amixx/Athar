"""Rewrites that change no model content report nothing."""

from __future__ import annotations

import re

import ifcopenshell
import ifcopenshell.util.element
import pytest

from athar.engine import diff_files
from tests.test_change_coverage import _build, _extrusion, _material_profile_of_the_swept_area, _modified


def _explicit_default_axes(f):
    c = f.create_entity
    for placement in f.by_type("IfcAxis2Placement3D"):
        if placement.Axis is None and placement.RefDirection is None:
            placement.Axis = c("IfcDirection", (0.0, 0.0, 1.0))
            placement.RefDirection = c("IfcDirection", (1.0, 0.0, 0.0))
    for placement in f.by_type("IfcAxis2Placement2D"):
        if placement.RefDirection is None:
            placement.RefDirection = c("IfcDirection", (1.0, 0.0))
    for operator in f.by_type("IfcCartesianTransformationOperator3D"):
        assert operator.Axis1 is None and operator.Scale is None
        operator.Axis1 = c("IfcDirection", (1.0, 0.0, 0.0))
        operator.Axis2 = c("IfcDirection", (0.0, 1.0, 0.0))
        operator.Axis3 = c("IfcDirection", (0.0, 0.0, 1.0))
        operator.Scale = 1.0


def _directions_scaled(f):
    for direction in f.by_type("IfcDirection"):
        direction.DirectionRatios = tuple(2.0 * ratio for ratio in direction.DirectionRatios)


def _ref_direction_projected(f):
    """A RefDirection off the placement plane is the same frame as its projection."""
    placement = next(p for p in f.by_type("IfcAxis2Placement3D") if p.RefDirection is not None)
    assert placement.RefDirection.DirectionRatios == (3.0, 4.0, 0.0)
    placement.RefDirection.DirectionRatios = (3.0, 4.0, 7.0)
    position = _extrusion(f, "W1").Position
    position.Axis = f.create_entity("IfcDirection", (0.0, 0.0, 1.0))
    position.RefDirection = f.create_entity("IfcDirection", (1.0, 0.0, 5.0))


def _by_value(f, cls):
    groups: dict[tuple, list] = {}
    for entity in f.by_type(cls):
        groups.setdefault(tuple(entity), []).append(entity)
    return groups


def _points_and_directions_merged(f):
    merged = 0
    for cls in ("IfcCartesianPoint", "IfcDirection"):
        for keep, *duplicates in _by_value(f, cls).values():
            for duplicate in duplicates:
                for user in f.get_inverse(duplicate):
                    ifcopenshell.util.element.replace_attribute(user, duplicate, keep)
                f.remove(duplicate)
                merged += 1
    assert merged > 10


def _points_and_directions_split(f):
    seen: set[int] = set()
    split = 0

    def own_copy(value):
        nonlocal split
        if not isinstance(value, ifcopenshell.entity_instance) or not value.is_a() in ("IfcCartesianPoint", "IfcDirection"):
            return value
        if value.id() not in seen:
            seen.add(value.id())
            return value
        split += 1
        return f.create_entity(value.is_a(), *tuple(value))

    for entity in list(f):
        for index, value in enumerate(entity):
            if isinstance(value, tuple) and value and isinstance(value[0], ifcopenshell.entity_instance):
                entity[index] = [own_copy(item) for item in value]
            elif isinstance(value, ifcopenshell.entity_instance):
                entity[index] = own_copy(value)
    assert split > 0


def _representations_reversed(f):
    reversed_lists = 0
    for shape in f.by_type("IfcProductDefinitionShape"):
        if len(shape.Representations) > 1:
            shape.Representations = list(reversed(shape.Representations))
            reversed_lists += 1
    assert reversed_lists == 1


@pytest.mark.parametrize(
    "rewrite",
    [
        _explicit_default_axes,
        _directions_scaled,
        _ref_direction_projected,
        _points_and_directions_merged,
        _points_and_directions_split,
        _representations_reversed,
    ],
    ids=lambda rewrite: rewrite.__name__,
)
def test_rewrite_reports_nothing(tmp_path, rewrite):
    base = tmp_path / "base.ifc"
    twin = tmp_path / "twin.ifc"
    _build().write(str(base))
    f = ifcopenshell.open(str(base))
    rewrite(f)
    f.write(str(twin))

    report = diff_files(str(base), str(twin))
    assert _modified(report) == {}
    assert report["stats"]["coverage"]["unexamined_changes"] == []


def _with_dependency_cycle() -> ifcopenshell.file:
    """Two properties on different walls that each depend on the other."""
    f = _build()
    c = f.create_entity
    fire = next(p for p in f.by_type("IfcPropertySingleValue") if p.Name == "FireRating")
    acoustic = c("IfcPropertySingleValue", "AcousticRating", None, c("IfcLabel", "R50"), None)
    pset = c("IfcPropertySet", "pset2".ljust(22, "0"), None, "Pset_WallCommon", None, [acoustic])
    w2 = next(w for w in f.by_type("IfcWall") if w.Name == "W2")
    c("IfcRelDefinesByProperties", "relpset2".ljust(22, "0"), None, None, None, [w2], pset)
    c("IfcPropertyDependencyRelationship", None, None, fire, acoustic, None)
    c("IfcPropertyDependencyRelationship", None, None, acoustic, fire, None)
    return f


def _renumbered_in_reverse_order(source, target):
    lines = source.read_text().splitlines()
    start = lines.index("DATA;") + 1
    end = lines.index("ENDSEC;", start)
    records = [re.sub(r"#(\d+)", lambda m: f"#{100000 - int(m.group(1))}", line) for line in lines[start:end]]
    target.write_text("\n".join(lines[:start] + records[::-1] + lines[end:]) + "\n")


def test_include_cycle_hashes_do_not_depend_on_record_order(tmp_path):
    base = tmp_path / "base.ifc"
    twin = tmp_path / "twin.ifc"
    _with_dependency_cycle().write(str(base))
    _renumbered_in_reverse_order(base, twin)

    report = diff_files(str(base), str(twin))
    assert report["stats"]["old_diagnostics"]["cycle_breaks"] == 1
    assert _modified(report) == {}
    assert report["stats"]["coverage"]["unexamined_changes"] == []


def test_change_inside_an_include_cycle_reaches_every_owner(tmp_path):
    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    _with_dependency_cycle().write(str(base))
    f = ifcopenshell.open(str(base))
    next(p for p in f.by_type("IfcPropertySingleValue") if p.Name == "FireRating").NominalValue = f.create_entity("IfcLabel", "EI30")
    f.write(str(edited))

    assert _modified(diff_files(str(base), str(edited))) == {"W1": {"data"}, "W2": {"data"}}


def test_member_named_twice_in_a_set_is_one_membership(tmp_path):
    base = tmp_path / "base.ifc"
    twin = tmp_path / "twin.ifc"
    _build().write(str(base))
    text, count = re.subn(r"(IFCPRESENTATIONLAYERASSIGNMENT\('A-WALL',\$,\()(#\d+)", r"\1\2,\2", base.read_text())
    assert count == 1
    twin.write_text(text)

    report = diff_files(str(base), str(twin))
    assert _modified(report) == {}


def _value_identical_set_members_added(f):
    shell = f.by_type("IfcClosedShell")[0]
    shell.CfsFaces = list(shell.CfsFaces) + [ifcopenshell.util.element.copy_deep(f, shell.CfsFaces[0])]
    styled = f.by_type("IfcStyledItem")[0]
    styled.Styles = list(styled.Styles) + [ifcopenshell.util.element.copy_deep(f, styled.Styles[0])]


@pytest.mark.parametrize("direction", ["split", "merge"])
def test_value_identical_members_of_a_set_are_one_member(tmp_path, direction):
    single = tmp_path / "single.ifc"
    doubled = tmp_path / "doubled.ifc"
    _build().write(str(single))
    f = ifcopenshell.open(str(single))
    _value_identical_set_members_added(f)
    f.write(str(doubled))
    assert len(ifcopenshell.open(str(doubled)).by_type("IfcFace")) == 2

    old, new = (single, doubled) if direction == "split" else (doubled, single)
    report = diff_files(str(old), str(new))
    assert _modified(report) == {}
    assert report["stats"]["coverage"]["unexamined_changes"] == []


def test_set_member_with_different_content_is_another_member(tmp_path):
    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    f = _build()
    _value_identical_set_members_added(f)
    f.write(str(base))
    f = ifcopenshell.open(str(base))
    f.by_type("IfcStyledItem")[0].Styles[1].Name = "Render"
    f.write(str(edited))

    assert _modified(diff_files(str(base), str(edited))) == {"W2": {"data"}}


def test_member_repeated_in_a_list_is_content(tmp_path):
    def repeat_point(f):
        polyline = f.by_type("IfcPolyline")[0]
        points = list(polyline.Points)
        polyline.Points = points[:2] + [points[1]] + points[2:]

    def repeat_representation(f):
        shape = next(w for w in f.by_type("IfcWall") if w.Name == "W1").Representation
        shape.Representations = list(shape.Representations) * 2

    base = tmp_path / "base.ifc"
    twin = tmp_path / "twin.ifc"
    _build().write(str(base))
    f = ifcopenshell.open(str(base))
    repeat_point(f)
    repeat_representation(f)
    f.write(str(twin))

    modified = _modified(diff_files(str(base), str(twin)))
    assert "geometry" in modified["W3"]
    assert "geometry" in modified["W1"]


@pytest.mark.parametrize("direction", ["merged", "split"])
def test_material_profile_copy_of_the_swept_area_merged_or_split_reports_nothing(tmp_path, direction):
    paths = {}
    for name, shared in (("shared", True), ("copied", False)):
        f = _build()
        swept = _extrusion(f, "W3").SweptArea
        _material_profile_of_the_swept_area(f, "W3", swept if shared else ifcopenshell.util.element.copy_deep(f, swept))
        paths[name] = str(tmp_path / f"{name}.ifc")
        f.write(paths[name])
    old, new = (paths["copied"], paths["shared"]) if direction == "merged" else (paths["shared"], paths["copied"])

    report = diff_files(old, new)
    assert _modified(report) == {}
    for item in report["modified"] + report["unchanged"]:
        assert (item["aspects"]["data"] == "unchanged") == (item["data_hash"]["old"] == item["data_hash"]["new"])


def _with_placement_cycle() -> ifcopenshell.file:
    """W2 and W3 placed on two local placements that are each relative to the other."""
    f = _build()
    first, second = (next(w for w in f.by_type("IfcWall") if w.Name == name).ObjectPlacement for name in ("W2", "W3"))
    first.PlacementRelTo = second
    second.PlacementRelTo = first
    return f


def test_placement_cycle_is_unresolved_whatever_the_record_order(tmp_path):
    base = tmp_path / "base.ifc"
    twin = tmp_path / "twin.ifc"
    _with_placement_cycle().write(str(base))
    _renumbered_in_reverse_order(base, twin)

    report = diff_files(str(base), str(twin))
    assert _modified(report) == {}
    in_cycle = {item["new"]["name"]: item["new"] for item in report["unchanged"] if item["new"]["name"] in ("W2", "W3")}
    assert in_cycle.keys() == {"W2", "W3"}
    assert all(summary.get("placement_unresolved") is True for summary in in_cycle.values())


def test_change_inside_a_placement_cycle_reaches_every_member(tmp_path):
    base = tmp_path / "base.ifc"
    edited = tmp_path / "edited.ifc"
    _with_placement_cycle().write(str(base))
    f = ifcopenshell.open(str(base))
    next(w for w in f.by_type("IfcWall") if w.Name == "W3").ObjectPlacement.RelativePlacement.Location.Coordinates = (0.0, 7.0, 0.0)
    f.write(str(edited))

    assert _modified(diff_files(str(base), str(edited))) == {"W2": {"placement"}, "W3": {"placement"}}
