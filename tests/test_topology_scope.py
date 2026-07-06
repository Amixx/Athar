"""Change-scope contracts of the canon-v5 WL topology semantics.

`vh_topology` answers "did the class-multiset of my direct relationship
neighborhood change" (see athar/bottom/AGENTS.md). Two consequences are pinned
here with constructed spatial models:

- Re-containment between two same-class containers with identical placement is
  invisible on the element itself and surfaces on both containers instead.
  This is the documented blind spot of class-only seeds: the element's direct
  neighborhood is `{IfcBuildingStorey}` before and after.
- Re-containment into a different-class container plus a data edit on the same
  element is the positive `mixed` case: intrinsic data change plus a genuine
  neighborhood-structure change on one entity.
"""

from __future__ import annotations

import ifcopenshell

from athar.engine import diff_files
from tests.corpus import assert_report_invariants

_VICTIM = "0victimvictimvictim001"
_SIBLING_A = "0siblingsiblingsibl00a"
_SIBLING_B = "0siblingsiblingsibl00b"
_STOREY_A = "0storeystoreystorey00a"
_STOREY_B = "0storeystoreystorey00b"
_SPACE = "0spacespacespacespac01"
_BUILDING = "0buildingbuildingbld01"


def _build(path: str, *, victim_container: str, victim_name: str) -> None:
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
    project = f.create_entity(
        "IfcProject",
        GlobalId="0projectprojectprojec3",
        Name="topology-scope",
        UnitsInContext=units,
        RepresentationContexts=[context],
    )
    building = f.create_entity("IfcBuilding", GlobalId=_BUILDING, Name="building")
    storey_a = f.create_entity("IfcBuildingStorey", GlobalId=_STOREY_A, Name="storey-a")
    storey_b = f.create_entity("IfcBuildingStorey", GlobalId=_STOREY_B, Name="storey-b")
    space = f.create_entity("IfcSpace", GlobalId=_SPACE, Name="space")

    def placement(x: float) -> object:
        return f.create_entity(
            "IfcLocalPlacement",
            RelativePlacement=f.create_entity(
                "IfcAxis2Placement3D",
                Location=f.create_entity("IfcCartesianPoint", Coordinates=(x, 0.0, 0.0)),
            ),
        )

    victim = f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_VICTIM,
        Name=victim_name,
        ObjectPlacement=placement(1.0),
    )
    sibling_a = f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_SIBLING_A,
        Name="sibling-a",
        ObjectPlacement=placement(2.0),
    )
    sibling_b = f.create_entity(
        "IfcBuildingElementProxy",
        GlobalId=_SIBLING_B,
        Name="sibling-b",
        ObjectPlacement=placement(3.0),
    )

    f.create_entity(
        "IfcRelAggregates",
        GlobalId="0relaggprojectbuildin1",
        RelatingObject=project,
        RelatedObjects=[building],
    )
    f.create_entity(
        "IfcRelAggregates",
        GlobalId="0relaggbuildingstorey1",
        RelatingObject=building,
        RelatedObjects=[storey_a, storey_b],
    )
    f.create_entity(
        "IfcRelAggregates",
        GlobalId="0relaggstoreyaspace001",
        RelatingObject=storey_a,
        RelatedObjects=[space],
    )

    containment = {"storey_a": storey_a, "storey_b": storey_b, "space": space}[victim_container]
    contained = {storey_a: [sibling_a], storey_b: [sibling_b], space: []}
    contained[containment].append(victim)
    for index, (structure, elements) in enumerate(contained.items()):
        if not elements:
            continue
        f.create_entity(
            "IfcRelContainedInSpatialStructure",
            GlobalId=f"0relcontainstructure{index:02d}",
            RelatingStructure=structure,
            RelatedElements=elements,
        )
    f.write(path)


def _by_guid(section: list[dict], guid: str) -> dict | None:
    for item in section:
        key = item.get("new") or item
        if key.get("guid") == guid:
            return item
    return None


def test_same_class_recontainment_surfaces_on_containers_not_element(tmp_path):
    old = str(tmp_path / "contained-a.ifc")
    new = str(tmp_path / "contained-b.ifc")
    _build(old, victim_container="storey_a", victim_name="victim")
    _build(new, victim_container="storey_b", victim_name="victim")

    report = diff_files(old, new)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0

    # Documented canon-v5 blind spot: the moved element's direct neighborhood
    # is one IfcBuildingStorey before and after, so it reports unchanged.
    assert _by_guid(report["unchanged"], _VICTIM) is not None

    for storey_guid in (_STOREY_A, _STOREY_B):
        item = _by_guid(report["modified"], storey_guid)
        assert item is not None, storey_guid
        assert item["change_scope"] == "transitive", item
        assert item["aspects"]["topology"] == "changed"
    assert _by_guid(report["unchanged"], _BUILDING) is not None
    for sibling_guid in (_SIBLING_A, _SIBLING_B):
        assert _by_guid(report["unchanged"], sibling_guid) is not None


def test_data_edit_plus_recontainment_is_mixed(tmp_path):
    old = str(tmp_path / "in-storey.ifc")
    new = str(tmp_path / "in-space.ifc")
    _build(old, victim_container="storey_a", victim_name="victim")
    _build(new, victim_container="space", victim_name="victim [renamed]")

    report = diff_files(old, new)
    assert_report_invariants(report)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0

    item = _by_guid(report["modified"], _VICTIM)
    assert item is not None
    assert item["aspects"]["data"] == "changed"
    assert item["aspects"]["topology"] == "changed"
    assert item["aspects"]["geometry"] == "unchanged"
    assert item["change_scope"] == "mixed"
