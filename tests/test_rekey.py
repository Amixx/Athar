import json
import os
import re
import subprocess
import sys
import time

import ifcopenshell
import pytest

from athar.engine import SchemaMismatchError, diff_files
from athar.rekey import RekeyRefused, _ifcmerge_splits, rekey_files


def model(path, offset=0, moved=False, schema="IFC4", extra=False):
    f = ifcopenshell.file(schema=schema)

    def entity(cls, number, **attrs):
        return f.create_entity(cls, id=number + offset, **attrs)

    point = entity("IfcCartesianPoint", 10, Coordinates=(7.0 if moved else 1.0, 2.0, 3.0))
    axis = entity("IfcAxis2Placement3D", 20, Location=point)
    placement = entity("IfcLocalPlacement", 30, RelativePlacement=axis)
    for number, guid in ((40, "0000000000000000000001"), (50, "0000000000000000000002")):
        entity("IfcWall", number, GlobalId=guid, Name=f"wall #{number}'s", ObjectPlacement=placement)
    prop = entity("IfcPropertySingleValue", 60, Name="Rating", NominalValue=f.create_entity("IfcLabel", "B" if moved else "A"))
    second = entity("IfcPropertySingleValue", 61, Name="Unchanged", NominalValue=f.create_entity("IfcReal", 2.5))
    pset = entity("IfcPropertySet", 70, GlobalId="0000000000000000000003", Name="Pset_Test", HasProperties=(second, prop) if moved else (prop, second))
    entity("IfcRelDefinesByProperties", 80, GlobalId="0000000000000000000004", RelatedObjects=(f.by_id(40 + offset),), RelatingPropertyDefinition=pset)
    for number in (90, 100):
        entity("IfcCartesianPoint", number, Coordinates=(9.0, 8.0, 7.0))
    if extra:
        entity("IfcCartesianPoint", 110, Coordinates=(11.0, 12.0, 13.0))
    f.write(str(path))
    return f


@pytest.mark.parametrize("schema", ["IFC4", "IFC2X3"])
def test_rekey_changed_roles_shared_resources_duplicates_and_fresh_ids(tmp_path, schema):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base, schema=schema)
    source = model(new, 1000, moved=True, schema=schema, extra=True)
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert stats["fresh"] == 1
    assert stats["matched"] == len(list(source)) - 1
    assert restored.by_id(10).Coordinates == (7.0, 2.0, 3.0)
    assert restored.by_id(30).RelativePlacement.Location.id() == 10
    assert restored.by_id(40).ObjectPlacement.id() == restored.by_id(50).ObjectPlacement.id() == 30
    assert restored.by_id(60).NominalValue.wrappedValue == "B"
    assert [e.id() for e in restored.by_id(70).HasProperties] == [61, 60]
    assert restored.by_id(40).Name == "wall #40's"
    assert restored.by_id(90).Coordinates == restored.by_id(100).Coordinates == (9.0, 8.0, 7.0)
    assert restored.by_id(101).Coordinates == (11.0, 12.0, 13.0)
    report = diff_files(str(new), str(out))
    assert [report["stats"][s] for s in ("added", "deleted", "modified")] == [0, 0, 0]
    again = tmp_path / "again.ifc"
    rekey_files(str(base), str(new), str(again))
    assert out.read_bytes() == again.read_bytes()


def test_rekey_cli_and_input_protection(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    model(new, 300)
    result = subprocess.run([sys.executable, "-m", "athar", "rekey", str(base), str(new), "-o", str(out)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["fresh"] == 0
    before = base.read_bytes()
    with pytest.raises(ValueError, match="overwrite"):
        rekey_files(str(base), str(new), str(base))
    assert base.read_bytes() == before
    model(new, schema="IFC2X3")
    with pytest.raises(SchemaMismatchError):
        rekey_files(str(base), str(new), str(out))


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root reads any file")
@pytest.mark.parametrize("broken", ["unreadable", "directory"])
@pytest.mark.parametrize("side", [0, 1])
def test_rekey_cli_reports_an_unreadable_input_as_an_error(tmp_path, broken, side):
    good, bad, out = tmp_path / "good.ifc", tmp_path / "bad.ifc", tmp_path / "out.ifc"
    model(good)
    if broken == "unreadable":
        model(bad)
        bad.chmod(0)
    else:
        bad.mkdir()
    inputs = [str(good), str(good)]
    inputs[side] = str(bad)

    try:
        result = subprocess.run(
            [sys.executable, "-m", "athar", "rekey", *inputs, "-o", str(out)], capture_output=True, text=True
        )
    finally:
        if broken == "unreadable":
            bad.chmod(0o600)

    assert result.returncode == 1
    assert result.stdout == ""
    [line] = result.stderr.splitlines()
    assert line.startswith("Error: ") and str(bad) in line
    assert not out.exists()


def test_rekey_cycles_and_replaced_root_do_not_break_references(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    original = model(base)
    original.by_id(40).Name = "Original wall"
    original.write(str(base))
    f = model(new, 1000)
    f.by_id(1040).GlobalId = "0000000000000000000009"
    f.by_id(1040).Name = "Replacement wall"
    placement = f.by_id(1030)
    placement.PlacementRelTo = placement
    f.write(str(new))
    with pytest.raises(ValueError, match="unmatched rooted"):
        rekey_files(str(base), str(new), str(out))
    assert not out.exists()
    f.by_id(1040).GlobalId = "0000000000000000000001"
    f.write(str(new))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert restored.by_guid("0000000000000000000001").Name == "Replacement wall"
    p = restored.by_guid("0000000000000000000002").ObjectPlacement
    assert p.PlacementRelTo.id() == p.id()
    assert stats["cyclic_content_unavailable"]["new"] > 0


def test_rekey_preserves_lexical_values_and_recovers_regenerated_pset_guids(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    f = model(new, 1000, moved=True)
    f.by_id(1070).GlobalId = "0000000000000000000007"
    f.by_id(1080).GlobalId = "0000000000000000000008"
    f.write(str(new))
    encoded = b"IFCLABEL('B\\X\\0D\\X\\0A#123 '' quoted')"
    data = new.read_bytes().replace(b"IFCLABEL('B')", encoded)
    data = data.replace(b"((7.,2.,3.));\n", b"((7.000E+000,2.,3.));\n/* #999 is a comment */\n")
    new.write_bytes(data.replace(b"\n", b"\r\n"))
    rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert restored.by_id(70).GlobalId == "0000000000000000000007"
    assert restored.by_id(80).RelatingPropertyDefinition.id() == 70
    assert restored.by_id(60).NominalValue.wrappedValue == "B\r\n#123 ' quoted"
    assert encoded in out.read_bytes()
    assert b"7.000E+000,2." in out.read_bytes()
    assert b"/* #999 is a comment */" in out.read_bytes()
    report = diff_files(str(new), str(out))
    assert all(report["stats"][s] == 0 for s in ("added", "deleted", "modified"))


def test_rekey_orphan_psets_match_content_not_regenerated_guid(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, guid in ((base, 0, "0000000000000000000010"), (new, 1000, "0000000000000000000020")):
        f = model(path, offset)
        prop = f.create_entity("IfcPropertySingleValue", id=140 + offset, Name="Orphan value", NominalValue=f.create_entity("IfcInteger", 17))
        f.create_entity("IfcPropertySet", id=150 + offset, GlobalId=guid, Name="Unattached", HasProperties=(prop,))
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert stats["fresh"] == 0
    assert restored.by_id(150).GlobalId == "0000000000000000000020"
    assert restored.by_id(150).HasProperties[0].id() == 140


def test_rekey_role_fallback_scales_with_many_unmatched_resources(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    count = 1500
    for path, offset, shift in ((base, 0, 0.0), (new, 5000, 0.5)):
        f = model(path, offset)
        for k in range(count):
            f.create_entity("IfcCartesianPoint", id=1000 + offset + k, Coordinates=(k + shift, 0.0, 0.0))
        f.write(str(path))
    start = time.monotonic()
    stats = rekey_files(str(base), str(new), str(out))
    assert time.monotonic() - start < 10
    assert stats["fresh"] == count


@pytest.mark.parametrize("empty", [False, True])
def test_rekey_refuses_new_rooted_additions_without_publishing_output(tmp_path, empty):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    f = model(new, 1000)
    prop = f.create_entity("IfcPropertySingleValue", Name="Frame Type", NominalValue=f.create_entity("IfcLabel", "Test frame type"))
    pset = f.create_entity("IfcPropertySet", GlobalId="0000000000000000000011", Name="Construction", HasProperties=() if empty else (prop,))
    f.create_entity("IfcRelDefinesByProperties", GlobalId="0000000000000000000012", RelatedObjects=(f.by_id(1040),), RelatingPropertyDefinition=pset)
    f.write(str(new))
    out.write_bytes(b"existing output")
    with pytest.raises(ValueError, match="unmatched rooted"):
        rekey_files(str(base), str(new), str(out))
    assert out.read_bytes() == b"existing output"
    result = subprocess.run([sys.executable, "-m", "athar", "rekey", str(base), str(new), "-o", str(out)], capture_output=True, text=True)
    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert report["status"] == "refused"
    assert report["unsafe_rooted_additions"] == [pset.id(), pset.id() + 1]
    assert out.read_bytes() == b"existing output"


@pytest.mark.parametrize("edit", ["triangles", "closed", "quoted name", "split record", "coordinates", "plain name"])
def test_rekey_refuses_changes_ifcmerge_cannot_split_into_attributes(tmp_path, edit):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset in ((base, 0), (new, 1000)):
        f = model(path, offset)
        points = f.create_entity("IfcCartesianPointList3D", id=200 + offset, CoordList=((0., 0., 0.), (2., 0., 0.), (0., 3., 0.), (0., 0., 4.)))
        f.create_entity("IfcTriangulatedFaceSet", id=210 + offset, Coordinates=points, Closed=False, CoordIndex=((1, 2, 3), (1, 3, 4)))
        f.write(str(path))
    if edit == "triangles":
        f.by_id(1210).CoordIndex = ((1, 3, 4), (1, 2, 3))
    elif edit == "closed":
        f.by_id(1210).Closed = True
    elif edit == "quoted name":
        f.by_id(1040).Description = "edited"
    elif edit == "coordinates":
        f.by_id(1200).CoordList = ((0., 0., 0.), (2., 0., 0.), (0., 3., 0.), (0., 0., 5.))
    else:
        f.by_id(1070).Name = "Pset_Edited"
    f.write(str(new))
    if edit == "split record":
        new.write_bytes(new.read_bytes().replace(b"((1.,2.,3.))", b"((1.,\n2.,3.))"))
    out.write_bytes(b"existing output")
    if edit in ("coordinates", "plain name"):
        assert rekey_files(str(base), str(new), str(out))["fresh"] == 0
        return
    reason = "not alone on one line" if edit == "split record" else "cannot split into attributes"
    with pytest.raises(RekeyRefused, match=reason) as refused:
        rekey_files(str(base), str(new), str(out))
    diagnostics = refused.value.diagnostics
    if edit == "split record":
        assert diagnostics["unmergeable_changes"] == []
        assert diagnostics["records_off_their_line"] == {"base": [], "new": [1010]}
    else:
        assert diagnostics["unmergeable_changes"] == [1040 if edit == "quoted name" else 1210]
        assert diagnostics["records_off_their_line"] == {"base": [], "new": []}
    assert out.read_bytes() == b"existing output"


def test_rekey_pairs_equal_content_twins_through_their_matched_parents(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, (first, second) in ((base, 0, (300, 310)), (new, 1000, (310, 300))):
        f = model(path, offset)
        for number, point, x in ((320, first, 1.0), (330, second, 0.0)):
            location = f.create_entity("IfcCartesianPoint", id=point + offset, Coordinates=(5.0, 5.0, 5.0))
            direction = f.create_entity("IfcDirection", id=number + 1 + offset, DirectionRatios=(x, 1.0, 0.0))
            f.create_entity("IfcAxis2Placement3D", id=number + offset, Location=location, RefDirection=direction)
        f.write(str(path))
    rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert restored.by_id(320).Location.id() == 300
    assert restored.by_id(330).Location.id() == 310


@pytest.mark.parametrize(("members", "splits"), [(16383, True), (16384, False)])
def test_ifcmerge_splits_stops_at_perls_group_repetition_cap(members, splits):
    related = b",".join(b"#%d" % i for i in range(10, 10 + members))
    record = b"IFCRELCONTAINEDINSPATIALSTRUCTURE('0000000000000000000005',$,$,$,(" + related + b"),#1)"
    assert _ifcmerge_splits(record, 6) is splits


def test_rekey_refuses_fresh_records_spanning_lines(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    model(new, 1000, extra=True)
    new.write_bytes(new.read_bytes().replace(b"(11.,12.,13.)", b"(11.,\n12.,13.)"))
    with pytest.raises(RekeyRefused, match="not alone on one line") as refused:
        rekey_files(str(base), str(new), str(out))
    assert refused.value.diagnostics["records_off_their_line"] == {"base": [], "new": [1110]}
    assert not out.exists()


@pytest.mark.parametrize("side", ["base", "new"])
def test_rekey_refuses_records_broken_after_a_string_holding_a_semicolon(tmp_path, side):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    model(new, 1000)
    path, offset = (base, 0) if side == "base" else (new, 1000)
    f = ifcopenshell.open(str(path))
    f.create_entity("IfcPropertySingleValue", id=120 + offset, Name="a;b")
    f.write(str(path))
    path.write_bytes(path.read_bytes().replace(b"'a;b',", b"'a;b',\n"))
    with pytest.raises(RekeyRefused, match="not alone on one line") as refused:
        rekey_files(str(base), str(new), str(out))
    assert refused.value.diagnostics["records_off_their_line"] == {"base": [], "new": [], side: [120 + offset]}
    assert not out.exists()


def storey_with_wall(path, offset, guid, length):
    f = model(path, offset)

    def entity(cls, number, **attrs):
        return f.create_entity(cls, id=number + offset, **attrs)

    context = entity("IfcGeometricRepresentationContext", 200, ContextType="Model", CoordinateSpaceDimension=3, Precision=1e-5, WorldCoordinateSystem=f.by_id(20 + offset))
    start = entity("IfcCartesianPoint", 210, Coordinates=(0.0, 0.0, 0.0))
    end = entity("IfcCartesianPoint", 220, Coordinates=(length, 0.0, 0.0))
    curve = entity("IfcPolyline", 230, Points=(start, end))
    representation = entity("IfcShapeRepresentation", 240, ContextOfItems=context, RepresentationIdentifier="Axis", RepresentationType="Curve3D", Items=(curve,))
    shape = entity("IfcProductDefinitionShape", 250, Representations=(representation,))
    wall = entity("IfcWall", 260, GlobalId=guid, Name="W", ObjectPlacement=f.by_id(30 + offset), Representation=shape)
    storey = entity("IfcBuildingStorey", 270, GlobalId="0000000000000000000030", Name="Level 1")
    entity("IfcRelContainedInSpatialStructure", 280, GlobalId="0000000000000000000031", RelatedElements=(f.by_id(40 + offset), f.by_id(50 + offset), wall), RelatingStructure=storey)
    f.write(str(path))


@pytest.mark.parametrize("new_length", [3.0, 5.0], ids=["regenerated guid", "replaced wall"])
def test_rekey_never_hands_a_products_id_to_a_different_globalid(tmp_path, new_length):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    storey_with_wall(base, 0, "0000000000000000000041", 3.0)
    storey_with_wall(new, 1000, "0000000000000000000042", new_length)
    with pytest.raises(RekeyRefused, match="unmatched rooted") as refused:
        rekey_files(str(base), str(new), str(out))
    diagnostics = refused.value.diagnostics
    assert diagnostics["unsafe_rooted_additions"] == [1260]
    assert diagnostics["globalid_refusals"] == 1
    assert not out.exists()


def test_rekey_role_fallback_refuses_a_role_ambiguous_on_the_new_side(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, values in ((base, 0, (1,)), (new, 1000, (2, 3))):
        f = model(path, offset)
        for k, value in enumerate(values):
            f.create_entity("IfcPropertySingleValue", id=140 + k + offset, Name="Orphan", NominalValue=f.create_entity("IfcInteger", value))
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    assert 140 not in {e.id() for e in ifcopenshell.open(str(out))}
    assert stats["fresh"] == 2
    assert stats["role_fallback_ambiguous"] == 2
    assert stats["role_fallback_matches"] == 0


def test_rekey_leaves_unequal_twin_buckets_unmatched(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    model(base)
    f = model(new, 1000)
    f.remove(f.by_id(1100))
    f.write(str(new))
    stats = rekey_files(str(base), str(new), str(out))
    assert not {e.id() for e in ifcopenshell.open(str(out))} & {90, 100}
    assert stats["fresh"] == 1
    assert stats["unmatched_twins"] == 1


def test_rekey_leaves_referenced_twins_without_matched_parents_unmatched(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, z in ((base, 0, 0.0), (new, 1000, 5.0)):
        f = model(path, offset)
        for number, direction in ((300, (1.0, 0.0, z)), (330, (0.0, 1.0, z))):
            twin = f.create_entity("IfcCartesianPoint", id=number + offset, Coordinates=(0.0, 0.0, 0.0))
            other = f.create_entity("IfcCartesianPoint", id=number + 10 + offset, Coordinates=direction)
            f.create_entity("IfcPolyline", id=number + 20 + offset, Points=(twin, other))
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    assert not {e.id() for e in ifcopenshell.open(str(out))} & {300, 330}
    assert stats["fresh"] == 6
    assert stats["unmatched_twins"] == 2


def test_rekey_equal_hash_sharing_twins_are_unmatched_and_step_order_independent(tmp_path):
    base, new, permuted, out, permuted_out = (
        tmp_path / name for name in ("base.ifc", "new.ifc", "permuted.ifc", "out.ifc", "permuted-out.ifc")
    )
    for path, offset in ((base, 0), (new, 1000)):
        f = model(path, offset)
        first = f.create_entity("IfcCartesianPoint", id=400 + offset, Coordinates=(4.0, 5.0, 6.0))
        second = f.create_entity("IfcCartesianPoint", id=410 + offset, Coordinates=(4.0, 5.0, 6.0))
        for number, point in ((300, first), (310, first), (320, second)):
            f.create_entity("IfcAxis2Placement3D", id=number + offset, Location=point)
        f.write(str(path))
    data = new.read_text()
    ids = [int(m[1]) for m in re.finditer(r"^#([0-9]+)=", data, re.MULTILINE)]
    renumber = dict(zip(ids, reversed(ids)))
    permuted.write_text(re.sub(r"#([0-9]+)", lambda m: f"#{renumber.get(int(m[1]), int(m[1]))}", data))

    original_stats = rekey_files(str(base), str(new), str(out))
    permuted_stats = rekey_files(str(base), str(permuted), str(permuted_out))
    assert original_stats["unmatched_twins"] == permuted_stats["unmatched_twins"] == 5
    assert original_stats["fresh"] == permuted_stats["fresh"] == 5
    assert original_stats["role_conflicts"] == permuted_stats["role_conflicts"] == 0

    graphs = []
    for path in (out, permuted_out):
        restored = ifcopenshell.open(str(path))
        assert not {300, 310, 320, 400, 410} & {e.id() for e in restored}
        anchored = [str(e) for e in restored if e.id() <= 410]
        points = restored.by_type("IfcCartesianPoint")
        sharing = sorted(
            sum(axis.Location.id() == point.id() for axis in restored.by_type("IfcAxis2Placement3D"))
            for point in points if point.id() > 410
        )
        assert sharing == [1, 2]
        graphs.append((anchored, sharing))
    assert graphs[0] == graphs[1]


@pytest.mark.parametrize("same_target", [False, True])
def test_rekey_twins_require_identical_resolved_reference_targets(tmp_path, same_target):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset in ((base, 0), (new, 1000)):
        f = model(path, offset)
        for number, x in ((300, 12.0), (310, 13.0)):
            point = f.create_entity("IfcCartesianPoint", id=number + offset, Coordinates=(4.0, 5.0, 6.0))
            other = f.create_entity("IfcCartesianPoint", id=number + 1 + offset, Coordinates=(x, 0.0, 0.0))
            f.create_entity("IfcPolyline", id=number + 2 + offset, Points=(point, other))
        for number, target in ((400, 300), (410, 300 if same_target else 310)):
            f.create_entity("IfcAxis2Placement3D", id=number + offset, Location=f.by_id(target + offset))
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert stats["fresh"] == (0 if same_target else 2)
    assert stats["unmatched_twins"] == (0 if same_target else 2)
    if same_target:
        assert restored.by_id(400).Location.id() == restored.by_id(410).Location.id() == 300
    else:
        assert not {400, 410} & {e.id() for e in restored}


@pytest.mark.parametrize("extra", [False, True])
def test_rekey_ordered_duplicate_children_follow_positions_not_step_ids(tmp_path, extra):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset in ((base, 0), (new, 1000)):
        f = model(path, offset)
        first = f.create_entity("IfcCartesianPoint", id=300 + offset, Coordinates=(4.0, 5.0, 6.0))
        second = f.create_entity("IfcCartesianPoint", id=310 + offset, Coordinates=(4.0, 5.0, 6.0))
        tail = f.create_entity("IfcCartesianPoint", id=320 + offset, Coordinates=(8.0, 9.0, 10.0))
        points = [first, second, tail] if not offset else [second, first, tail]
        if offset and extra:
            added = f.create_entity("IfcCartesianPoint", id=330 + offset, Coordinates=(4.0, 5.0, 6.0))
            points.insert(2, added)
        f.create_entity("IfcPolyline", id=340 + offset, Points=points)
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    point_ids = [point.id() for point in restored.by_id(340).Points]
    assert point_ids[:2] == [300, 310]
    assert point_ids[-1] == 320
    if extra:
        assert point_ids[2] > 340
    assert stats["fresh"] == int(extra)


def test_rekey_unequal_unordered_child_twins_stay_unmatched(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, count in ((base, 0, 2), (new, 1000, 3)):
        f = model(path, offset)
        props = [f.create_entity("IfcPropertySingleValue", id=300 + offset + i, Name="Twin") for i in range(count)]
        f.create_entity("IfcPropertySet", id=340 + offset, GlobalId="0000000000000000000040", HasProperties=props)
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert all(prop.id() > 340 for prop in restored.by_id(340).HasProperties)
    assert stats["fresh"] == stats["unmatched_twins"] == 3


def test_rekey_recovers_unique_content_after_final_role_propagation(tmp_path):
    base, new, out = (tmp_path / name for name in ("base.ifc", "new.ifc", "out.ifc"))
    for path, offset, x in ((base, 0, 12.0), (new, 1000, 13.0)):
        f = model(path, offset)
        first = f.create_entity("IfcCartesianPoint", id=300 + offset, Coordinates=(4.0, 5.0, 6.0))
        f.create_entity("IfcCartesianPoint", id=310 + offset, Coordinates=(4.0, 5.0, 6.0))
        tail = f.create_entity("IfcCartesianPoint", id=320 + offset, Coordinates=(x, 0.0, 0.0))
        f.create_entity("IfcPolyline", id=340 + offset, Points=(first, tail))
        f.write(str(path))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert stats["fresh"] == stats["unmatched_twins"] == 0
    assert [point.id() for point in restored.by_id(340).Points] == [300, 320]
    assert restored.by_id(310).Coordinates == (4.0, 5.0, 6.0)
    assert restored.by_id(320).Coordinates == (13.0, 0.0, 0.0)
