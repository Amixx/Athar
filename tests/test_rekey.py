import json
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
    with pytest.raises(RekeyRefused, match="cannot split into attributes") as refused:
        rekey_files(str(base), str(new), str(out))
    assert refused.value.diagnostics["unmergeable_changes"] == [{"quoted name": 1040, "split record": 1010}.get(edit, 1210)]
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


@pytest.mark.parametrize(("members", "splits"), [(32768, True), (32769, False)])
def test_ifcmerge_splits_stops_at_perls_group_repetition_cap(members, splits):
    related = b",".join(b"#%d" % i for i in range(10, 10 + members))
    record = b"IFCRELCONTAINEDINSPATIALSTRUCTURE('0000000000000000000005',$,$,$,(" + related + b"),#1)"
    assert _ifcmerge_splits(record, 6) is splits
