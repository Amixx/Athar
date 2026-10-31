import json
import subprocess
import sys

import ifcopenshell
import pytest

from athar.engine import SchemaMismatchError, diff_files
from athar.rekey import rekey_files


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
    model(base)
    f = model(new, 1000)
    f.by_id(1040).GlobalId = "0000000000000000000009"
    f.by_id(1040).Name = "Replacement wall"
    placement = f.by_id(1030)
    placement.PlacementRelTo = placement
    f.write(str(new))
    stats = rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert restored.by_guid("0000000000000000000009").id() > 100
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
    data = data.replace(b"((7.,2.,3.))", b"((7.000E+000,\n2.,3.)) /* #999 is a comment */")
    new.write_bytes(data.replace(b"\n", b"\r\n"))
    rekey_files(str(base), str(new), str(out))
    restored = ifcopenshell.open(str(out))
    assert restored.by_id(70).GlobalId == "0000000000000000000007"
    assert restored.by_id(80).RelatingPropertyDefinition.id() == 70
    assert restored.by_id(60).NominalValue.wrappedValue == "B\r\n#123 ' quoted"
    assert encoded in out.read_bytes()
    assert b"7.000E+000,\r\n2." in out.read_bytes()
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
