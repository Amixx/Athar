from __future__ import annotations

import ast
import os
from pathlib import Path

import ifcopenshell
import ifcopenshell.guid
import pytest

from scripts.explore.derive_revit_truth import derive_truth

A, B, C = (ifcopenshell.guid.compress(f"{value:032x}") for value in (1, 2, 3))


def test_truth_module_has_no_engine_imports():
    path = Path(__file__).resolve().parents[1] / "scripts/explore/derive_revit_truth.py"
    tree = ast.parse(path.read_text())
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not any(name.split(".")[0] in {"athar", "athar_native", "benchmark_competitors"} for name in imports)


def _model(padding=0):
    model = ifcopenshell.file(schema="IFC4")
    for _ in range(padding):
        model.create_entity("IfcCartesianPoint", (0.0, 0.0, 0.0))
    for guid, x in ((A, 2.0), (B, 9.0)):
        placement = model.create_entity("IfcLocalPlacement", RelativePlacement=model.create_entity(
            "IfcAxis2Placement3D", Location=model.create_entity("IfcCartesianPoint", (x, 0., 0.))))
        solid = model.create_entity("IfcExtrudedAreaSolid", Depth=3.)
        rep = model.create_entity("IfcShapeRepresentation", Items=[solid])
        model.create_entity("IfcWall", GlobalId=guid, Name=guid, ObjectPlacement=placement,
                            Representation=model.create_entity("IfcProductDefinitionShape", Representations=[rep]))
    pset = model.create_entity("IfcPropertySet", GlobalId=ifcopenshell.guid.compress(f"{9:032x}"), Name="Pset_Test",
                               HasProperties=[model.create_entity("IfcPropertySingleValue", Name="Mark",
                                                                  NominalValue=model.create_entity("IfcLabel", "W1"))])
    model.create_entity("IfcRelDefinesByProperties", GlobalId=ifcopenshell.guid.compress(f"{10:032x}"),
                        RelatedObjects=[model.by_guid(A)], RelatingPropertyDefinition=pset)
    return model


def test_truth_detects_edits_and_ignores_step_ids(tmp_path):
    model = _model()
    old, new = tmp_path / "old.ifc", tmp_path / "new.ifc"
    model.write(str(old))
    _model(padding=5).write(str(new))
    assert derive_truth(old, new)["counts"] == {"added": 0, "deleted": 0, "modified": 0}
    a, b = model.by_guid(A), model.by_guid(B)
    a.Name = "edited"
    a.ObjectPlacement.RelativePlacement.Location.Coordinates = (2.01, 0., 0.)
    a.Representation.Representations[0].Items[0].Depth = 3.2
    model.remove(b)
    model.create_entity("IfcWall", GlobalId=C)
    model.write(str(new))
    truth = derive_truth(old, new)
    assert truth["changed_guids"] == {"added": [C], "deleted": [B], "modified": [A]}
    assert truth["aspects"][A] == ["attributes", "world_placement", "representation"]


def test_truth_numeric_tolerance_and_duplicate_identity(tmp_path):
    model = _model()
    old, new = tmp_path / "old.ifc", tmp_path / "new.ifc"
    model.write(str(old))
    model.by_guid(A).Representation.Representations[0].Items[0].Depth += 1e-10
    model.write(str(new))
    assert derive_truth(old, new)["changed_guids"]["modified"] == []
    model.create_entity("IfcWall", GlobalId=A)
    model.write(str(new))
    with pytest.raises(ValueError, match="duplicate product GlobalId"):
        derive_truth(old, new)


@pytest.mark.large_acceptance
@pytest.mark.skipif(os.getenv("ATHAR_RUN_LARGE_ACCEPTANCE") != "1", reason="set ATHAR_RUN_LARGE_ACCEPTANCE=1")
def test_truth_on_smallest_revit_pair():
    from scripts.explore.benchmark_competitors import _revit_export_path

    # r2 -> r3 changes only an export option, so no product's content changed.
    # Representation lists may still be re-emitted in a different order, which
    # the order-preserving graph comparison reports.
    old, new = (_revit_export_path(name) for name in ("r2", "r3"))
    truth = derive_truth(old, new)
    assert truth["changed_guids"]["added"] == []
    assert truth["changed_guids"]["deleted"] == []
    assert all(aspects == ["representation"] for aspects in truth["aspects"].values())
