"""Independent STEP and placement oracles over authored and real IFC files."""

from __future__ import annotations

import hashlib
import json
import os
from itertools import zip_longest
from pathlib import Path

import ifcopenshell
import pytest

from athar.bottom._native import native
from tests.corpus import REPO_ROOT, corpus_path


def _large_paths():
    paths = set()
    for directory in ("corpus/roundtrip-revit-2026-07-06/exports", "corpus/gni-bim-sample"):
        paths.update((REPO_ROOT / directory).rglob("*.ifc"))
    directory = os.getenv("ATHAR_IFCOS_CORPUS_DIR")
    if directory:
        manifest = json.loads((Path(__file__).parent / "fixtures/ifcos_corpus_manifest.json").read_text())
        paths.update(Path(directory) / entry["path"] for entry in manifest["files"])
    return sorted(path for path in paths if path.is_file())


_FILES = [pytest.param(key, id=key) for key in ("duplex_arch", "bl_v0")]
_FILES += [pytest.param(path, id=str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path),
                        marks=pytest.mark.skipif(os.getenv("ATHAR_RUN_LARGE_ACCEPTANCE") != "1",
                                                 reason="set ATHAR_RUN_LARGE_ACCEPTANCE=1"))
           for path in _large_paths()]


def _ifcos_value(value, kind=None):
    if kind == "DERIVED":
        return ("derived", None)
    if value is None:
        return ("null", None)
    if isinstance(value, ifcopenshell.entity_instance):
        if value.id():
            return ("reference", value.id())
        return ("typed", value.is_a().upper(), [_ifcos_value(v, value.attribute_type(i)) for i, v in enumerate(value)])
    if isinstance(value, tuple):
        return ("list", [_ifcos_value(v, kind) for v in value])
    if isinstance(value, bool):
        return ("enum", "T" if value else "F")
    if kind in ("ENUMERATION", "LOGICAL", "BOOL") or value == "UNKNOWN" and kind != "STRING":
        return ("enum", "U" if value == "UNKNOWN" else str(value).upper())
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, float):
        return ("real", value)
    if isinstance(value, int):
        return ("integer", value)
    raise AssertionError((kind, value))


def _parser_disagreements(path):
    model = ifcopenshell.open(str(path))
    records = {step: (name, attrs) for step, name, attrs in native().parse_step_file(str(path))}
    assert set(records) == {entity.id() for entity in model}
    differences = []
    for entity in model:
        name, attrs = records[entity.id()]
        assert name.casefold() == entity.is_a().casefold(), (entity, name)
        expected = [_ifcos_value(v, entity.attribute_type(i)) for i, v in enumerate(entity)]
        if attrs != expected:
            for i, (actual, oracle) in enumerate(zip_longest(attrs, expected, fillvalue=("absent", None))):
                if actual != oracle:
                    attribute = entity.attribute_name(i) if i < len(entity) else f"extra[{i}]"
                    differences.append((entity.id(), entity.is_a(), attribute, actual, oracle))
    return differences


def _expected_parser_differences(name):
    entries = []
    if name in ("1001--reinforcingBar--segfault.ifc", "1076--beam--missingSubtraction.ifc"):
        step = 15 if name.startswith("1001") else 367
        entries += [(step, "IfcBuildingStorey", f"extra[{i}]", ("null", None), ("absent", None),
                     "Invalid storey has twelve arguments; schema and IfcOpenShell expose ten") for i in (10, 11)]
    quoted_enums = {
        "444--beam--segfault--augmented.ifc": (7573, "IfcBuildingStorey", "CompositionType", ""),
        "474--walls--missing-subtractions--1--augmented.ifc": (1098, "IfcMonetaryUnit", "Currency", ".NOK."),
        "474--walls--missing-subtractions--2--augmented.ifc": (1417, "IfcMonetaryUnit", "Currency", ".NOK."),
        "587--walls--wrong-geometry.ifc": (1585, "IfcMonetaryUnit", "Currency", ".NOK."),
    }
    quoted_actions = {
        "685--wall--invalidMaterialFormat--augmented.ifc": 6068555,
        "686--wall--missing-subtraction--augmented.ifc": 140,
        "701--slab--segfault--augmented.ifc": 904688,
        "702--slab--segfault--augmented.ifc": 2059533,
        "703--beam--segfault--augmented.ifc": 1057927,
        "705--wall--segfault--augmented.ifc": 264968,
        "710--wall--segfault--augmented.ifc": 238,
        "786--reinforcingBar--segfault--augmented.ifc": 469500,
        "818--beam--segfault--augmented.ifc": 262,
        "987--cableSegment--segfault--augmented.ifc": 42246,
        "987--flowSegment--segfault--augmented.ifc": 58426,
        "988--reinforcingBar--segfault--augmented.ifc": 860487,
    }
    if name in quoted_actions:
        quoted_enums[name] = (quoted_actions[name], "IfcOwnerHistory", "ChangeAction", "MODIFIED")
    if name in quoted_enums:
        step, cls, attr, text = quoted_enums[name]
        entries.append((step, cls, attr, ("string", text), ("enum", text),
                        "Invalid quoted enumeration; IfcOpenShell coerces the string to the schema enum"))
    if name == "580--wall--wrong-geometry.ifc":
        entries.append((7, "IfcBeam", "ObjectPlacement", ("reference", 2714), ("null", None),
                        "Dangling #2714 is retained by the tokenizer and discarded by IfcOpenShell"))
    if name == "MaintenanceZoneIFC4_ver2.ifc":
        entries += [(step, "IfcGeometricRepresentationSubContext", attr, ("null", None), ("derived", None),
                     "Invalid $ at a DERIVE-redeclared attribute; IfcOpenShell exposes the schema-derived marker")
                    for step in (78, 135)
                    for attr in ("CoordinateSpaceDimension", "Precision", "WorldCoordinateSystem", "TrueNorth")]
    if name == "advanced_brep.ifc":
        endpoints = {94: (30, 36), 95: (36, 34), 96: (34, 32), 97: (32, 30),
                     105: (38, 44), 106: (44, 42), 107: (42, 40), 108: (40, 38),
                     116: (36, 38), 117: (38, 44), 118: (44, 30), 119: (30, 36),
                     132: (32, 30), 133: (30, 44), 134: (44, 42), 135: (42, 32),
                     148: (34, 32), 149: (32, 42), 150: (42, 40), 151: (40, 34),
                     164: (36, 34), 165: (34, 40), 166: (40, 38), 167: (38, 36)}
        entries += [(step, "IfcOrientedEdge", attr, ("reference", ref), ("derived", None),
                     "Invalid explicit reference at a DERIVE-redeclared endpoint; IfcOpenShell exposes the derived marker")
                    for step, refs in endpoints.items() for attr, ref in zip(("EdgeStart", "EdgeEnd"), refs)]
    return entries


@pytest.mark.parametrize("source", _FILES)
def test_parser_agreement(source):
    path = source if isinstance(source, Path) else Path(corpus_path(source))
    assert not path.read_bytes()[:64].startswith(b"version https://git-lfs"), path
    if os.getenv("ATHAR_IFCOS_CORPUS_DIR") and path.parent == Path(os.environ["ATHAR_IFCOS_CORPUS_DIR"]):
        manifest = json.loads((Path(__file__).parent / "fixtures/ifcos_corpus_manifest.json").read_text())
        entry = next(e for e in manifest["files"] if e["path"] == path.name)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"], path
    differences = _parser_disagreements(path)
    expected = _expected_parser_differences(path.name)
    assert all(entry[-1] for entry in expected)
    assert sorted(differences) == sorted(entry[:-1] for entry in expected), (
        f"{len(differences)} attribute disagreements in {path}:\n" + "\n".join(map(repr, differences))
    )


def test_parser_authored_values(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")
    model.create_entity("IfcPropertySingleValue", Name="a'b\\c\u00e9\U0001f642", NominalValue=model.create_entity("IfcLabel", "x"))
    model.create_entity("IfcPropertySingleValue", Name="logical", NominalValue=model.create_entity("IfcLogical", "UNKNOWN"))
    model.create_entity("IfcCartesianPointList3D", CoordList=((1.25, -2.5, 3.0), (4.5, 6.25, -7.0)))
    model.create_entity("IfcPropertySingleValue", Name="boolean", NominalValue=model.create_entity("IfcBoolean", True))
    path = tmp_path / "values.ifc"
    model.write(str(path))
    assert _parser_disagreements(path) == []
