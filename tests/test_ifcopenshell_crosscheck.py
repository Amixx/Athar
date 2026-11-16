"""Independent STEP and placement oracles over authored and real IFC files."""

from __future__ import annotations

import hashlib
import json
import math
import os
from itertools import zip_longest
from pathlib import Path

import ifcopenshell
import ifcopenshell.geom
import ifcopenshell.util.placement
import ifcopenshell.util.unit
import numpy as np
import pytest

from athar.bottom.signatures import build_signature_bundle

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
        entries += [(step, "IfcOrientedEdge", attr, ("reference", ref), ("derived", None), _DERIVED_ENDPOINT)
                    for step, refs in _ADVANCED_BREP_ENDPOINTS.items() for attr, ref in zip(("EdgeStart", "EdgeEnd"), refs)]
    return entries


_DERIVED_ENDPOINT = "Invalid explicit reference at a DERIVE-redeclared endpoint; IfcOpenShell exposes the derived marker"
_ADVANCED_BREP_ENDPOINTS = {94: (30, 36), 95: (36, 34), 96: (34, 32), 97: (32, 30),
                            105: (38, 44), 106: (44, 42), 107: (42, 40), 108: (40, 38),
                            116: (36, 38), 117: (38, 44), 118: (44, 30), 119: (30, 36),
                            132: (32, 30), 133: (30, 44), 134: (44, 42), 135: (42, 32),
                            148: (34, 32), 149: (32, 42), 150: (42, 40), 151: (40, 34),
                            164: (36, 34), 165: (34, 40), 166: (40, 38), 167: (38, 36)}


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


def _placement_disagreements(path):
    model = ifcopenshell.open(str(path))
    bundle = build_signature_bundle(str(path))
    scale = ifcopenshell.util.unit.calculate_unit_scale(model)
    differences, unresolved = [], []
    for step, signature in bundle.signatures.items():
        placement = getattr(model[step], "ObjectPlacement", None)
        if placement is None:
            continue
        current, seen = placement, set()
        while current is not None and current.is_a("IfcLocalPlacement") and current.id() not in seen:
            seen.add(current.id())
            current = current.PlacementRelTo
        plain_chain = current is None
        if signature.placement_unresolved:
            assert not plain_chain, (step, placement)
            assert signature.placement is None
            unresolved.append((step, current.is_a(), current.id()))
            continue
        assert plain_chain, (step, placement)
        assert signature.placement is not None, step
        oracle = ifcopenshell.util.placement.get_local_placement(placement)
        oracle[:3, 3] *= scale
        actual = [value / 1_000_000 for value in signature.placement]
        expected = oracle.flatten().tolist()
        # PLACEMENT_SCALE is 1e6; translations and rotation entries have a 1e-6 quantum.
        if any(not (math.isfinite(a) and math.isfinite(b)) or abs(a - b) > 1e-6 + 1e-9 * abs(b)
               for a, b in zip(actual, expected)):
            differences.append((step, actual, expected))
    return differences, unresolved


@pytest.mark.parametrize("source", _FILES)
def test_placement_agreement(source):
    path = source if isinstance(source, Path) else Path(corpus_path(source))
    differences, unresolved = _placement_disagreements(path)
    print(f"{path.name}: unresolved placements {unresolved}")
    expected_differences = {
        "cylinders.ifc": ({36}, "IfcBuildAxes projects the non-perpendicular RefDirection; IfcOpenShell a2p retains it"),
        "standard_case_element_beam.ifc": (set(range(2000, 2900, 100)),
                                           "IfcBuildAxes projects the rounded, non-perpendicular axes; IfcOpenShell a2p retains them"),
    }
    steps, reason = expected_differences.get(path.name, (set(), ""))
    assert {step for step, _, _ in differences} == steps, (
        f"{len(differences)} placement disagreements in {path}:\n" + "\n".join(map(repr, differences))
    )
    if differences:
        assert reason
        model = ifcopenshell.open(str(path))
        for step, actual, _ in differences:
            chain, placement = [], model[step].ObjectPlacement
            while placement is not None:
                chain.append(placement.RelativePlacement)
                placement = placement.PlacementRelTo
            projected = np.eye(4)
            for relative in reversed(chain):
                local = ifcopenshell.util.placement.get_axis2placement(relative)
                z, x = local[:3, 2].copy(), local[:3, 0].copy()
                x -= z * np.dot(x, z)
                x /= np.linalg.norm(x)
                local[:3, 0] = x
                local[:3, 1] = np.cross(z, x)
                projected = projected @ local
            projected[:3, 3] *= ifcopenshell.util.unit.calculate_unit_scale(model)
            assert actual == pytest.approx(projected.flatten().tolist(), abs=1e-6, rel=1e-9), (step, reason)


@pytest.mark.parametrize("prefix", [None, "MILLI"])
def test_placement_authored_chain(tmp_path, prefix):
    model = ifcopenshell.file(schema="IFC4")
    unit = model.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE", Prefix=prefix)
    assignment = model.create_entity("IfcUnitAssignment", Units=[unit])
    model.create_entity("IfcProject", GlobalId=ifcopenshell.guid.new(), UnitsInContext=assignment)
    parent = None
    for position, axis, ref in [((23.125, -4.5, 8.0), (0.0, 3.0, 4.0), (2.0, 0.0, 0.0)),
                                ((-1.25, 6.75, 2.0), (0.0, 0.0, 1.0), (3.0, 4.0, 0.0)),
                                ((0.00000049, 0.00000051, -0.0000015), None, None)]:
        relative = model.create_entity("IfcAxis2Placement3D",
                                      Location=model.create_entity("IfcCartesianPoint", Coordinates=position),
                                      Axis=model.create_entity("IfcDirection", DirectionRatios=axis) if axis else None,
                                      RefDirection=model.create_entity("IfcDirection", DirectionRatios=ref) if ref else None)
        parent = model.create_entity("IfcLocalPlacement", PlacementRelTo=parent, RelativePlacement=relative)
        model.create_entity("IfcWall", GlobalId=ifcopenshell.guid.new(), ObjectPlacement=parent)
    path = tmp_path / "placements.ifc"
    model.write(str(path))
    assert _parser_disagreements(path) == []
    assert _placement_disagreements(path) == ([], [])


def test_placement_nested_rounding(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    parent = None
    for _ in range(4):
        point = model.create_entity("IfcCartesianPoint", Coordinates=(0.00000049, -0.00000049, 0.0))
        relative = model.create_entity("IfcAxis2Placement3D", Location=point)
        parent = model.create_entity("IfcLocalPlacement", PlacementRelTo=parent, RelativePlacement=relative)
    model.create_entity("IfcWall", GlobalId=ifcopenshell.guid.new(), ObjectPlacement=parent)
    path = tmp_path / "nested.ifc"
    model.write(str(path))
    for point in model.by_type("IfcCartesianPoint"):
        point.Coordinates = (0.0, 0.0, 0.0)
    moved = tmp_path / "moved.ifc"
    model.write(str(moved))
    from athar.engine import diff_files

    report = diff_files(str(path), str(moved))
    (item,) = report["modified"]
    assert item["aspects"]["placement"] == "changed"
    assert item["aspects"]["placement_delta_mm"] == pytest.approx([-0.002, 0.002, 0.0])
    assert _placement_disagreements(path) == ([], [])


def test_placement_coverage_model(tmp_path):
    from tests.test_change_coverage import _build

    path = tmp_path / "coverage.ifc"
    _build().write(str(path))
    assert _parser_disagreements(path) == []
    differences, unresolved = _placement_disagreements(path)
    assert differences == []
    model = ifcopenshell.open(str(path))
    assert [(model[step].Name, cls) for step, cls, _ in unresolved] == [
        ("GP1", "IfcGridPlacement"), ("GP2", "IfcGridPlacement")
    ]


# Without convert-back-units the kernel returns a standalone placement in
# metres for some files and in file units for others.
_KERNEL = ifcopenshell.geom.settings()
_KERNEL.set("convert-back-units", True)


def _kernel_matrix(instance, scale=1.0):
    matrix = np.array(ifcopenshell.geom.create_shape(_KERNEL, instance).matrix).reshape((4, 4), order="F")
    matrix[:3, 3] *= scale
    return matrix


def _native_matrix(signature):
    return np.array(signature.placement).reshape(4, 4) / 1_000_000


def _kernel_placement_disagreements(path):
    model = ifcopenshell.open(str(path))
    scale = ifcopenshell.util.unit.calculate_unit_scale(model)
    differences = []
    for step, signature in build_signature_bundle(str(path)).signatures.items():
        if signature.placement is None:
            continue
        actual = _native_matrix(signature)
        expected = _kernel_matrix(model[step].ObjectPlacement, scale)
        if not np.allclose(actual, expected, rtol=1e-9, atol=1e-6):
            differences.append((step, actual.tolist(), expected.tolist()))
    return differences


@pytest.mark.parametrize("source", _FILES)
def test_kernel_placement_agreement(source):
    path = source if isinstance(source, Path) else Path(corpus_path(source))
    differences = _kernel_placement_disagreements(path)
    assert differences == [], f"{len(differences)} kernel placement disagreements in {path}:\n" + "\n".join(map(repr, differences))


_ZERO_FRAME = ((0.0, 0.0, 0.0), (0.0, 0.0, 0.0))
_PARALLEL = "Undefined frame (RefDirection parallel to Axis): native has a zero rotation, the kernel keeps Z with zero X and Y"
# (Axis, RefDirection, native (X, Z) where it differs from the kernel, reason).
# A 2-tuple Axis is an IfcAxis2Placement2D with that RefDirection.
_AXES = [
    (None, None, None, ""),
    ((0.0, 0.0, 5.0), None, None, ""),
    ((0.0, 1.0, 0.0), None, None, ""),
    (None, (2.0, 0.0, 0.0), None, ""),
    (None, (1.0, 1.0, 1.0), None, ""),
    ((0.0, 3.0, 4.0), (2.0, 0.0, 0.0), None, ""),
    ((0.0, 0.0, 1.0), (1.0, 0.0, 1.0), None, ""),
    ((0.0, 0.0, 1.0), (1e-10, 0.0, 1.0), None, ""),
    ((0.0, 0.0, 1e-13), (1.0, 0.0, 0.0), None, ""),
    ((0.0, 0.0, 1.0), (0.0, 1.0), None, ""),
    ((0.0, 0.0, 0.0), (1.0, 0.0, 0.0), None, ""),
    ((1.0, 0.0, 0.0), None, ((0.0, 1.0, 0.0), (1.0, 0.0, 0.0)),
     "IfcFirstProjAxis takes (0,1,0) for Z=(1,0,0); the kernel keeps OCC's default X (0,0,1)"),
    ((1.0, 1.0, 1.0), None, ((2 / 6**0.5, -1 / 6**0.5, -1 / 6**0.5), (3**-0.5,) * 3),
     "IfcFirstProjAxis projects (1,0,0) onto the plane of Z; the kernel keeps OCC's default X (-1,-1,2)/sqrt(6)"),
    ((-1.0, 0.0, 0.0), None, ((0.0, 1.0, 0.0), (-1.0, 0.0, 0.0)),
     "IfcFirstProjAxis is undefined (its default (1,0,0) is parallel to Z); native takes (0,1,0) as for +X, "
     "the kernel returns zero X and Y axes"),
    (None, (0.0, 0.0, 3.0), _ZERO_FRAME, _PARALLEL),
    ((0.0, 0.0, 1.0), (0.0, 0.0, -1.0), _ZERO_FRAME, _PARALLEL),
    ((0.0, 0.0, 1e-300), (1.0, 0.0, 0.0), ((1.0, 0.0, 0.0), (0.0, 0.0, 1.0)),
     "The kernel squares the ratios before normalising and underflows; IfcNormalise is defined for any nonzero direction"),
    ((3.0, 4.0), None, None, ""),
    ((1.0, 1.0, 1.0), None, None, ""),
    ((0.0, 0.0), None, _ZERO_FRAME, "Zero RefDirection: native has a zero rotation, the kernel keeps Z with zero X and Y"),
    ((0.0, 0.0, 2.0), None, _ZERO_FRAME, _PARALLEL),
]


def _axes_model(tmp_path):
    model = ifcopenshell.file(schema="IFC4")
    direction = lambda ratios: model.create_entity("IfcDirection", DirectionRatios=ratios) if ratios else None
    for index, (axis, ref, _, _) in enumerate(_AXES):
        if axis is not None and len(axis) == 2 or index >= len(_AXES) - 4:
            relative = model.create_entity("IfcAxis2Placement2D",
                                          Location=model.create_entity("IfcCartesianPoint", Coordinates=(1.0, 2.0)),
                                          RefDirection=direction(axis))
        else:
            relative = model.create_entity("IfcAxis2Placement3D",
                                          Location=model.create_entity("IfcCartesianPoint", Coordinates=(1.0, 2.0, 3.0)),
                                          Axis=direction(axis), RefDirection=direction(ref))
        placement = model.create_entity("IfcLocalPlacement", RelativePlacement=relative)
        model.create_entity("IfcWall", GlobalId=ifcopenshell.guid.new(), Name=str(index), ObjectPlacement=placement)
    path = tmp_path / "axes.ifc"
    model.write(str(path))
    return path


def test_axes_authored(tmp_path):
    path = _axes_model(tmp_path)
    model = ifcopenshell.open(str(path))
    for step, signature in build_signature_bundle(str(path)).signatures.items():
        axis, ref, native_frame, reason = _AXES[int(model[step].Name)]
        actual = _native_matrix(signature)
        with np.errstate(all="ignore"):
            kernel = _kernel_matrix(model[step].ObjectPlacement.RelativePlacement)
        agrees = np.allclose(actual, kernel, rtol=1e-9, atol=1e-6)
        assert agrees == (native_frame is None), (axis, ref, actual.tolist(), kernel.tolist(), reason)
        if native_frame is not None:
            assert reason
            x, z = (np.array(v) for v in native_frame)
            expected = np.column_stack([x, np.cross(z, x), z])
            assert actual[:3, :3] == pytest.approx(expected, abs=1e-6), (axis, ref, reason)


def _frame_hash_classes(tmp_path, variants, build):
    model = ifcopenshell.file(schema="IFC4")
    context = model.create_entity("IfcGeometricRepresentationContext", ContextType="Model", CoordinateSpaceDimension=3,
                                  WorldCoordinateSystem=model.create_entity(
                                      "IfcAxis2Placement3D", Location=model.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0))))
    profile = model.create_entity("IfcRectangleProfileDef", ProfileType="AREA", XDim=1.0, YDim=2.0)
    instances = []
    for index, variant in enumerate(variants):
        item, frame = build(model, context, profile, variant)
        representation = model.create_entity("IfcShapeRepresentation", ContextOfItems=context, RepresentationIdentifier="Body",
                                             RepresentationType="SweptSolid", Items=[item])
        shape = model.create_entity("IfcProductDefinitionShape", Representations=[representation])
        model.create_entity("IfcBuildingElementProxy", GlobalId=ifcopenshell.guid.new(), Name=str(index), Representation=shape)
        instances.append(frame)
    path = tmp_path / "frames.ifc"
    model.write(str(path))
    signatures = build_signature_bundle(str(path)).signatures.values()
    hashes = {int(signature.name): signature.vh_geometry for signature in signatures}
    return model, [hashes[index] for index in range(len(variants))], instances


def _class_disagreements(keys, matrices):
    pairs = range(len(keys))
    return {(i, j) for i in pairs for j in pairs
            if i < j and (keys[i] == keys[j]) != np.allclose(matrices[i], matrices[j], atol=1e-9)}


def test_placement_frame_hashes_follow_kernel_frames(tmp_path):
    variants = [(None, None), ((0.0, 0.0, 1.0), (1.0, 0.0, 0.0)), ((0.0, 0.0, 7.0), (3.0, 0.0, 0.0)),
                ((0.0, 0.0, 1.0), (1.0, 0.0, 0.5)), (None, (1.0, 0.0, -2.0)), ((0.0, 0.0, 1.0), (0.0, 1.0, 0.0)),
                (None, (0.0, 2.0, 0.0)), ((0.0, 1.0, 0.0), (1.0, 0.0, 0.0)), ((0.0, 0.0, 1e-13), (1.0, 0.0, 0.0))]

    def build(model, context, profile, variant):
        axis, ref = (model.create_entity("IfcDirection", DirectionRatios=r) if r else None for r in variant)
        position = model.create_entity("IfcAxis2Placement3D", Location=model.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
                                       Axis=axis, RefDirection=ref)
        solid = model.create_entity("IfcExtrudedAreaSolid", SweptArea=profile, Position=position, Depth=1.0,
                                    ExtrudedDirection=model.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)))
        return solid, position

    _model, keys, positions = _frame_hash_classes(tmp_path, variants, build)
    matrices = [_kernel_matrix(position) for position in positions]
    assert _class_disagreements(keys, matrices) == set(), list(zip(variants, keys))
    assert len(set(keys)) == 3


def test_operator_frame_hashes_follow_ifcopenshell_frames(tmp_path):
    variants = [(None, None, None, None), ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0), (0.0, 0.0, 1.0), 1.0),
                ((2.0, 0.0, 0.0), (0.0, 3.0, 0.0), (0.0, 0.0, 4.0), None), (None, (0.0, -1.0, 0.0), None, None),
                (None, None, None, 2.0), ((0.0, 1.0, 0.0), None, None, None), ((0.0, 5.0, 0.0), (-1.0, 0.0, 0.0), None, None),
                ((0.0, 1.0, 0.0), (1.0, 0.0, 0.0), None, None)]

    def build(model, context, profile, variant):
        *axes, scale = variant
        axis1, axis2, axis3 = (model.create_entity("IfcDirection", DirectionRatios=r) if r else None for r in axes)
        origin = model.create_entity("IfcAxis2Placement3D", Location=model.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)))
        solid = model.create_entity("IfcExtrudedAreaSolid", SweptArea=profile, Depth=1.0,
                                    ExtrudedDirection=model.create_entity("IfcDirection", DirectionRatios=(0.0, 0.0, 1.0)))
        mapped = model.create_entity("IfcShapeRepresentation", ContextOfItems=context, RepresentationIdentifier="Body",
                                     RepresentationType="SweptSolid", Items=[solid])
        source = model.create_entity("IfcRepresentationMap", MappingOrigin=origin, MappedRepresentation=mapped)
        operator = model.create_entity("IfcCartesianTransformationOperator3D", Axis1=axis1, Axis2=axis2, Axis3=axis3, Scale=scale,
                                       LocalOrigin=model.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)))
        return model.create_entity("IfcMappedItem", MappingSource=source, MappingTarget=operator), operator

    _model, keys, operators = _frame_hash_classes(tmp_path, variants, build)
    matrices = [ifcopenshell.util.placement.get_cartesiantransformationoperator3d(operator) for operator in operators]
    # IfcSecondProjAxis leaves Y undefined when the default Axis2 (0,1,0) is
    # parallel to Axis1, so native hashes variant 5 attribute-wise; IfcOpenShell
    # takes Z x X, the frame of variant 6.
    assert _class_disagreements(keys, matrices) == {(5, 6)}, list(zip(variants, keys))
    assert len(set(keys)) == 6


def _ifcos_references(entity):
    def walk(value):
        if isinstance(value, ifcopenshell.entity_instance):
            if value.id():
                yield value.id()
            else:
                for item in value:
                    yield from walk(item)
        elif isinstance(value, tuple):
            for item in value:
                yield from walk(item)

    return [(entity.attribute_name(i), target) for i, value in enumerate(entity) for target in walk(value)]


def _reference_policy(schema):
    policy = native().reference_policy()
    expected = {(keyword, attr): ("none", reason) for keyword, attr, reason in policy["denied_references"]}
    expected.update({(keyword, attr): ("reversed", "content points at the entity it describes; the owner includes it")
                     for keyword, attr, _ in policy["reverse_attachments"]})
    for keyword, links, ignored in native().relationship_policy():
        if ignored:
            entities = {e.name().upper(): e for e in schema.entities()}
            if keyword in entities:
                expected.update({(keyword, attr.name()): ("none", f"ignored relationship: {ignored}")
                                 for attr in entities[keyword].all_attributes()})
            continue
        for source, target, kind in links:
            for attr in (source, target):
                expected[(keyword, attr)] = ("projected", f"{kind} role, projected onto the pairs of related entities")
    return expected


def _graph_disagreements(path):
    from athar.bottom.native_schema import schema_descriptors_json

    model = ifcopenshell.open(str(path))
    schema = ifcopenshell.ifcopenshell_wrapper.schema_by_name(model.schema_identifier)
    refs, includes, context, spatial = native().reference_graph(str(path), schema_descriptors_json(model.schema))
    ids = {entity.id() for entity in model}
    native_refs = {}
    for source, attr, target in refs:
        if target in ids:
            native_refs.setdefault(source, []).append((attr, target))
    edges = {(source, target, label) for source, target, label, _ in includes}
    adjacent = {frozenset(pair) for pair in context + spatial} | {frozenset((s, t)) for s, t, _ in edges}
    policy = _reference_policy(schema)
    referrers = {}
    for source, pairs in native_refs.items():
        for _, target in pairs:
            referrers.setdefault(target, set()).add(source)
    links_of = {keyword: links for keyword, links, ignored in native().relationship_policy() if not ignored}
    differences, explained, dispositions = [], set(), {}
    for entity in model:
        keyword, step = entity.is_a().upper(), entity.id()
        oracle = _ifcos_references(entity)
        if sorted(oracle) != sorted(native_refs.get(step, [])):
            differences.append(("references", step, entity.is_a(), sorted(oracle), sorted(native_refs.get(step, []))))
        if {target for _, target in oracle} != {e.id() for e in model.traverse(entity, max_levels=1) if e.id()} - {step}:
            differences.append(("traverse", step, entity.is_a()))
        if len(model.get_inverse(entity)) != len(referrers.get(step, ())):
            differences.append(("inverse", step, entity.is_a()))
        for attr, target in oracle:
            disposition, _ = policy.get((keyword, attr)) or policy.get(("*", attr)) or ("edge", "")
            if (step, target, attr) in edges or frozenset((step, target)) in adjacent and disposition == "edge":
                found = "edge"
            elif (target, step, f"~{keyword}.{attr}") in edges:
                found = "reversed"
            elif disposition == "projected":
                found = "projected"
            else:
                found = "none" if frozenset((step, target)) not in adjacent else "edge"
            if found != disposition:
                differences.append(("disposition", step, entity.is_a(), attr, target, disposition, found))
            dispositions[found] = dispositions.get(found, 0) + 1
            explained.add(frozenset((step, target)))
    for rel in model.by_type("IfcRelationship"):
        references = _ifcos_references(rel)
        for source_attr, target_attr, _ in links_of.get(rel.is_a().upper(), ()):
            for s in (t for a, t in references if a == source_attr):
                for t in (t for a, t in references if a == target_attr):
                    if s != t:
                        explained.add(frozenset((s, t)))
                        if frozenset((s, t)) not in adjacent:
                            differences.append(("projection", rel.id(), rel.is_a(), source_attr, s, target_attr, t))
    differences += [("unexplained", tuple(sorted(pair))) for pair in adjacent - explained]
    owners = {}
    for source, target, label in edges:
        if label.startswith("~"):
            owners[(source, label)] = owners.get((source, label), 0) + 1
    for (owner, label), count in owners.items():
        keyword, attr = label[1:].split(".")
        attached = [e for e in model.get_inverse(model[owner]) if e.is_a().upper() == keyword
                    and owner in {t for a, t in _ifcos_references(e) if a == attr}]
        if count != len(attached):
            differences.append(("attached", owner, label, count, len(attached)))
    return differences, dispositions


def _expected_graph_differences(name):
    if name != "advanced_brep.ifc":
        return set()
    return ({("references", step) for step in _ADVANCED_BREP_ENDPOINTS}
            | {("unexplained", tuple(sorted((step, ref)))) for step, refs in _ADVANCED_BREP_ENDPOINTS.items() for ref in refs})


@pytest.mark.parametrize("source", _FILES)
def test_graph_agreement(source):
    path = source if isinstance(source, Path) else Path(corpus_path(source))
    differences, _ = _graph_disagreements(path)
    keys = {(kind, detail[0]) for kind, *detail in differences}
    assert keys == _expected_graph_differences(path.name), (
        f"{len(differences)} reference graph disagreements in {path}:\n" + "\n".join(map(repr, differences))
    )


def test_graph_policy_dispositions(tmp_path):
    from tests.test_change_coverage import _build

    path = tmp_path / "coverage.ifc"
    _build().write(str(path))
    differences, dispositions = _graph_disagreements(path)
    assert differences == []
    assert set(dispositions) == {"edge", "none", "reversed", "projected"}, dispositions
    model = ifcopenshell.open(str(path))
    policy = _reference_policy(ifcopenshell.ifcopenshell_wrapper.schema_by_name(model.schema_identifier))
    assert all(reason for _, reason in policy.values())
