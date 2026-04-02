from __future__ import annotations

import json
from pathlib import Path

from athar.bottom.constants import SUPPORTED_SCHEMA_PREFIXES
from athar.bottom.edge_policy import build_edge_set
from athar.bottom.merkle import compute_merkle_hashes
from athar.bottom.parser import _quantize_real
from athar.bottom.types import EntityRef, ParseDiagnostics, ParseResult, ParsedEntity, SignatureVector
from athar.bottom.wl_gossip import compute_topology_hashes
from athar.engine import _guid_collision_count
from athar.engine import diff_files, stream_diff_files


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_engine_diff_same_file_is_unchanged():
    src = str(FIXTURES / "house_v1.ifc")
    report = diff_files(src, src)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    assert report["stats"]["unchanged"] > 0
    assert report["canon_version"] == "athar-canon-v1"


def test_engine_diff_detects_changes_between_versions():
    old_path = str(FIXTURES / "house_v1.ifc")
    new_path = str(FIXTURES / "house_v2.ifc")
    report = diff_files(old_path, new_path)
    assert report["stats"]["modified"] > 0
    assert report["stats"]["unchanged"] > 0


def test_engine_metamorphic_step_renumbering_is_stable():
    base = _toy_parse_result(step_shift=0, reverse_insert=False)
    remapped = _toy_parse_result(step_shift=100, reverse_insert=False)

    base_sig = _toy_signatures(base)
    remapped_sig = _toy_signatures(remapped)
    assert base_sig == remapped_sig


def test_engine_metamorphic_entity_reordering_is_stable():
    base = _toy_parse_result(step_shift=0, reverse_insert=False)
    reordered = _toy_parse_result(step_shift=0, reverse_insert=True)

    base_sig = _toy_signatures(base)
    reordered_sig = _toy_signatures(reordered)
    assert base_sig == reordered_sig


def test_engine_length_quantization_is_unit_normalized():
    mm_context = {"unit_factors": {"LENGTHUNIT": 0.001}}
    m_context = {"unit_factors": {"LENGTHUNIT": 1.0}}
    q_mm = _quantize_real(1000.0, measure_type="IfcLengthMeasure", attr_name="Length", unit_context=mm_context)
    q_m = _quantize_real(1.0, measure_type="IfcLengthMeasure", attr_name="Length", unit_context=m_context)
    assert q_mm == q_m


def test_engine_schema_support_includes_ifc4_and_ifc2x3():
    assert "IFC4" in SUPPORTED_SCHEMA_PREFIXES
    assert "IFC2X3" in SUPPORTED_SCHEMA_PREFIXES


def test_engine_guid_collision_scan_counts_duplicates():
    signatures = {
        1: SignatureVector(1, "A", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
        2: SignatureVector(2, "A", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
        3: SignatureVector(3, "B", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
    }
    assert _guid_collision_count(signatures) == 1


def test_engine_stream_ndjson_has_header_and_end():
    old_path = str(FIXTURES / "house_v1.ifc")
    new_path = str(FIXTURES / "house_v2.ifc")
    lines = list(stream_diff_files(old_path, new_path, mode="ndjson"))
    assert json.loads(lines[0])["record_type"] == "header"
    assert json.loads(lines[-1])["record_type"] == "end"


def test_engine_stream_chunked_json_has_header_and_end():
    old_path = str(FIXTURES / "house_v1.ifc")
    new_path = str(FIXTURES / "house_v2.ifc")
    lines = list(stream_diff_files(old_path, new_path, mode="chunked_json", chunk_size=2))
    assert json.loads(lines[0])["chunk_type"] == "header"
    assert json.loads(lines[-1])["chunk_type"] == "end"


def _toy_parse_result(*, step_shift: int, reverse_insert: bool) -> ParseResult:
    wall = 1 + step_shift
    pset = 2 + step_shift
    rel = 3 + step_shift
    entities: list[tuple[int, ParsedEntity]] = [
        (
            wall,
            ParsedEntity(
                step_id=wall,
                entity_type="IfcWall",
                canonical_class="IfcWall",
                global_id="WALL_GUID",
                attributes={
                    "Name": {"kind": "string", "value": "Wall A"},
                    "ObjectPlacement": {"kind": "ref"},
                },
                refs=[
                    EntityRef(
                        source_step=wall,
                        target_step=pset,
                        source_type="IfcWall",
                        target_type="IfcPropertySet",
                        attr_name="HasProperties",
                        path="/HasProperties",
                    )
                ],
                is_product=True,
                is_spatial=False,
            ),
        ),
        (
            pset,
            ParsedEntity(
                step_id=pset,
                entity_type="IfcPropertySet",
                canonical_class="IfcPropertySet",
                global_id=None,
                attributes={"Name": {"kind": "string", "value": "Pset_WallCommon"}},
                refs=[],
                is_product=False,
                is_spatial=False,
            ),
        ),
        (
            rel,
            ParsedEntity(
                step_id=rel,
                entity_type="IfcRelDefinesByProperties",
                canonical_class="IfcRelDefinesByProperties",
                global_id="REL_GUID",
                attributes={},
                refs=[
                    EntityRef(
                        source_step=rel,
                        target_step=wall,
                        source_type="IfcRelDefinesByProperties",
                        target_type="IfcWall",
                        attr_name="RelatedObjects",
                        path="/RelatedObjects/0",
                    ),
                    EntityRef(
                        source_step=rel,
                        target_step=pset,
                        source_type="IfcRelDefinesByProperties",
                        target_type="IfcPropertySet",
                        attr_name="RelatingPropertyDefinition",
                        path="/RelatingPropertyDefinition",
                    ),
                ],
                is_product=False,
                is_spatial=False,
            ),
        ),
    ]

    if reverse_insert:
        entities = list(reversed(entities))

    return ParseResult(
        filepath="toy.ifc",
        schema="IFC2X3",
        index={},
        entities={step: ent for step, ent in entities},
        incoming_refs={},
        unit_context={"unit_factors": {"LENGTHUNIT": 1.0}},
        diagnostics=ParseDiagnostics(),
    )


def _toy_signatures(parse_result: ParseResult) -> dict[str, tuple[str, str, str]]:
    edges = build_edge_set(parse_result)
    merkle = compute_merkle_hashes(parse_result, edges)
    topology = compute_topology_hashes(parse_result, edges, merkle)
    out: dict[str, tuple[str, str, str]] = {}
    for entity in parse_result.entities.values():
        if not entity.global_id:
            continue
        if not (entity.is_product or entity.is_spatial):
            continue
        out[entity.global_id] = (
            merkle[entity.step_id]["geometry"],
            merkle[entity.step_id]["data"],
            topology[entity.step_id],
        )
    return out
