from __future__ import annotations

import json
import uuid
from pathlib import Path

import ifcopenshell
import ifcopenshell.guid
import pytest

from athar.bottom.constants import SUPPORTED_SCHEMA_PREFIXES
from athar.bottom.edge_policy import build_edge_set
from athar.bottom.merkle import compute_merkle_hashes
from athar.bottom.parser import _quantize_real
from athar.bottom.types import EntityRef, ParseDiagnostics, ParseResult, ParsedEntity, SignatureVector
from athar.bottom.wl_gossip import compute_topology_hashes
from athar.engine import _assert_schema_compatible, _guid_collision_count
from athar.engine import diff_files, stream_diff_files


REAL_WORLD = Path(__file__).resolve().parent.parent / "real-world-test"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
OLD_IFC = str(REAL_WORLD / "Building-Landscaping-v1.ifc")
NEW_IFC = str(REAL_WORLD / "Building-Landscaping-v2.ifc")
IFC2X3_MODEL = str(REAL_WORLD / "Duplex-Architecture.ifc")
EMPTY_MODEL = str(FIXTURES / "tiny_no_products.ifc")
SECTIONS = ("added", "deleted", "modified", "unchanged")
_GUID_SCRAMBLE_NAMESPACE = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")


def _require_real_content(path: str) -> None:
    with open(path, "rb") as fh:
        if fh.read(64).startswith(b"version https://git-lfs"):
            pytest.fail(f"{path} is an unfetched git-lfs pointer; run `git lfs pull`")


_require_real_content(OLD_IFC)
_require_real_content(NEW_IFC)
_require_real_content(IFC2X3_MODEL)


@pytest.fixture(scope="module")
def guid_scrambled_pair(tmp_path_factory):
    """(clean, scrambled) copies of NEW_IFC that differ only in GlobalId values.

    Both files go through the same ifcopenshell write path so the GUID rewrite
    is the only delta between them.
    """
    base = tmp_path_factory.mktemp("guid-scramble")
    clean = str(base / "clean.ifc")
    scrambled = str(base / "scrambled.ifc")
    model = ifcopenshell.open(NEW_IFC)
    model.write(clean)
    for entity in model:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if isinstance(guid, str) and guid.strip():
            entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(_GUID_SCRAMBLE_NAMESPACE, guid).hex)
    model.write(scrambled)
    return clean, scrambled


def test_engine_diff_same_file_is_unchanged():
    report = diff_files(OLD_IFC, OLD_IFC)
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    assert report["stats"]["unchanged"] > 0
    assert report["canon_version"] == "athar-canon-v1"


def test_engine_diff_detects_changes_between_versions():
    # Pins observed behavior on the frozen Building-Landscaping v1/v2 exports:
    # one proxy element was deleted; the deletion ripples through the spatial
    # topology hashes of everything else, so the rest report as transitive
    # modifications matched by GlobalId.
    report = diff_files(OLD_IFC, NEW_IFC)
    assert report["engine"] == "athar"
    assert report["schemas"] == {"old": "IFC4", "new": "IFC4"}
    assert report["stats"]["old_signatures"] == 8
    assert report["stats"]["new_signatures"] == 7
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 1
    assert report["stats"]["modified"] == 7
    assert report["stats"]["unchanged"] == 0
    assert report["stats"]["modified_match_reasons"] == {"guid": 7}
    assert report["stats"]["modified_change_scope"] == {"intrinsic": 0, "transitive": 7, "mixed": 0}


def test_engine_report_sections_and_stats_are_consistent():
    report = diff_files(OLD_IFC, NEW_IFC)
    stats = report["stats"]
    for section in SECTIONS:
        assert stats[section] == len(report[section])
    assert stats["deleted"] + stats["modified"] + stats["unchanged"] == stats["old_signatures"]
    assert stats["added"] + stats["modified"] + stats["unchanged"] == stats["new_signatures"]
    assert stats["dropped_matches"] == 0


def test_engine_report_matcher_diagnostics_are_consistent():
    report = diff_files(OLD_IFC, NEW_IFC)
    stats = report["stats"]
    diag = stats["matcher_diagnostics"]
    assert diag["pools"] == {"old": stats["old_signatures"], "new": stats["new_signatures"]}
    assert diag["unmatched"] == {"old": stats["deleted"], "new": stats["added"]}
    assert sum(diag["matched_by_tier"].values()) == stats["modified"] + stats["unchanged"]


def test_engine_guid_scramble_alone_yields_zero_diff(guid_scrambled_pair):
    # Metamorphic invariant: rewriting every GlobalId must not invent any
    # added/deleted/modified entities; identity is recovered through the
    # structural tiers without GlobalId evidence.
    clean, scrambled = guid_scrambled_pair
    report = diff_files(clean, scrambled)
    stats = report["stats"]
    assert stats["added"] == 0
    assert stats["deleted"] == 0
    assert stats["modified"] == 0
    assert stats["unchanged"] == stats["new_signatures"] > 0
    diag = stats["matcher_diagnostics"]
    assert diag["matched_by_tier"]["guid"] == 0
    assert sum(diag["matched_by_tier"].values()) == stats["unchanged"]


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


def test_engine_rejects_cross_schema_comparison():
    _assert_schema_compatible("IFC4", "IFC4")
    with pytest.raises(ValueError, match="Schema mismatch"):
        _assert_schema_compatible("IFC2X3", "IFC4")


def test_engine_ifc2x3_same_file_is_unchanged():
    # The Building-Landscaping pair is IFC4; this is the only default-tier
    # end-to-end run through the IFC2X3 parse path (spatial roots, schema dict).
    report = diff_files(IFC2X3_MODEL, IFC2X3_MODEL)
    assert report["schemas"] == {"old": "IFC2X3", "new": "IFC2X3"}
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 0
    assert report["stats"]["modified"] == 0
    assert report["stats"]["unchanged"] > 100


def test_engine_model_without_products_yields_empty_report():
    report = diff_files(EMPTY_MODEL, EMPTY_MODEL)
    for section in SECTIONS:
        assert report["stats"][section] == 0
        assert report[section] == []
    assert report["stats"]["old_signatures"] == 0
    assert report["stats"]["matcher_diagnostics"]["matched_by_tier"]["guid"] == 0


def test_engine_output_is_byte_identical_across_runs():
    # Run-to-run determinism of the full wire output, without freezing the
    # format the way golden files would. The bundle cache is cleared between
    # runs so the second run re-executes the whole parse/signature pipeline.
    import athar.engine as engine_mod

    first = json.dumps(diff_files(OLD_IFC, NEW_IFC), sort_keys=True)
    engine_mod._BUNDLE_CACHE.clear()
    second = json.dumps(diff_files(OLD_IFC, NEW_IFC), sort_keys=True)
    assert first == second

    ndjson_a = list(stream_diff_files(OLD_IFC, NEW_IFC, mode="ndjson"))
    ndjson_b = list(stream_diff_files(OLD_IFC, NEW_IFC, mode="ndjson"))
    assert ndjson_a == ndjson_b

    chunked_a = list(stream_diff_files(OLD_IFC, NEW_IFC, mode="chunked_json", chunk_size=3))
    chunked_b = list(stream_diff_files(OLD_IFC, NEW_IFC, mode="chunked_json", chunk_size=3))
    assert chunked_a == chunked_b


def test_engine_guid_collision_scan_counts_duplicates():
    signatures = {
        1: SignatureVector(1, "A", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
        2: SignatureVector(2, "A", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
        3: SignatureVector(3, "B", "IfcWall", "IfcWall", "g", "d", "t", None, None, None, "athar-canon-v1"),
    }
    assert _guid_collision_count(signatures) == 1


def test_engine_stream_ndjson_matches_report():
    report = diff_files(OLD_IFC, NEW_IFC)
    lines = [json.loads(line) for line in stream_diff_files(OLD_IFC, NEW_IFC, mode="ndjson")]

    header, end = lines[0], lines[-1]
    assert header["record_type"] == "header"
    assert header["engine"] == "athar"
    assert header["canon_version"] == report["canon_version"]
    assert end["record_type"] == "end"
    assert end["stats"] == report["stats"]

    streamed_counts = {section: 0 for section in SECTIONS}
    for record in lines[1:-1]:
        streamed_counts[record["record_type"]] += 1
    assert streamed_counts == {section: report["stats"][section] for section in SECTIONS}


def test_engine_stream_chunked_json_matches_report():
    chunk_size = 3
    report = diff_files(OLD_IFC, NEW_IFC)
    chunks = [json.loads(line) for line in stream_diff_files(OLD_IFC, NEW_IFC, mode="chunked_json", chunk_size=chunk_size)]

    header, end = chunks[0], chunks[-1]
    assert header["chunk_type"] == "header"
    assert end["chunk_type"] == "end"
    assert end["stats"] == report["stats"]

    streamed_counts = {section: 0 for section in SECTIONS}
    for chunk in chunks[1:-1]:
        assert 1 <= len(chunk["items"]) <= chunk_size
        streamed_counts[chunk["chunk_type"]] += len(chunk["items"])
    assert streamed_counts == {section: report["stats"][section] for section in SECTIONS}


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
