from __future__ import annotations

import json

import pytest

from athar.bottom.constants import SUPPORTED_SCHEMA_PREFIXES
from athar.bottom.types import SignatureVector
from athar.engine import SchemaMismatchError, _assert_schema_compatible, _guid_collision_count
from athar.engine import diff_files, stream_diff_files
from tests.corpus import CORPUS, corpus_path

OLD_IFC = str(CORPUS["bl_v1"].path)
NEW_IFC = str(CORPUS["bl_v2"].path)
EMPTY_MODEL = str(CORPUS["tiny_no_products"].path)
SECTIONS = ("added", "deleted", "modified", "unchanged")


@pytest.fixture(scope="module", autouse=True)
def _corpus_files_present():
    for key in ("bl_v1", "bl_v2", "tiny_no_products"):
        corpus_path(key)


def test_engine_diff_detects_changes_between_versions():
    # Pins observed behavior on the frozen Building-Landscaping v1/v2 exports:
    # one proxy element was deleted; with radius-1 class-only WL gossip the
    # deletion ripples only to its direct spatial container, which reports as
    # the single transitive modification. Everything else stays unchanged.
    report = diff_files(OLD_IFC, NEW_IFC)
    assert report["engine"] == "athar"
    assert report["schemas"] == {"old": "IFC4", "new": "IFC4"}
    assert report["stats"]["old_signatures"] == 8
    assert report["stats"]["new_signatures"] == 7
    assert report["stats"]["added"] == 0
    assert report["stats"]["deleted"] == 1
    assert report["stats"]["modified"] == 1
    assert report["stats"]["unchanged"] == 6
    assert report["stats"]["matcher_diagnostics"]["matched_by_tier"] == {"guid": 7, "geometry_hash": 0}
    assert report["stats"]["modified_change_scope"] == {"intrinsic": 0, "transitive": 1, "mixed": 0}


def test_engine_schema_support_includes_ifc4_and_ifc2x3():
    assert "IFC4" in SUPPORTED_SCHEMA_PREFIXES
    assert "IFC2X3" in SUPPORTED_SCHEMA_PREFIXES


def test_engine_rejects_cross_schema_comparison():
    _assert_schema_compatible("IFC4", "IFC4")
    with pytest.raises(ValueError, match="Schema mismatch"):
        _assert_schema_compatible("IFC2X3", "IFC4")


def test_schema_mismatch_error_is_typed_and_structured():
    # Subclasses ValueError so existing handlers keep catching it, while
    # exposing the two schemas for callers that present a clear result.
    assert issubclass(SchemaMismatchError, ValueError)
    with pytest.raises(SchemaMismatchError) as excinfo:
        _assert_schema_compatible("IFC2X3", "IFC4")
    err = excinfo.value
    assert err.old_schema == "IFC2X3"
    assert err.new_schema == "IFC4"
    assert "IFC2X3" in str(err) and "IFC4" in str(err)


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
    assert header["audit"] == report["audit"]
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
    assert header["audit"] == report["audit"]
    assert end["chunk_type"] == "end"
    assert end["stats"] == report["stats"]

    streamed_counts = {section: 0 for section in SECTIONS}
    for chunk in chunks[1:-1]:
        assert 1 <= len(chunk["items"]) <= chunk_size
        streamed_counts[chunk["chunk_type"]] += len(chunk["items"])
    assert streamed_counts == {section: report["stats"][section] for section in SECTIONS}
