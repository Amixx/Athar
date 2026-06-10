from __future__ import annotations

import pytest

from athar.bottom.constants import CANON_VERSION
from athar.bottom.edge_policy import EDGE_POLICY_TABLE
from athar.bottom.signatures import build_signature_bundle
from athar.engine import diff_files
from tests.corpus import CORPUS, corpus_path

OLD_IFC = str(CORPUS["bl_v1"].path)
NEW_IFC = str(CORPUS["bl_v2"].path)


@pytest.fixture(scope="module", autouse=True)
def _corpus_files_present():
    corpus_path("bl_v1")
    corpus_path("bl_v2")


def test_edge_policy_table_contains_required_engine_relationships():
    relationships = {row.relationship for row in EDGE_POLICY_TABLE}
    required = {
        "IfcRelDefinesByProperties",
        "IfcRelAssociatesMaterial",
        "IfcRelContainedInSpatialStructure",
        "IfcRelAggregates",
        "IfcRelVoidsElement",
        "IfcRelFillsElement",
        "IfcRelConnectsPathElements",
        "IfcRelConnectsElements",
        "IfcRelDefinesByType",
    }
    assert required.issubset(relationships)


def test_edge_policy_table_has_no_duplicate_rule_keys():
    keys = [(r.relationship, r.source_attr, r.target_attr, r.classification, r.domain, r.bidirectional) for r in EDGE_POLICY_TABLE]
    assert len(keys) == len(set(keys))


def test_signature_bundle_stamps_canon_version():
    bundle = build_signature_bundle(OLD_IFC)
    assert bundle.canon_version == CANON_VERSION
    assert bundle.signatures
    assert all(sig.canon_version == CANON_VERSION for sig in bundle.signatures.values())


def test_diff_report_stamps_canon_version():
    report = diff_files(OLD_IFC, NEW_IFC)
    assert report["canon_version"] == CANON_VERSION
