from __future__ import annotations

from athar.bottom.constants import CANON_VERSION
from athar.bottom.edge_policy import EDGE_POLICY_TABLE
from athar.bottom.signatures import build_signature_bundle
from athar.engine import diff_files


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
    bundle = build_signature_bundle("tests/fixtures/house_v1.ifc")
    assert bundle.canon_version == CANON_VERSION
    assert bundle.signatures
    assert all(sig.canon_version == CANON_VERSION for sig in bundle.signatures.values())


def test_diff_report_stamps_canon_version():
    report = diff_files("tests/fixtures/house_v1.ifc", "tests/fixtures/house_v2.ifc")
    assert report["canon_version"] == CANON_VERSION
