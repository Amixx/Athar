from __future__ import annotations

from pathlib import Path

import pytest

from athar.bottom.constants import CANON_VERSION
from athar.bottom.edge_policy import EDGE_POLICY_TABLE
from athar.bottom.signatures import build_signature_bundle
from athar.engine import diff_files


REAL_WORLD = Path(__file__).resolve().parent.parent / "real-world-test"
OLD_IFC = str(REAL_WORLD / "Building-Landscaping-v1.ifc")
NEW_IFC = str(REAL_WORLD / "Building-Landscaping-v2.ifc")


def _require_real_content(path: str) -> None:
    with open(path, "rb") as fh:
        if fh.read(64).startswith(b"version https://git-lfs"):
            pytest.fail(f"{path} is an unfetched git-lfs pointer; run `git lfs pull`")


_require_real_content(OLD_IFC)
_require_real_content(NEW_IFC)


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
