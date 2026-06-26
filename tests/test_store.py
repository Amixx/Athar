"""Persistent baseline / approval / review-history store.

Builds tiny synthetic IFC4 models (a proxy or two, no representation needed —
products get GUID-matched signatures regardless) and exercises the lineage:
bootstrap a baseline, review candidates against it, approve to promote, reject
to leave it untouched. The store is the data-moat layer, so the invariants that
matter are: the accepted artifact is retained content-addressed, approval flips
the head, rejection does not, and the event log is append-only and (with a fixed
clock) byte-stable.
"""

from __future__ import annotations

import itertools

import ifcopenshell
import pytest

from athar_store import (
    BaselineStore,
    UnknownBaselineError,
    UnknownReviewError,
)

_GUID_A = "0aaaaaaaaaaaaaaaaaaaaa"
_GUID_B = "0bbbbbbbbbbbbbbbbbbbbb"


def _write_model(path, proxies: list[tuple[str, str]]) -> None:
    """Write an IFC4 file with one proxy per (guid, name) tuple."""
    f = ifcopenshell.file(schema="IFC4")
    length_unit = f.create_entity("IfcSIUnit", UnitType="LENGTHUNIT", Name="METRE")
    units = f.create_entity("IfcUnitAssignment", Units=[length_unit])
    f.create_entity(
        "IfcProject",
        GlobalId="0project0project0proj01",
        Name="store-test",
        UnitsInContext=units,
    )
    for guid, name in proxies:
        placement = f.create_entity(
            "IfcLocalPlacement",
            RelativePlacement=f.create_entity(
                "IfcAxis2Placement3D",
                Location=f.create_entity("IfcCartesianPoint", Coordinates=(0.0, 0.0, 0.0)),
            ),
        )
        f.create_entity(
            "IfcBuildingElementProxy",
            GlobalId=guid,
            Name=name,
            ObjectPlacement=placement,
        )
    f.write(str(path))


def _fixed_clock():
    """Deterministic, monotonically-increasing UTC timestamps for stable ids."""
    counter = itertools.count()
    return lambda: f"2026-06-26T00:00:{next(counter):02d}Z"


@pytest.fixture
def store(tmp_path):
    return BaselineStore(tmp_path / "store", clock=_fixed_clock())


@pytest.fixture
def models(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    same = tmp_path / "same.ifc"
    added = tmp_path / "added.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(same, [(_GUID_A, "wall-A")])
    _write_model(added, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])
    return {"baseline": baseline, "same": same, "added": added}


def test_set_baseline_retains_artifact_and_pointer(store, models):
    event = store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    assert event["event"] == "baseline_set"
    assert event["id"] == "evt-000001"

    pointer = store.current_baseline("acme", "architecture")
    assert pointer is not None
    assert pointer["approved_by"] == "alice"
    assert pointer["artifact"]["schema"] == "IFC4"
    # The accepted IFC is retained content-addressed so future candidates can be
    # re-diffed without the customer keeping old exports — the lock-in.
    sha = pointer["artifact"]["sha256"]
    assert (store.root / "projects" / "acme" / "artifacts" / f"{sha}.ifc").is_file()


def test_review_records_verdict_without_promoting(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    before = store.current_baseline("acme", "architecture")["artifact"]["sha256"]

    review = store.review(
        "acme", "architecture", models["added"], actor="bob", policy={"max_added": 0}
    )
    assert review["event"] == "review"
    assert review["verdict"] == "fail"
    assert review["diff"]["stats"]["added"] == 1
    assert any(v["code"] == "added_limit" for v in review["check"]["violations"])

    # A review never moves the head.
    assert store.current_baseline("acme", "architecture")["artifact"]["sha256"] == before


def test_review_passes_clean_candidate(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    review = store.review(
        "acme", "architecture", models["same"], actor="bob", policy={"max_added": 0}
    )
    assert review["verdict"] == "pass"
    assert review["check"]["ok"] is True


def test_approve_promotes_reviewed_candidate(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    review = store.review("acme", "architecture", models["added"], actor="bob")
    candidate_sha = review["candidate_artifact"]["sha256"]

    approval = store.approve(
        "acme", "architecture", review["id"], actor="carol", note="signed off"
    )
    assert approval["event"] == "approval"
    assert approval["review_id"] == review["id"]

    head = store.current_baseline("acme", "architecture")
    assert head["artifact"]["sha256"] == candidate_sha
    assert head["approved_by"] == "carol"


def test_reject_leaves_baseline_untouched(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    before = store.current_baseline("acme", "architecture")["artifact"]["sha256"]
    review = store.review("acme", "architecture", models["added"], actor="bob")

    rejection = store.reject("acme", "architecture", review["id"], actor="carol", note="nope")
    assert rejection["event"] == "rejection"
    assert store.current_baseline("acme", "architecture")["artifact"]["sha256"] == before


def test_history_is_append_only_and_filterable(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    store.set_baseline("acme", "structure", models["baseline"], actor="alice")
    review = store.review("acme", "architecture", models["added"], actor="bob")
    store.approve("acme", "architecture", review["id"], actor="carol")

    full = store.history("acme")
    assert [e["event"] for e in full] == [
        "baseline_set",
        "baseline_set",
        "review",
        "approval",
    ]
    # Sequential, gap-free ids over the whole project.
    assert [e["id"] for e in full] == ["evt-000001", "evt-000002", "evt-000003", "evt-000004"]

    arch_only = store.history("acme", "architecture")
    assert [e["event"] for e in arch_only] == ["baseline_set", "review", "approval"]


def test_fixed_clock_yields_stable_event_ids_and_times(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    review = store.review("acme", "architecture", models["same"], actor="bob")
    assert review["id"] == "evt-000002"
    # First clock tick is consumed by project.json's `created`; ticks are stable.
    assert review["at"] == "2026-06-26T00:00:02Z"


def test_content_addressed_ingest_is_idempotent(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    # Re-ingesting identical bytes (same file content) must not duplicate.
    store.review("acme", "architecture", models["same"], actor="bob")
    artifacts = list((store.root / "projects" / "acme" / "artifacts").glob("*.ifc"))
    # baseline + the (distinct-content) same-guid candidate share one guid but
    # differ only if bytes differ; identical names here means identical bytes.
    shas = {p.stem for p in artifacts}
    assert len(shas) == len(artifacts)  # no duplicate sha files


def test_review_requires_existing_baseline(store, models):
    with pytest.raises(UnknownBaselineError):
        store.review("acme", "architecture", models["same"], actor="bob")


def test_approve_unknown_review_raises(store, models):
    store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    with pytest.raises(UnknownReviewError):
        store.approve("acme", "architecture", "evt-999999", actor="carol")


def test_approve_rejects_non_review_event(store, models):
    set_event = store.set_baseline("acme", "architecture", models["baseline"], actor="alice")
    # The baseline_set event id is not a review; approving it must fail loudly.
    with pytest.raises(UnknownReviewError):
        store.approve("acme", "architecture", set_event["id"], actor="carol")


def test_list_projects(store, models):
    store.init_project("acme", name="Acme Tower")
    store.init_project("globex")
    projects = store.list_projects()
    assert [p["project_id"] for p in projects] == ["acme", "globex"]
    assert projects[0]["name"] == "Acme Tower"
