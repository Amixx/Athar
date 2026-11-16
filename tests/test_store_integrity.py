"""Public integrity checks for replacement, verification, and engine errors."""

from __future__ import annotations

import gzip
import hashlib
import json

import pytest

from athar_store import (
    ArtifactPack,
    BaselineStore,
    EngineError,
    StoreConflictError,
    StoreError,
    UnknownReviewError,
)
from athar_store.cli import build_store_parser, run_store
from tests.test_store import _GUID_A, _GUID_B, _write_model


def test_baseline_replacement_requires_flag_and_records_old_hash(tmp_path):
    first = tmp_path / "first.ifc"
    second = tmp_path / "second.ifc"
    _write_model(first, [(_GUID_A, "wall-A")])
    _write_model(second, [(_GUID_B, "wall-B")])
    store = BaselineStore(tmp_path / "store")
    initial = store.set_baseline("p", "m", first, actor="a")
    with pytest.raises(StoreConflictError, match="replace=True"):
        store.set_baseline("p", "m", second, actor="a")
    replacement = store.set_baseline("p", "m", second, actor="a", replace=True)
    assert replacement["replaced_baseline_sha256"] == initial["artifact"]["sha256"]


def test_verify_reports_corrupt_artifact_with_exit_two(tmp_path):
    model = tmp_path / "model.ifc"
    _write_model(model, [(_GUID_A, "wall-A")])
    root = tmp_path / "store"
    store = BaselineStore(root)
    event = store.set_baseline("p", "m", model, actor="a")
    artifacts = root / "projects" / "p" / "artifacts"
    meta = ArtifactPack(artifacts).meta(event["artifact"]["sha256"])
    suffix = ".ifc" if meta["encoding"] == "raw" else ".ifc.zst"
    (artifacts / f"{event['artifact']['sha256']}{suffix}").write_bytes(b"corrupt")
    problems = store.verify("p")["problems"]
    assert any("evt-000001" in problem and "artifact" in problem for problem in problems)
    args = build_store_parser().parse_args(["--root", str(root), "verify", "p"])
    assert run_store(args) == 2


def test_engine_refusal_is_store_error_without_history(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    bad = tmp_path / "bad.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    bad.write_text("not an IFC", encoding="utf-8")
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", baseline, actor="a")
    with pytest.raises(EngineError, match="candidate could not be compared"):
        store.review("p", "m", bad, actor="b")
    assert [event["event"] for event in store.history("p")] == ["baseline_set"]


@pytest.mark.parametrize("damage", ["missing", "truncated", "json"])
def test_verify_reads_every_review_report(tmp_path, damage):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", baseline, actor="a")
    review = store.review("p", "m", candidate, actor="b")
    assert store.verify("p")["problems"] == []
    report = store.root / "projects" / "p" / "reports" / f"{review['id']}.json.gz"
    if damage == "missing":
        report.unlink()
    elif damage == "truncated":
        report.write_bytes(report.read_bytes()[:12])
    else:
        report.write_bytes(gzip.compress(b"not JSON"))
    problems = store.verify("p")["problems"]
    assert any(review["id"] in problem and "report" in problem for problem in problems)


@pytest.mark.parametrize("damage", ["missing", "wrong_sha", "wrong_actor", "extra"])
def test_verify_rebuilds_full_pointer_from_history(tmp_path, capsys, damage):
    model = tmp_path / "model.ifc"
    _write_model(model, [(_GUID_A, "A")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", model, actor="a")
    pointer = store.root / "projects" / "p" / "baselines" / "m.json"
    expected = json.loads(pointer.read_text())
    if damage == "missing":
        pointer.unlink()
    elif damage == "extra":
        pointer.with_name("spurious.json").write_text(json.dumps(expected))
    else:
        edited = json.loads(pointer.read_text())
        if damage == "wrong_sha":
            edited["artifact"]["sha256"] = "0" * 64
        else:
            edited["approved_by"] = "impostor"
        pointer.write_text(json.dumps(edited))
    assert any("pointer" in problem for problem in store.verify("p")["problems"])
    args = build_store_parser().parse_args([
        "--root", str(store.root), "verify", "p", "--rebuild-pointers",
    ])
    assert run_store(args) == 2
    output = capsys.readouterr()
    assert json.loads(output.out)["problems"]
    assert json.loads(output.err)["pointer_changes"]
    assert store.verify("p")["problems"] == []
    assert store.current_baseline("p", "m") == expected


def test_verify_rejects_duplicate_ids_and_invalid_history(tmp_path):
    model = tmp_path / "model.ifc"
    _write_model(model, [(_GUID_A, "A")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", model, actor="a")
    history = store.root / "projects" / "p" / "history.jsonl"
    history.write_text(history.read_text() * 2)
    assert "history ids are not consecutive and unique" in store.verify("p")["problems"]
    history.write_text('{"id":')
    assert store.verify("p")["problems"][0].startswith("unreadable history:")


def _ifc2x3_model(path) -> None:
    import ifcopenshell

    model = ifcopenshell.file(schema="IFC2X3")
    model.create_entity("IfcProject", GlobalId="0project0project0proj01", Name="older")
    model.write(str(path))


def test_cli_schema_mismatch_exits_one_and_records_nothing(tmp_path, capsys):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _ifc2x3_model(candidate)
    root = tmp_path / "store"
    store = BaselineStore(root)
    store.set_baseline("p", "m", baseline, actor="a")
    artifacts = sorted(path.name for path in (root / "projects" / "p" / "artifacts").iterdir())
    args = build_store_parser().parse_args(
        ["--root", str(root), "review", "p", "m", str(candidate), "--actor", "b"]
    )
    assert run_store(args) == 1
    assert "Schema mismatch" in capsys.readouterr().err
    assert [event["event"] for event in store.history("p")] == ["baseline_set"]
    assert sorted(path.name for path in (root / "projects" / "p" / "artifacts").iterdir()) == artifacts
    assert not (root / "projects" / "p" / "reports").exists()


def test_missing_candidate_is_a_store_error_not_an_engine_refusal(tmp_path, capsys):
    baseline = tmp_path / "baseline.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    root = tmp_path / "store"
    store = BaselineStore(root)
    store.set_baseline("p", "m", baseline, actor="a")
    missing = tmp_path / "missing.ifc"
    with pytest.raises(StoreError, match="IFC file not found") as error:
        store.review("p", "m", missing, actor="b")
    assert not isinstance(error.value, EngineError)
    args = build_store_parser().parse_args(
        ["--root", str(root), "review", "p", "m", str(missing), "--actor", "b"]
    )
    assert run_store(args) == 1
    assert "IFC file not found" in capsys.readouterr().err
    assert [event["event"] for event in store.history("p")] == ["baseline_set"]


def test_writes_to_an_unknown_project_create_nothing(tmp_path, capsys):
    root = tmp_path / "store"
    store = BaselineStore(root)
    for decide in (store.approve, store.reject):
        with pytest.raises(StoreError, match="Unknown project ghost"):
            decide("ghost", "m", "evt-000001", actor="a")
    assert store.verify("ghost", rebuild_pointers=True)["problems"] == ["project ghost does not exist"]
    args = build_store_parser().parse_args(["--root", str(root), "verify", "ghost", "--rebuild-pointers"])
    assert run_store(args) == 2
    assert not (root / "projects" / "ghost").exists()


def test_orphan_report_is_not_served_and_verify_flags_it(tmp_path, capsys):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", baseline, actor="a")
    review = store.review("p", "m", candidate, actor="b")
    reports = store.root / "projects" / "p" / "reports"
    approval = store.approve("p", "m", review["id"], actor="c")
    assert approval["id"] == "evt-000003"
    stored = (reports / f"{review['id']}.json.gz").read_bytes()
    (reports / "evt-000003.json.gz").write_bytes(stored)
    with pytest.raises(UnknownReviewError, match="not a review"):
        store.review_report("p", "evt-000003")
    args = build_store_parser().parse_args(["--root", str(store.root), "report", "p", "evt-000003"])
    assert run_store(args) == 1
    assert "not a review" in capsys.readouterr().err
    assert store.verify("p")["problems"] == [
        "reports/evt-000003.json.gz: no review event references this report"
    ]


def _release_010_store(root, baseline, candidate) -> None:
    """Lay out a project as release 0.1.0 wrote it: raw artifacts, no reports."""
    project = root / "projects" / "p"
    (project / "artifacts").mkdir(parents=True)
    (project / "baselines").mkdir()
    (project / "project.json").write_text(
        json.dumps({"project_id": "p", "name": "p", "created": "2026-01-01T00:00:00Z"})
    )
    artifacts = []
    for path in (baseline, candidate):
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        (project / "artifacts" / f"{sha}.ifc").write_bytes(path.read_bytes())
        artifacts.append({"sha256": sha, "schema": "IFC4", "size_bytes": path.stat().st_size})
    events = [
        {
            "id": "evt-000001", "event": "baseline_set", "at": "2026-01-01T00:00:00Z",
            "model_key": "m", "actor": "a", "artifact": artifacts[0], "note": None,
        },
        {
            "id": "evt-000002", "event": "review", "at": "2026-01-02T00:00:00Z",
            "model_key": "m", "actor": "b", "baseline_artifact": artifacts[0],
            "candidate_artifact": artifacts[1], "verdict": "pass", "has_policy": False,
            "check": {"ok": True, "violations": []},
            "diff": {"schemas": {"old": "IFC4", "new": "IFC4"}, "stats": {"added": 1}},
        },
    ]
    (project / "history.jsonl").write_text(
        "".join(json.dumps(event, sort_keys=True) + "\n" for event in events)
    )
    pointer = {
        "model_key": "m", "artifact": artifacts[0], "approved_by": "a",
        "approved_at": "2026-01-01T00:00:00Z", "approval_event_id": "evt-000001",
        "established_event_id": "evt-000001",
    }
    (project / "baselines" / "m.json").write_text(json.dumps(pointer))


def test_release_010_reviews_without_reports_verify_clean(tmp_path, capsys):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    root = tmp_path / "store"
    _release_010_store(root, baseline, candidate)
    note = "evt-000002: no stored report (written before reports were kept)"
    assert BaselineStore(root).verify("p") == {"problems": [], "notes": [note]}
    args = build_store_parser().parse_args(["--root", str(root), "verify", "p"])
    assert run_store(args) == 0
    assert json.loads(capsys.readouterr().out) == {"problems": [], "notes": [note]}


def test_current_review_without_report_still_fails_verify(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("p", "m", baseline, actor="a")
    review = store.review("p", "m", candidate, actor="b")
    (store.root / "projects" / "p" / "reports" / f"{review['id']}.json.gz").unlink()
    assert store.verify("p") == {"problems": [f"{review['id']}: missing review report"], "notes": []}
