"""A killed writer leaves a complete old or new record after recovery."""

from __future__ import annotations

import hashlib
import multiprocessing
import os

import pytest

from athar_store import BaselineStore
from tests.test_store import _GUID_A, _GUID_B, _write_model


def _crash_writer(root, candidate, action, review_id, step):
    def fail(at):
        if at == step:
            os._exit(73)

    store = BaselineStore(root, fault_hook=fail)
    if action == "review":
        store.review("p", "m", candidate, actor="b")
    elif action == "replace":
        store.set_baseline("p", "m", candidate, actor="b", replace=True)
    else:
        store.approve("p", "m", review_id, actor="c")


@pytest.mark.parametrize("action,step,committed", [
    ("review", "artifact_durable", False),
    ("review", "report_durable", False),
    ("review", "history_durable", True),
    ("replace", "artifact_durable", False),
    ("replace", "pointer_prepared", False),
    ("replace", "history_durable", True),
    ("replace", "pointer_durable", True),
    ("approve", "pointer_prepared", False),
    ("approve", "history_durable", True),
    ("approve", "pointer_durable", True),
])
def test_crashed_writer_recovers_complete_record(tmp_path, action, step, committed):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    root = tmp_path / "store"
    store = BaselineStore(root)
    initial = store.set_baseline("p", "m", baseline, actor="a")
    review = store.review("p", "m", candidate, actor="b") if action == "approve" else None
    count = len(store.history("p"))
    worker = multiprocessing.get_context("spawn").Process(
        target=_crash_writer,
        args=(str(root), str(candidate), action, review and review["id"], step),
    )
    worker.start()
    worker.join(30)
    assert worker.exitcode == 73
    reopened = BaselineStore(root)
    events = reopened.history("p")
    assert len(events) == count + int(committed)
    pointer = reopened.current_baseline("p", "m")
    expected_sha = (
        hashlib.sha256(candidate.read_bytes()).hexdigest()
        if committed and action != "review" else initial["artifact"]["sha256"]
    )
    assert pointer["artifact"]["sha256"] == expected_sha
    leftover = [
        f"reports/evt-{count + 1:06d}.json.gz: report of an interrupted review; "
        "removed at the next write"
    ]
    result = reopened.verify("p")
    assert result["problems"] == []
    assert result["notes"] == (leftover if (action, step) == ("review", "report_durable") else [])
    for event in events:
        if event["event"] == "review":
            assert reopened.review_report("p", event["id"])["stats"]["added"] == 1
        for key in ("artifact", "baseline_artifact", "candidate_artifact"):
            if key in event:
                sha = event[key]["sha256"]
                out = reopened.export_artifact("p", sha, tmp_path / "out.ifc")
                assert hashlib.sha256(out.read_bytes()).hexdigest() == sha


@pytest.mark.parametrize("next_write", ["review", "approve", "reject", "replace"])
def test_next_write_removes_interrupted_review_report(tmp_path, next_write):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    second = tmp_path / "second.ifc"
    _write_model(baseline, [(_GUID_A, "A")])
    _write_model(candidate, [(_GUID_A, "A"), (_GUID_B, "B")])
    _write_model(second, [(_GUID_B, "B")])
    root = tmp_path / "store"
    store = BaselineStore(root)
    store.set_baseline("p", "m", baseline, actor="a")
    pending = store.review("p", "m", candidate, actor="b")
    worker = multiprocessing.get_context("spawn").Process(
        target=_crash_writer, args=(str(root), str(candidate), "review", None, "report_durable")
    )
    worker.start()
    worker.join(30)
    assert worker.exitcode == 73
    leftover = root / "projects" / "p" / "reports" / "evt-000003.json.gz"
    assert leftover.is_file()

    if next_write == "review":
        event = store.review("p", "m", second, actor="b")
    elif next_write == "approve":
        event = store.approve("p", "m", pending["id"], actor="c")
    elif next_write == "reject":
        event = store.reject("p", "m", pending["id"], actor="c")
    else:
        event = store.set_baseline("p", "m", second, actor="a", replace=True)

    assert event["id"] == "evt-000003"
    assert store.verify("p") == {"problems": [], "notes": []}
    if next_write == "review":
        assert store.review_report("p", event["id"])["stats"]["deleted"] == 1
    else:
        assert not leftover.exists()
