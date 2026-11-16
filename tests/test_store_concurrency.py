"""Concurrent writers on one project: real processes, released together."""

from __future__ import annotations

import fcntl
import hashlib
import multiprocessing
import os
import traceback

from athar_store import BaselineStore
from tests.test_store import _GUID_A, _GUID_B, _write_model

_WRITERS = 6
_ROUNDS = 3


def _writer(root: str, candidates: list[str], index: int, barrier, errors) -> None:
    try:
        store = BaselineStore(root)
        barrier.wait()
        for round_index in range(_ROUNDS):
            candidate = candidates[(index + round_index) % len(candidates)]
            review = store.review("acme", "architecture", candidate, actor=f"writer-{index}")
            store.reject("acme", "architecture", review["id"], actor=f"writer-{index}")
    except BaseException:
        errors.put(traceback.format_exc())


def test_concurrent_writers_keep_ids_and_artifacts_intact(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    candidates = []
    for index in range(3):
        path = tmp_path / f"candidate-{index}.ifc"
        _write_model(path, [(_GUID_A, "wall-A"), (f"0c{index:020d}", f"wall-{index}")])
        candidates.append(str(path))

    root = tmp_path / "store"
    BaselineStore(root).set_baseline("acme", "architecture", baseline, actor="alice")

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(_WRITERS)
    errors = context.Queue()
    workers = [
        context.Process(target=_writer, args=(str(root), candidates, index, barrier, errors))
        for index in range(_WRITERS)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=300)
    failures = []
    while not errors.empty():
        failures.append(errors.get())
    assert failures == []
    assert [worker.exitcode for worker in workers] == [0] * _WRITERS

    store = BaselineStore(root)
    events = store.history("acme")
    expected_count = 1 + _WRITERS * _ROUNDS * 2
    assert [event["id"] for event in events] == [
        f"evt-{seq:06d}" for seq in range(1, expected_count + 1)
    ]

    shas = set()
    for event in events:
        for key in ("artifact", "baseline_artifact", "candidate_artifact"):
            if key in event:
                shas.add(event[key]["sha256"])
    assert len(shas) == 1 + len(candidates)
    for sha in sorted(shas):
        out = store.export_artifact("acme", sha, tmp_path / f"{sha}.ifc")
        assert hashlib.sha256(out.read_bytes()).hexdigest() == sha


def test_readers_are_not_blocked_while_a_review_stores_its_candidate(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(candidate, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])
    lock_path = tmp_path / "store" / "projects" / "acme" / ".lock"
    shared_lock_free: list[bool] = []

    def try_shared_lock(step):
        if step != "artifact_durable" or not lock_path.exists():
            return
        descriptor = os.open(lock_path, os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            shared_lock_free.append(True)
        except BlockingIOError:
            shared_lock_free.append(False)
        finally:
            os.close(descriptor)

    BaselineStore(tmp_path / "store").set_baseline("acme", "architecture", baseline, actor="a")
    store = BaselineStore(tmp_path / "store", fault_hook=try_shared_lock)
    review = store.review("acme", "architecture", candidate, actor="b")
    assert shared_lock_free == [True]
    assert store.has_artifact("acme", review["candidate_artifact"]["sha256"])
