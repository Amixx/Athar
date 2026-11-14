"""Filesystem-backed baseline / approval / review-history store.

The store is the durable system of record that makes Athar's value compound
per project: it retains the accepted IFC baseline (content-addressed), an
immutable append-only event log of every review/approval/rejection, and a
materialized pointer to the currently-accepted version per model lineage.

Integration layer: depends on the core engine (`diff_files`) and policy gate
(`evaluate_report`), never the reverse.
"""

from __future__ import annotations

import fcntl
import gzip
import json
import os
import re
import shutil
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from importlib import metadata

from athar.bottom.constants import CANON_VERSION
from athar.bottom.parser import IfcParseError, open_ifc
from athar.check import evaluate_report
from athar.engine import diff_files

from ._io import atomic_file, mkdir_durable, sync_directory
from .pack import ArtifactPack, PackError

_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_REPORT_NAME_RE = re.compile(r"^evt-(\d{6,})\.json\.gz$")
_DEFAULT_ROOT_ENV = "ATHAR_STORE_ROOT"


class StoreError(Exception):
    """Base error for store operations."""


class UnknownBaselineError(StoreError):
    """Raised when an operation needs a baseline that does not exist yet."""


class UnknownReviewError(StoreError):
    """Raised when an approval/rejection references a missing review event."""


class StoreConflictError(StoreError):
    """The operation conflicts with the store's current state."""


class EngineError(StoreError):
    """The candidate could not be parsed or compared with the baseline."""


class StaleBaselineError(StoreConflictError):
    """Raised when the baseline changed while a review was being computed."""


class ReviewStateError(StoreConflictError):
    """A decision targets a review that is no longer pending.

    ``state`` is ``approved``, ``rejected`` or ``stale``; ``event_id`` is the
    event that decided the review or moved the baseline past it.
    """

    def __init__(self, review_id: str, state: str, event_id: str) -> None:
        if state == "approved":
            message = f"Review {review_id} is already approved by event {event_id}."
        elif state == "rejected":
            message = f"Review {review_id} was rejected by event {event_id}."
        else:
            message = f"Review {review_id} is stale: the baseline changed by event {event_id}."
        super().__init__(message)
        self.review_id = review_id
        self.state = state
        self.event_id = event_id


_held_locks = threading.local()


@contextmanager
def _project_lock(project_dir: Path, *, exclusive: bool) -> Iterator[None]:
    """Hold the project's ``flock`` (exclusive or shared) for this thread.

    Re-entrant per thread: a nested request under an already held lock of the
    same or stronger mode is a no-op. A shared lock never upgrades.
    """
    held: dict[str, bool] = _held_locks.__dict__.setdefault("modes", {})
    key = str(project_dir.resolve())
    if key in held:
        if exclusive and not held[key]:
            raise RuntimeError(f"cannot upgrade a shared store lock on {project_dir}")
        yield
        return
    if not exclusive and not project_dir.is_dir():
        yield
        return
    mkdir_durable(project_dir)
    lock_handle = os.open(project_dir / ".lock", os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        held[key] = exclusive
        try:
            yield
        finally:
            del held[key]
            fcntl.flock(lock_handle, fcntl.LOCK_UN)
    finally:
        os.close(lock_handle)


def default_root() -> Path:
    """Resolve the store root from env or the per-user default."""
    env = os.environ.get(_DEFAULT_ROOT_ENV)
    if env:
        return Path(env)
    return Path.home() / ".athar" / "store"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_actor(actor: str | None) -> str:
    if not isinstance(actor, str) or not actor.strip():
        raise StoreError("An actor is required: name who performs this store operation.")
    return actor.strip()


def _producer() -> dict[str, str]:
    try:
        version = metadata.version("athar-ifc")
    except metadata.PackageNotFoundError as exc:
        raise StoreError(
            "athar-ifc is not installed, so events cannot record the producing version; "
            "install it with `make native-build` or `pip install -e .`"
        ) from exc
    return {"athar_version": version, "canon_version": CANON_VERSION}


def _validate_id(value: str, kind: str) -> str:
    if not isinstance(value, str) or not _ID_RE.match(value):
        raise StoreError(f"Invalid {kind} {value!r}: use only letters, digits, '.', '_', '-'")
    return value


class BaselineStore:
    """Persistent per-project baseline / approval / review history.

    Args:
        root: directory under which projects live. Defaults to
            ``$ATHAR_STORE_ROOT`` or ``~/.athar/store``.
        clock: injectable UTC-timestamp factory (for deterministic tests).
    """

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        clock: Callable[[], str] | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        self.root = Path(root) if root is not None else default_root()
        self._clock = clock or _utc_now
        self._fault_hook = fault_hook

    def _fault(self, step: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(step)

    # ----- project lifecycle -------------------------------------------------

    def init_project(self, project_id: str, *, name: str | None = None) -> dict[str, Any]:
        """Create (or return) a project namespace."""
        _validate_id(project_id, "project_id")
        project_dir = self._project_dir(project_id)
        meta_path = project_dir / "project.json"
        with self._locked(project_id, exclusive=True, create=True):
            if meta_path.exists():
                return _read_json(meta_path)
            mkdir_durable(project_dir / "artifacts")
            mkdir_durable(project_dir / "baselines")
            meta = {"project_id": project_id, "name": name or project_id, "created": self._clock()}
            _write_json(meta_path, meta)
            return meta

    def list_projects(self) -> list[dict[str, Any]]:
        """Every registered project, ordered by id."""
        projects_root = self.root / "projects"
        if not projects_root.is_dir():
            return []
        out: list[dict[str, Any]] = []
        for child in sorted(projects_root.iterdir()):
            meta = child / "project.json"
            if meta.is_file():
                out.append(_read_json(meta))
        return out

    # ----- baselines ---------------------------------------------------------

    def set_baseline(
        self,
        project_id: str,
        model_key: str,
        ifc_path: str | Path,
        *,
        actor: str,
        note: str | None = None,
        source_name: str | None = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        """Bootstrap the first accepted version for a model lineage.

        No diff is run: this establishes the reference every later candidate is
        compared against. The file is ingested into the content-addressed store.
        ``source_name`` is the delivery's file name as received; it defaults to
        the name of ``ifc_path``.
        """
        actor = _require_actor(actor)
        _validate_id(model_key, "model_key")
        with self._locked(project_id, exclusive=True, create=True):
            self.init_project(project_id)
            previous = self.current_baseline(project_id, model_key)
            if previous is not None and not replace:
                raise StoreConflictError(
                    f"Baseline for {project_id}/{model_key} already exists; pass replace=True."
                )
            src = Path(ifc_path)
            if not src.is_file():
                raise StoreError(f"IFC file not found: {ifc_path}")
            try:
                schema = open_ifc(str(src)).schema
            except IfcParseError as exc:
                raise EngineError(str(exc)) from exc
            artifact = self._ingest_artifact(project_id, src, schema)
            event = self._append_event(
                project_id,
                self._read_history(project_id),
                {
                    "event": "baseline_set",
                    "model_key": model_key,
                    "actor": actor,
                    "source_name": source_name or Path(ifc_path).name,
                    "artifact": artifact,
                    "note": note,
                    **({"replaced_baseline_sha256": previous["artifact"]["sha256"]} if previous else {}),
                },
            )
            self._write_baseline_pointer(
                project_id,
                model_key,
                artifact=artifact,
                approved_by=actor,
                approved_at=event["at"],
                approval_event_id=event["id"],
                established_event_id=event["id"],
            )
            return event

    def current_baseline(self, project_id: str, model_key: str) -> dict[str, Any] | None:
        """Return the accepted-version pointer, or None if unset."""
        _validate_id(project_id, "project_id")
        _validate_id(model_key, "model_key")
        pointer = self._baseline_path(project_id, model_key)
        with self._locked(project_id, exclusive=False):
            if not pointer.is_file():
                return None
            return _read_json(pointer)

    def list_baselines(self, project_id: str) -> list[dict[str, Any]]:
        """Every model lineage's current baseline pointer, ordered by key."""
        _validate_id(project_id, "project_id")
        baselines_dir = self._project_dir(project_id) / "baselines"
        with self._locked(project_id, exclusive=False):
            if not baselines_dir.is_dir():
                return []
            return [_read_json(path) for path in sorted(baselines_dir.glob("*.json"))]

    # ----- review / approve / reject ----------------------------------------

    def review(
        self,
        project_id: str,
        model_key: str,
        candidate_path: str | Path,
        *,
        actor: str,
        policy: dict[str, Any] | None = None,
        source_name: str | None = None,
    ) -> dict[str, Any]:
        """Diff a candidate against the current baseline and record the verdict.

        Does not promote the candidate. The candidate is ingested at review time
        so the verdict is reproducible and a later approval is just a pointer
        flip. Raises ``UnknownBaselineError`` if no baseline exists yet.
        """
        actor = _require_actor(actor)
        candidate_path = Path(candidate_path)
        if not candidate_path.is_file():
            raise StoreError(f"IFC file not found: {candidate_path}")
        with tempfile.TemporaryDirectory(prefix="athar-store-") as tmp:
            snapshot = Path(tmp) / "candidate.ifc"
            shutil.copyfile(candidate_path, snapshot)
            with self._locked(project_id, exclusive=False):
                baseline = self.current_baseline(project_id, model_key)
                if baseline is None:
                    raise UnknownBaselineError(
                        f"No baseline for {project_id}/{model_key}; call set_baseline first."
                    )
                baseline_copy = self._materialize(
                    project_id, baseline["artifact"]["sha256"], Path(tmp) / "baseline.ifc"
                )
            try:
                report = diff_files(str(baseline_copy), str(snapshot))
            except ValueError as exc:
                raise EngineError(f"candidate could not be compared: {exc}") from exc
            self._fault("candidate_compared")
            check = evaluate_report(report, policy or {})
            baseline_artifact = baseline["artifact"]
            candidate_artifact = self._ingest_artifact(
                project_id,
                snapshot,
                report["schemas"]["new"],
                base_sha256=baseline_artifact["sha256"],
            )

            with self._locked(project_id, exclusive=True):
                current = self.current_baseline(project_id, model_key)
                if current is None or current["established_event_id"] != baseline["established_event_id"]:
                    changed_by = current["established_event_id"] if current else "a removal"
                    raise StaleBaselineError(
                        f"The baseline of {project_id}/{model_key} changed ({changed_by}) "
                        "while the review ran; review the candidate again."
                    )
                events = self._read_history(project_id)
                lineage = [event for event in events if event.get("model_key") == model_key]
                conflicts = self._detect_conflicts(project_id, lineage, report)
                return self._append_event(
                    project_id,
                    events,
                    {
                        "event": "review",
                        "model_key": model_key,
                        "actor": actor,
                        "source_name": source_name or candidate_path.name,
                        "baseline_artifact": baseline_artifact,
                        "candidate_artifact": candidate_artifact,
                        "verdict": "pass" if check["ok"] else "fail",
                        "has_policy": bool(policy),
                        "check": check,
                        "conflicts": conflicts,
                        "diff": {
                            "schemas": report.get("schemas"),
                            "stats": _stats_summary(report),
                        },
                    },
                    report=report,
                )

    def approve(
        self,
        project_id: str,
        model_key: str,
        review_id: str,
        *,
        actor: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Promote a reviewed candidate to the new accepted baseline."""
        actor = _require_actor(actor)
        with self._locked(project_id, exclusive=True):
            events = self._read_history(project_id)
            review = self._pending_review(project_id, events, model_key, review_id)
            artifact = review["candidate_artifact"]
            event = self._append_event(
                project_id,
                events,
                {
                    "event": "approval",
                    "model_key": model_key,
                    "actor": actor,
                    "review_id": review_id,
                    "source_name": review.get("source_name"),
                    "artifact": artifact,
                    "note": note,
                },
            )
            self._write_baseline_pointer(
                project_id,
                model_key,
                artifact=artifact,
                approved_by=actor,
                approved_at=event["at"],
                approval_event_id=event["id"],
                established_event_id=event["id"],
            )
            return event

    def reject(
        self,
        project_id: str,
        model_key: str,
        review_id: str,
        *,
        actor: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Record rejection of a reviewed candidate; baseline is unchanged."""
        actor = _require_actor(actor)
        with self._locked(project_id, exclusive=True):
            events = self._read_history(project_id)
            review = self._pending_review(project_id, events, model_key, review_id)
            return self._append_event(
                project_id,
                events,
                {
                    "event": "rejection",
                    "model_key": model_key,
                    "actor": actor,
                    "review_id": review_id,
                    "source_name": review.get("source_name"),
                    "artifact": review["candidate_artifact"],
                    "note": note,
                },
            )

    # ----- history -----------------------------------------------------------

    def history(self, project_id: str, model_key: str | None = None) -> list[dict[str, Any]]:
        """Return the append-only event log, optionally filtered by model_key."""
        _validate_id(project_id, "project_id")
        if model_key is not None:
            _validate_id(model_key, "model_key")
        with self._locked(project_id, exclusive=False):
            events = self._read_history(project_id)
        if model_key is None:
            return events
        return [event for event in events if event.get("model_key") == model_key]

    # ----- review reports & conflicts ---------------------------------------

    def review_report(self, project_id: str, review_id: str) -> dict[str, Any] | None:
        """The full persisted delta report of a review, or None if absent.

        Reviews recorded by release 0.1.0 have no stored report; they return
        None rather than recomputing. An id that names no review event raises
        ``UnknownReviewError``.
        """
        _validate_id(project_id, "project_id")
        _validate_id(review_id, "review_id")
        with self._locked(project_id, exclusive=False):
            event = next(
                (event for event in self._read_history(project_id) if event.get("id") == review_id),
                None,
            )
            if event is None:
                raise UnknownReviewError(f"No review {review_id} in {project_id}.")
            if event.get("event") != "review":
                raise UnknownReviewError(f"Event {review_id} is a {event.get('event')}, not a review.")
            return self._read_report(project_id, review_id)

    def pending_reviews(self, project_id: str, model_key: str) -> list[dict[str, Any]]:
        """Undecided reviews recorded since the last baseline change: the
        reviews a decision may target and a new review can conflict with."""
        return _pending_in(self.history(project_id, model_key))

    def _detect_conflicts(
        self, project_id: str, lineage: list[dict[str, Any]], report: dict[str, Any]
    ) -> list[dict[str, Any]]:
        mine = _changed_guids(report)
        if not mine:
            return []
        conflicts: list[dict[str, Any]] = []
        for pending in _pending_in(lineage):
            stored = self._read_report(project_id, pending["id"])
            if stored is None:
                continue
            overlap = sorted(mine & _changed_guids(stored))
            if overlap:
                conflicts.append(
                    {
                        "review_id": pending["id"],
                        "actor": pending.get("actor"),
                        "overlap_count": len(overlap),
                        "overlap_guids": overlap[:200],
                    }
                )
        return conflicts

    def _report_path(self, project_id: str, review_id: str) -> Path:
        return self._project_dir(project_id) / "reports" / f"{review_id}.json.gz"

    def _read_report(self, project_id: str, review_id: str) -> dict[str, Any] | None:
        path = self._report_path(project_id, review_id)
        if not path.is_file():
            return None
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_review_report(self, project_id: str, review_id: str, report: dict[str, Any]) -> None:
        path = self._report_path(project_id, review_id)
        payload = json.dumps(report, sort_keys=True).encode("utf-8")
        with atomic_file(path) as temporary:
            temporary.write_bytes(gzip.compress(payload, mtime=0))
        self._fault("report_durable")

    # ----- artifacts ---------------------------------------------------------

    def has_artifact(self, project_id: str, sha256: str) -> bool:
        """Whether the exact bytes for ``sha256`` are reconstructible."""
        with self._locked(project_id, exclusive=False):
            try:
                return self._pack(project_id).has(sha256)
            except PackError as exc:
                raise StoreError(str(exc)) from exc

    def export_artifact(self, project_id: str, sha256: str, dest: str | Path) -> Path:
        """Write an artifact's exact original IFC bytes to ``dest``."""
        return self._materialize(project_id, sha256, Path(dest))

    def artifact_stats(self, project_id: str) -> dict[str, Any]:
        """Aggregate logical vs stored bytes across a project's artifacts."""
        _validate_id(project_id, "project_id")
        with self._locked(project_id, exclusive=False):
            try:
                return self._pack(project_id).stats()
            except PackError as exc:
                raise StoreError(str(exc)) from exc

    def verify(
        self,
        project_id: str,
        *,
        rebuild_pointers: bool = False,
        before_rebuild: Callable[[list[str]], None] | None = None,
    ) -> dict[str, list[str]]:
        """Return ``problems`` found and non-failing ``notes``; pointer repair
        never changes history or evidence."""
        problems: list[str] = []
        notes: list[str] = []
        if not (self._project_dir(project_id) / "project.json").is_file():
            return {"problems": [f"project {project_id} does not exist"], "notes": notes}
        with self._locked(project_id, exclusive=rebuild_pointers):
            try:
                events = self.history(project_id)
            except (OSError, ValueError) as exc:
                return {"problems": [f"unreadable history: {exc}"], "notes": notes}
            if not all(isinstance(event, dict) for event in events):
                return {"problems": ["history contains a non-object event"], "notes": notes}
            expected_ids = [f"evt-{index:06d}" for index in range(1, len(events) + 1)]
            valid_ids = [event.get("id") for event in events] == expected_ids
            if not valid_ids:
                problems.append("history ids are not consecutive and unique")
            replay: dict[str, dict[str, Any]] = {}
            checked: set[str] = set()
            valid_replay = valid_ids
            for event in events:
                event_id = event.get("id")
                for key in ("artifact", "baseline_artifact", "candidate_artifact"):
                    if key not in event:
                        continue
                    try:
                        sha = event[key]["sha256"]
                        if sha not in checked:
                            checked.add(sha)
                            with self._pack(project_id).open(sha):
                                pass
                    except (PackError, OSError, ValueError, KeyError, TypeError) as exc:
                        problems.append(f"{event_id}: {key}: {exc}")
                if event.get("event") == "review":
                    try:
                        report = self._read_report(project_id, event_id)
                        if report is None and "conflicts" not in event:
                            notes.append(
                                f"{event_id}: no stored report (written before reports were kept)"
                            )
                        elif report is None:
                            problems.append(f"{event_id}: missing review report")
                        elif not isinstance(report, dict):
                            problems.append(f"{event_id}: review report is not an object")
                    except (OSError, ValueError, EOFError, StoreError) as exc:
                        problems.append(f"{event_id}: unreadable review report: {exc}")
                if event.get("event") in ("baseline_set", "approval"):
                    try:
                        key = _validate_id(event["model_key"], "model_key")
                        replay[key] = _pointer_from_event(event)
                    except (StoreError, KeyError, TypeError) as exc:
                        valid_replay = False
                        problems.append(f"{event_id}: cannot replay baseline: {exc}")
            review_ids = {event.get("id") for event in events if event.get("event") == "review"}
            reports = self._project_dir(project_id) / "reports"
            interrupted = _interrupted_reports(reports, len(events))
            for path in sorted(reports.glob("*.json.gz")):
                if path in interrupted:
                    notes.append(
                        f"reports/{path.name}: report of an interrupted review; "
                        "removed at the next write"
                    )
                elif path.name.removesuffix(".json.gz") not in review_ids:
                    problems.append(f"reports/{path.name}: no review event references this report")
            directory = self._project_dir(project_id) / "baselines"
            keys = set(replay) | {path.stem for path in directory.glob("*.json")}
            changes: list[str] = []
            repairs: list[tuple[Path, dict[str, Any] | None]] = []
            for key in sorted(keys):
                path = directory / f"{key}.json"
                try:
                    actual = _read_json(path) if path.is_file() else None
                except (OSError, ValueError) as exc:
                    actual = f"unreadable: {exc}"
                expected = replay.get(key)
                if actual != expected:
                    problems.append(f"baseline pointer {key} does not match history")
                    changes.append(f"{key}: {json.dumps(actual, sort_keys=True)} -> {json.dumps(expected, sort_keys=True)}")
                    repairs.append((path, expected))
            if rebuild_pointers and repairs:
                if not valid_replay:
                    problems.append("cannot rebuild pointers from invalid history")
                else:
                    if before_rebuild is not None:
                        before_rebuild(changes)
                    for path, pointer in repairs:
                        if pointer is None:
                            path.unlink()
                            sync_directory(path.parent)
                        else:
                            _write_json(path, pointer)
        return {"problems": problems, "notes": notes}

    # ----- internals ---------------------------------------------------------

    def _project_dir(self, project_id: str) -> Path:
        _validate_id(project_id, "project_id")
        return self.root / "projects" / project_id

    @contextmanager
    def _locked(self, project_id: str, *, exclusive: bool, create: bool = False) -> Iterator[None]:
        project = self._project_dir(project_id)
        held = _held_locks.__dict__.get("modes", {})
        if str(project.resolve()) in held:
            with _project_lock(project, exclusive=exclusive):
                yield
            return
        if exclusive and not create and not (project / "project.json").is_file():
            raise StoreError(f"Unknown project {project_id}.")
        while True:
            with _project_lock(project, exclusive=exclusive):
                if exclusive:
                    self._recover_pointer(project_id)
                    yield
                    return
                if not (project / "pending-pointer.json").exists():
                    yield
                    return
            with _project_lock(project, exclusive=True):
                self._recover_pointer(project_id)

    def _recover_pointer(self, project_id: str) -> None:
        project = self._project_dir(project_id)
        journal = project / "pending-pointer.json"
        if not journal.is_file():
            return
        pending = _read_json(journal)
        if pending in self._read_history(project_id):
            _write_json(self._baseline_path(project_id, pending["model_key"]), _pointer_from_event(pending))
        journal.unlink()
        sync_directory(project)

    def _materialize(self, project_id: str, sha256: str, dest: Path) -> Path:
        with self._locked(project_id, exclusive=False):
            try:
                return self._pack(project_id).materialize(sha256, dest)
            except PackError as exc:
                raise StoreError(str(exc)) from exc

    def _history_path(self, project_id: str) -> Path:
        return self._project_dir(project_id) / "history.jsonl"

    def _baseline_path(self, project_id: str, model_key: str) -> Path:
        return self._project_dir(project_id) / "baselines" / f"{model_key}.json"

    def _pack(self, project_id: str) -> ArtifactPack:
        return ArtifactPack(self._project_dir(project_id) / "artifacts")

    def _ingest_artifact(
        self, project_id: str, src: Path, schema: str, *, base_sha256: str | None = None
    ) -> dict[str, Any]:
        try:
            meta = self._pack(project_id).ingest(src, base_sha256=base_sha256)
        except PackError as exc:
            raise StoreError(str(exc)) from exc
        self._fault("artifact_durable")
        return {
            "sha256": meta["sha256"],
            "schema": schema,
            "size_bytes": meta["size_bytes"],
            "encoding": meta["encoding"],
            "stored_bytes": meta["stored_bytes"],
        }

    def _read_history(self, project_id: str) -> list[dict[str, Any]]:
        """The whole event log; callers hold the project's lock."""
        path = self._history_path(project_id)
        if not path.is_file():
            return []
        with open(path, encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def _append_event(
        self,
        project_id: str,
        history: list[dict[str, Any]],
        payload: dict[str, Any],
        *,
        report: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one event to ``history``, the log as read under the
        exclusive lock the caller still holds, after the review ``report``
        that the event refers to, if any."""
        event = {"id": _next_event_id(history), "at": self._clock()}
        event.update(payload)
        event.update(_producer())
        reports = self._project_dir(project_id) / "reports"
        interrupted = _interrupted_reports(reports, len(history))
        for leftover in interrupted:
            leftover.unlink()
        if interrupted:
            sync_directory(reports)
        if report is not None:
            self._write_review_report(project_id, event["id"], report)
        path = self._history_path(project_id)
        if event["event"] in ("baseline_set", "approval"):
            _write_json(path.parent / "pending-pointer.json", event)
            self._fault("pointer_prepared")
        with atomic_file(path) as temporary:
            with temporary.open("w", encoding="utf-8") as handle:
                for prior in history:
                    handle.write(json.dumps(prior, sort_keys=True) + "\n")
                handle.write(json.dumps(event, sort_keys=True) + "\n")
        self._fault("history_durable")
        return event

    def _pending_review(
        self, project_id: str, history: list[dict[str, Any]], model_key: str, review_id: str
    ) -> dict[str, Any]:
        _validate_id(model_key, "model_key")
        events = [event for event in history if event.get("model_key") == model_key]
        for index, event in enumerate(events):
            if event.get("id") != review_id:
                continue
            if event.get("event") != "review":
                raise UnknownReviewError(
                    f"Event {review_id} is a {event.get('event')}, not a review."
                )
            blocker = _review_blocker(events, index)
            if blocker is not None:
                raise ReviewStateError(review_id, *blocker)
            return event
        raise UnknownReviewError(f"No review {review_id} for {project_id}/{model_key}.")

    def _write_baseline_pointer(
        self,
        project_id: str,
        model_key: str,
        *,
        artifact: dict[str, Any],
        approved_by: str,
        approved_at: str,
        approval_event_id: str,
        established_event_id: str,
    ) -> None:
        pointer = {
            "model_key": model_key,
            "artifact": artifact,
            "approved_by": approved_by,
            "approved_at": approved_at,
            "approval_event_id": approval_event_id,
            "established_event_id": established_event_id,
        }
        path = self._baseline_path(project_id, model_key)
        _write_json(path, pointer)
        self._fault("pointer_durable")
        journal = self._project_dir(project_id) / "pending-pointer.json"
        if journal.exists():
            journal.unlink()
            sync_directory(journal.parent)


def _pointer_from_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "model_key": event["model_key"],
        "artifact": event["artifact"],
        "approved_by": event["actor"],
        "approved_at": event["at"],
        "approval_event_id": event["id"],
        "established_event_id": event["id"],
    }


def _next_event_id(history: list[dict[str, Any]]) -> str:
    return f"evt-{len(history) + 1:06d}"


def _interrupted_reports(reports: Path, event_count: int) -> set[Path]:
    """Report files numbered past the last event: written by a review that
    stopped before appending its event."""
    found = set()
    for path in reports.glob("evt-*.json.gz"):
        match = _REPORT_NAME_RE.match(path.name)
        if match and int(match.group(1)) > event_count:
            found.add(path)
    return found


def _pending_in(lineage: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Undecided reviews in one model lineage's history since its last
    baseline change."""
    return [
        event
        for index, event in enumerate(lineage)
        if event["event"] == "review" and _review_blocker(lineage, index) is None
    ]


def _review_blocker(events: list[dict[str, Any]], index: int) -> tuple[str, str] | None:
    """Why the review at ``events[index]`` is not pending, or None if it is.

    ``events`` is one model lineage's history in order. A decision on the
    review wins over staleness, since its own approval also moves the baseline.
    """
    review_id = events[index]["id"]
    later = events[index + 1 :]
    for event in later:
        if event.get("review_id") == review_id:
            if event["event"] == "approval":
                return "approved", event["id"]
            if event["event"] == "rejection":
                return "rejected", event["id"]
    for event in later:
        if event["event"] in ("baseline_set", "approval"):
            return "stale", event["id"]
    return None


def _changed_guids(report: dict[str, Any]) -> set[str]:
    """GUIDs a review actually touches: adds, deletes, and non-transitive
    modifications. Pure ripple (transitive) entities are excluded so two
    candidates editing disjoint elements in one storey do not conflict."""
    guids: set[str] = set()
    for section in ("added", "deleted"):
        for item in report.get(section) or []:
            if item.get("guid"):
                guids.add(item["guid"])
    for item in report.get("modified") or []:
        if item.get("change_scope") == "transitive":
            continue
        entity = item.get("new") or item.get("old") or {}
        if entity.get("guid"):
            guids.add(entity["guid"])
    return guids


def _stats_summary(report: dict[str, Any]) -> dict[str, Any]:
    stats = report.get("stats") or {}
    keys = (
        "added",
        "deleted",
        "modified",
        "unchanged",
        "old_signatures",
        "new_signatures",
    )
    return {key: stats.get(key) for key in keys if key in stats}


def _read_json(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with atomic_file(path) as temporary:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
