"""Filesystem-backed baseline / approval / review-history store.

The store is the durable system of record that makes Athar's value compound
per project: it retains the accepted IFC baseline (content-addressed), an
immutable append-only event log of every review/approval/rejection, and a
materialized pointer to the currently-accepted version per model lineage.

Integration layer: depends on the core engine (`diff_files`) and policy gate
(`evaluate_report`), never the reverse.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from athar.bottom.parser import open_ifc
from athar.check import evaluate_report
from athar.engine import diff_files

from .pack import ArtifactPack, PackError

_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_DEFAULT_ROOT_ENV = "ATHAR_STORE_ROOT"


class StoreError(Exception):
    """Base error for store operations."""


class UnknownBaselineError(StoreError):
    """Raised when an operation needs a baseline that does not exist yet."""


class UnknownReviewError(StoreError):
    """Raised when an approval/rejection references a missing review event."""


def default_root() -> Path:
    """Resolve the store root from env or the per-user default."""
    env = os.environ.get(_DEFAULT_ROOT_ENV)
    if env:
        return Path(env)
    return Path.home() / ".athar" / "store"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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

    def __init__(self, root: str | Path | None = None, *, clock: Callable[[], str] | None = None) -> None:
        self.root = Path(root) if root is not None else default_root()
        self._clock = clock or _utc_now

    # ----- project lifecycle -------------------------------------------------

    def init_project(self, project_id: str, *, name: str | None = None) -> dict[str, Any]:
        """Create (or return) a project namespace."""
        _validate_id(project_id, "project_id")
        project_dir = self._project_dir(project_id)
        meta_path = project_dir / "project.json"
        if meta_path.exists():
            return _read_json(meta_path)
        (project_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        (project_dir / "baselines").mkdir(parents=True, exist_ok=True)
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
    ) -> dict[str, Any]:
        """Bootstrap the first accepted version for a model lineage.

        No diff is run: this establishes the reference every later candidate is
        compared against. The file is ingested into the content-addressed store.
        """
        _validate_id(model_key, "model_key")
        self.init_project(project_id)
        artifact = self._ingest_artifact(project_id, ifc_path)
        event = self._append_event(
            project_id,
            {
                "event": "baseline_set",
                "model_key": model_key,
                "actor": actor,
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

    def current_baseline(self, project_id: str, model_key: str) -> dict[str, Any] | None:
        """Return the accepted-version pointer, or None if unset."""
        _validate_id(project_id, "project_id")
        _validate_id(model_key, "model_key")
        pointer = self._baseline_path(project_id, model_key)
        if not pointer.is_file():
            return None
        return _read_json(pointer)

    # ----- review / approve / reject ----------------------------------------

    def review(
        self,
        project_id: str,
        model_key: str,
        candidate_path: str | Path,
        *,
        actor: str,
        policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Diff a candidate against the current baseline and record the verdict.

        Does not promote the candidate. The candidate is ingested at review time
        so the verdict is reproducible and a later approval is just a pointer
        flip. Raises ``UnknownBaselineError`` if no baseline exists yet.
        """
        baseline = self.current_baseline(project_id, model_key)
        if baseline is None:
            raise UnknownBaselineError(
                f"No baseline for {project_id}/{model_key}; call set_baseline first."
            )
        baseline_artifact = baseline["artifact"]
        pack = self._pack(project_id)
        try:
            with pack.open(baseline_artifact["sha256"]) as baseline_path:
                report = diff_files(str(baseline_path), str(candidate_path))
        except PackError as exc:
            raise StoreError(str(exc)) from exc
        check = evaluate_report(report, policy or {})
        candidate_artifact = self._ingest_artifact(
            project_id, candidate_path, base_sha256=baseline_artifact["sha256"]
        )

        event = self._append_event(
            project_id,
            {
                "event": "review",
                "model_key": model_key,
                "actor": actor,
                "baseline_artifact": baseline_artifact,
                "candidate_artifact": candidate_artifact,
                "verdict": "pass" if check["ok"] else "fail",
                "has_policy": bool(policy),
                "check": check,
                "diff": {
                    "schemas": report.get("schemas"),
                    "stats": _stats_summary(report),
                },
            },
        )
        return event

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
        review = self._find_event(project_id, model_key, review_id, "review")
        artifact = review["candidate_artifact"]
        event = self._append_event(
            project_id,
            {
                "event": "approval",
                "model_key": model_key,
                "actor": actor,
                "review_id": review_id,
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
        review = self._find_event(project_id, model_key, review_id, "review")
        return self._append_event(
            project_id,
            {
                "event": "rejection",
                "model_key": model_key,
                "actor": actor,
                "review_id": review_id,
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
        path = self._history_path(project_id)
        if not path.is_file():
            return []
        events: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                if model_key is None or event.get("model_key") == model_key:
                    events.append(event)
        return events

    # ----- artifacts ---------------------------------------------------------

    def has_artifact(self, project_id: str, sha256: str) -> bool:
        """Whether the exact bytes for ``sha256`` are reconstructible."""
        return self._pack(project_id).has(sha256)

    def export_artifact(self, project_id: str, sha256: str, dest: str | Path) -> Path:
        """Write an artifact's exact original IFC bytes to ``dest``."""
        try:
            return self._pack(project_id).materialize(sha256, dest)
        except PackError as exc:
            raise StoreError(str(exc)) from exc

    def artifact_stats(self, project_id: str) -> dict[str, Any]:
        """Aggregate logical vs stored bytes across a project's artifacts."""
        _validate_id(project_id, "project_id")
        return self._pack(project_id).stats()

    # ----- internals ---------------------------------------------------------

    def _project_dir(self, project_id: str) -> Path:
        _validate_id(project_id, "project_id")
        return self.root / "projects" / project_id

    def _history_path(self, project_id: str) -> Path:
        return self._project_dir(project_id) / "history.jsonl"

    def _baseline_path(self, project_id: str, model_key: str) -> Path:
        return self._project_dir(project_id) / "baselines" / f"{model_key}.json"

    def _pack(self, project_id: str) -> ArtifactPack:
        return ArtifactPack(self._project_dir(project_id) / "artifacts")

    def _ingest_artifact(
        self, project_id: str, ifc_path: str | Path, *, base_sha256: str | None = None
    ) -> dict[str, Any]:
        src = Path(ifc_path)
        if not src.is_file():
            raise StoreError(f"IFC file not found: {ifc_path}")
        schema = open_ifc(str(src)).schema
        try:
            meta = self._pack(project_id).ingest(src, base_sha256=base_sha256)
        except PackError as exc:
            raise StoreError(str(exc)) from exc
        return {
            "sha256": meta["sha256"],
            "schema": schema,
            "size_bytes": meta["size_bytes"],
            "encoding": meta["encoding"],
            "stored_bytes": meta["stored_bytes"],
        }

    def _append_event(self, project_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.init_project(project_id)
        seq = len(self.history(project_id)) + 1
        event = {"id": f"evt-{seq:06d}", "at": self._clock()}
        event.update(payload)
        # Stable key order keeps the on-disk log diff-friendly.
        ordered = {"id": event["id"], "event": event["event"], "at": event["at"]}
        ordered.update({k: v for k, v in event.items() if k not in ordered})
        with open(self._history_path(project_id), "a", encoding="utf-8") as handle:
            handle.write(json.dumps(ordered, sort_keys=True) + "\n")
        return ordered

    def _find_event(
        self, project_id: str, model_key: str, event_id: str, expected: str
    ) -> dict[str, Any]:
        _validate_id(model_key, "model_key")
        for event in self.history(project_id, model_key):
            if event.get("id") == event_id:
                if event.get("event") != expected:
                    raise UnknownReviewError(
                        f"Event {event_id} is a {event.get('event')}, not a {expected}."
                    )
                return event
        raise UnknownReviewError(
            f"No {expected} {event_id} for {project_id}/{model_key}."
        )

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
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(path, pointer)


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
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
