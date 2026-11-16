"""Localhost HTTP server for the Athar Review UI.

JSON API over a ``BaselineStore`` plus static serving of the built SPA
(``review.html`` at the root). Each review also gets a viewer manifest at
``/viewer/{project}/{review}/manifest.json`` built from the persisted delta
report and the content-addressed artifacts, so the existing 3D diff viewer
(``index.html``) opens any recorded review without recomputing anything.
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from athar.check import resolve_policy
from athar_store import BaselineStore, StoreError

from athar_review import __version__

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".wasm": "application/wasm",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ifc": "application/x-step",
}

_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_UPLOAD_BYTES = 2_000_000_000


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class ReviewHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        store: BaselineStore,
        dist_dir: Path | None,
    ) -> None:
        super().__init__(address, ReviewRequestHandler)
        self.store = store
        self.dist_dir = dist_dir

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


def make_server(
    store: BaselineStore,
    *,
    dist_dir: Path | None = None,
    port: int = 0,
) -> ReviewHTTPServer:
    return ReviewHTTPServer(("127.0.0.1", port), store=store, dist_dir=dist_dir)


class ReviewRequestHandler(BaseHTTPRequestHandler):
    server: ReviewHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        pass

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def _handle(self, method: str) -> None:
        path, _, query = self.path.partition("?")
        params = {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}
        try:
            if path.startswith("/api/") or path.startswith("/viewer/"):
                self._route_api(method, path, params)
            elif method == "GET":
                self._serve_static(path)
            else:
                raise ApiError(HTTPStatus.NOT_FOUND, f"no such endpoint: {path}")
        except ApiError as exc:
            self._send_json({"error": str(exc)}, status=exc.status)
        except StoreError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except BrokenPipeError:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            self._send_json({"error": f"internal error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    # ----- routing -----------------------------------------------------------

    def _route_api(self, method: str, path: str, params: dict[str, str]) -> None:
        store = self.server.store
        parts = [urllib.parse.unquote(p) for p in path.strip("/").split("/")]

        if method == "GET" and parts == ["api", "overview"]:
            self._send_json(_overview(store))
            return

        if method == "GET" and len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "events":
            project_id = _checked_id(parts[2])
            model_key = params.get("model_key")
            self._send_json({"events": store.history(project_id, model_key)})
            return

        if method == "GET" and len(parts) == 5 and parts[:2] == ["api", "projects"] and parts[3] == "reviews":
            report = store.review_report(_checked_id(parts[2]), _checked_id(parts[4]))
            if report is None:
                raise ApiError(HTTPStatus.NOT_FOUND, f"no stored report for review {parts[4]}")
            self._send_json(report)
            return

        if method == "GET" and len(parts) == 5 and parts[:2] == ["api", "projects"] and parts[3] == "artifacts":
            self._send_artifact(_checked_id(parts[2]), parts[4])
            return

        if len(parts) == 6 and parts[:2] == ["api", "projects"] and parts[3] == "models":
            project_id = _checked_id(parts[2])
            model_key = _checked_id(parts[4])
            action = parts[5]
            if method == "POST" and action == "review":
                self._post_candidate(project_id, model_key, params, kind="review")
                return
            if method == "POST" and action == "baseline":
                self._post_candidate(project_id, model_key, params, kind="baseline")
                return
            if method == "POST" and action in ("approve", "reject"):
                self._post_resolution(project_id, model_key, action)
                return
            if method == "GET" and action == "pending":
                self._send_json({"pending": self.server.store.pending_reviews(project_id, model_key)})
                return

        if method == "GET" and len(parts) == 4 and parts[0] == "viewer" and parts[3] == "manifest.json":
            self._send_json(_viewer_manifest(store, _checked_id(parts[1]), _checked_id(parts[2])))
            return

        raise ApiError(HTTPStatus.NOT_FOUND, f"no such endpoint: {method} {path}")

    # ----- api handlers ------------------------------------------------------

    def _send_artifact(self, project_id: str, name: str) -> None:
        sha = name.removesuffix(".ifc")
        if not _SHA_RE.match(sha):
            raise ApiError(HTTPStatus.BAD_REQUEST, f"not an artifact sha256: {name}")
        if not self.server.store.has_artifact(project_id, sha):
            raise ApiError(HTTPStatus.NOT_FOUND, f"unknown artifact {sha}")
        with tempfile.TemporaryDirectory(prefix="athar-review-") as tmp:
            path = self.server.store.export_artifact(project_id, sha, Path(tmp) / f"{sha}.ifc")
            size = path.stat().st_size
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", _CONTENT_TYPES[".ifc"])
            self.send_header("Content-Length", str(size))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with open(path, "rb") as handle:
                shutil.copyfileobj(handle, self.wfile)

    def _post_candidate(
        self, project_id: str, model_key: str, params: dict[str, str], *, kind: str
    ) -> None:
        actor = params.get("actor") or "reviewer"
        filename = Path(params.get("filename") or "candidate.ifc").name
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(HTTPStatus.BAD_REQUEST, "empty upload: send the IFC bytes as the request body")
        if length > _MAX_UPLOAD_BYTES:
            raise ApiError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "upload too large")
        with tempfile.TemporaryDirectory(prefix="athar-review-") as tmp:
            candidate = Path(tmp) / filename
            with open(candidate, "wb") as out:
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise ApiError(HTTPStatus.BAD_REQUEST, "upload truncated")
                    out.write(chunk)
                    remaining -= len(chunk)
            if kind == "baseline":
                event = self.server.store.set_baseline(project_id, model_key, candidate, actor=actor)
            else:
                policy = resolve_policy(params["policy"]) if params.get("policy") else None
                event = self.server.store.review(project_id, model_key, candidate, actor=actor, policy=policy)
        self._send_json(event)

    def _post_resolution(self, project_id: str, model_key: str, action: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise ApiError(HTTPStatus.BAD_REQUEST, f"invalid JSON body: {exc}") from exc
        review_id = body.get("review_id")
        if not review_id:
            raise ApiError(HTTPStatus.BAD_REQUEST, "review_id is required")
        actor = body.get("actor") or "reviewer"
        note = body.get("note")
        store = self.server.store
        if action == "approve":
            event = store.approve(project_id, model_key, review_id, actor=actor, note=note)
        else:
            event = store.reject(project_id, model_key, review_id, actor=actor, note=note)
        self._send_json(event)

    # ----- static ------------------------------------------------------------

    def _serve_static(self, path: str) -> None:
        dist = self.server.dist_dir
        if dist is None:
            raise ApiError(
                HTTPStatus.NOT_FOUND,
                "no built UI available: run `make viewer-build` or set ATHAR_VIEWER_DIST",
            )
        relative = path.lstrip("/") or "review.html"
        candidate = (dist / relative).resolve()
        try:
            candidate.relative_to(dist.resolve())
        except ValueError as exc:
            raise ApiError(HTTPStatus.NOT_FOUND, "not found") from exc
        if not candidate.is_file():
            if "." not in relative:
                candidate = dist / "review.html"
            if not candidate.is_file():
                raise ApiError(HTTPStatus.NOT_FOUND, f"not found: {path}")
        body = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", _CONTENT_TYPES.get(candidate.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload: Any, *, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", _CONTENT_TYPES[".json"])
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def _checked_id(value: str) -> str:
    if not _ID_RE.match(value):
        raise ApiError(HTTPStatus.BAD_REQUEST, f"invalid identifier: {value!r}")
    return value


def _overview(store: BaselineStore) -> dict[str, Any]:
    projects = []
    for meta in store.list_projects():
        project_id = meta["project_id"]
        events = store.history(project_id)
        last_by_model: dict[str, str] = {}
        for event in events:
            key = event.get("model_key")
            if key:
                last_by_model[key] = event.get("at", "")
        models = []
        for pointer in store.list_baselines(project_id):
            key = pointer["model_key"]
            models.append(
                {
                    "model_key": key,
                    "baseline": pointer,
                    "pending_count": len(store.pending_reviews(project_id, key)),
                    "last_event_at": last_by_model.get(key),
                }
            )
        projects.append({**meta, "models": models, "event_count": len(events)})
    return {"generator": f"athar_review/{__version__}", "projects": projects}


def _viewer_manifest(store: BaselineStore, project_id: str, review_id: str) -> dict[str, Any]:
    review = next(
        (
            event
            for event in store.history(project_id)
            if event.get("id") == review_id and event.get("event") == "review"
        ),
        None,
    )
    if review is None:
        raise ApiError(HTTPStatus.NOT_FOUND, f"no review {review_id} in {project_id}")
    if store.review_report(project_id, review_id) is None:
        raise ApiError(HTTPStatus.NOT_FOUND, f"no stored report for review {review_id}")
    old_sha = review["baseline_artifact"]["sha256"]
    new_sha = review["candidate_artifact"]["sha256"]
    return {
        "athar_viewer_manifest": 1,
        "schema_version": 2,
        "generator": f"athar_review/{__version__}",
        "old": {
            "name": f"baseline {old_sha[:12]}",
            "url": f"/api/projects/{project_id}/artifacts/{old_sha}.ifc",
        },
        "new": {
            "name": f"candidate {new_sha[:12]}",
            "url": f"/api/projects/{project_id}/artifacts/{new_sha}.ifc",
        },
        "report": {"url": f"/api/projects/{project_id}/reviews/{review_id}"},
    }
