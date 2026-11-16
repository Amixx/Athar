"""Localhost HTTP server for the Athar Review UI.

JSON API over a ``BaselineStore`` plus static serving of the built SPA
(``review.html`` at the root). Each review also gets a viewer manifest at
``/viewer/{project}/{review}/manifest.json`` built from the persisted delta
report and the content-addressed artifacts, so the existing 3D diff viewer
(``index.html``) opens any recorded review without recomputing anything.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import tempfile
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from athar.check import resolve_policy
from athar_store import (
    BaselineStore,
    EngineError,
    ReviewStateError,
    StaleBaselineError,
    StoreConflictError,
    StoreError,
    UnknownBaselineError,
    UnknownReviewError,
)

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
DEFAULT_MAX_UPLOAD_MB = 2000
_MAX_JSON_BYTES = 1_000_000
# Neither is a CORS-safelisted type, so a cross-site page cannot send one
# without a preflight, which this server never grants.
_UPLOAD_TYPES = ("application/x-step", "application/octet-stream")


_log = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.detail = detail


def _api_error(exc: Exception) -> ApiError:
    if isinstance(exc, ApiError):
        return exc
    message = str(exc)
    if isinstance(exc, ReviewStateError):
        return ApiError(
            HTTPStatus.CONFLICT,
            f"review_{exc.state}",
            message,
            review_id=exc.review_id,
            state=exc.state,
            event_id=exc.event_id,
        )
    if isinstance(exc, StaleBaselineError):
        return ApiError(HTTPStatus.CONFLICT, "baseline_changed", message)
    if isinstance(exc, StoreConflictError):
        return ApiError(HTTPStatus.CONFLICT, "baseline_exists", message)
    if isinstance(exc, UnknownBaselineError):
        return ApiError(HTTPStatus.CONFLICT, "no_baseline", message)
    if isinstance(exc, UnknownReviewError):
        return ApiError(HTTPStatus.NOT_FOUND, "review_not_found", message)
    if isinstance(exc, EngineError):
        return ApiError(HTTPStatus.UNPROCESSABLE_ENTITY, "engine_refused", message)
    if isinstance(exc, StoreError):
        return ApiError(HTTPStatus.INTERNAL_SERVER_ERROR, "store_error", message)
    return ApiError(
        HTTPStatus.INTERNAL_SERVER_ERROR,
        "internal_error",
        "The review server hit an unexpected error; details are in its log.",
    )


class ReviewHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        store: BaselineStore,
        dist_dir: Path | None,
        max_upload_bytes: int,
    ) -> None:
        super().__init__(address, ReviewRequestHandler)
        self.store = store
        self.dist_dir = dist_dir
        self.max_upload_bytes = max_upload_bytes

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    @property
    def allowed_hosts(self) -> frozenset[str]:
        port = self.server_address[1]
        return frozenset({f"127.0.0.1:{port}", f"localhost:{port}"})

    @property
    def allowed_origins(self) -> frozenset[str]:
        return frozenset(f"http://{host}" for host in self.allowed_hosts)


def make_server(
    store: BaselineStore,
    *,
    dist_dir: Path | None = None,
    port: int = 0,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_MB * 1_000_000,
) -> ReviewHTTPServer:
    return ReviewHTTPServer(
        ("127.0.0.1", port), store=store, dist_dir=dist_dir, max_upload_bytes=max_upload_bytes
    )


class ReviewRequestHandler(BaseHTTPRequestHandler):
    server: ReviewHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        pass

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def _handle(self, method: str) -> None:
        self._body_started = False
        path, _, query = self.path.partition("?")
        params = {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}
        try:
            self._check_request_source(method)
            if path.startswith("/api/") or path.startswith("/viewer/"):
                self._route_api(method, path, params)
            elif method == "GET":
                self._serve_static(path)
            else:
                raise ApiError(HTTPStatus.NOT_FOUND, "not_found", f"No such endpoint: {path}")
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            error = _api_error(exc)
            self._drain_refused_body()
            if error.status >= 500:
                _log.exception("error serving %s %s", method, path)
            self.close_connection = True
            self._send_json(
                {"error": {"code": error.code, "message": str(error), **error.detail}},
                status=error.status,
            )

    def _drain_refused_body(self) -> None:
        if self.command != "POST" or self._body_started:
            return
        try:
            remaining = self._content_length()
        except ApiError:
            return
        if remaining > self.server.max_upload_bytes + 1_000_000:
            return
        self.connection.settimeout(5)
        try:
            while remaining:
                chunk = self.rfile.read(min(1024 * 1024, remaining))
                if not chunk:
                    return
                remaining -= len(chunk)
        except OSError:
            return

    def _check_request_source(self, method: str) -> None:
        # A page on another site can make the browser send requests here; the
        # Host check defeats DNS rebinding and the Origin check cross-site POSTs.
        if self.headers.get("Host") not in self.server.allowed_hosts:
            raise ApiError(
                HTTPStatus.FORBIDDEN, "host_not_allowed", "Request Host is not this server's loopback address."
            )
        origin = self.headers.get("Origin")
        if method == "POST" and origin is not None and origin not in self.server.allowed_origins:
            raise ApiError(HTTPStatus.FORBIDDEN, "origin_not_allowed", f"Cross-origin request refused: {origin}")

    def _require_content_type(self, *allowed: str) -> None:
        content_type = self.headers.get_content_type()
        if self.headers.get("Content-Type") is None or content_type not in allowed:
            raise ApiError(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                f"Content-Type must be {' or '.join(allowed)}.",
            )

    def _content_length(self) -> int:
        raw = self.headers.get("Content-Length")
        if raw is None or self.headers.get("Transfer-Encoding"):
            raise ApiError(HTTPStatus.LENGTH_REQUIRED, "length_required", "Send the body with a Content-Length.")
        if re.fullmatch(r"[0-9]+", raw) is None:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_length", "Content-Length must contain ASCII digits only.")
        try:
            return int(raw)
        except ValueError as exc:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_length", "Content-Length is too long.") from exc

    # ----- routing -----------------------------------------------------------

    def _route_api(self, method: str, path: str, params: dict[str, str]) -> None:
        store = self.server.store
        parts = [urllib.parse.unquote(p) for p in path.strip("/").split("/")]

        if method == "GET" and parts == ["api", "overview"]:
            self._send_json({**_overview(store), "max_upload_bytes": self.server.max_upload_bytes})
            return

        if method == "GET" and len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "events":
            project_id = _known_project(store, parts[2])
            model_key = params.get("model_key")
            self._send_json({"events": store.history(project_id, model_key)})
            return

        if method == "GET" and len(parts) == 5 and parts[:2] == ["api", "projects"] and parts[3] == "reviews":
            report = store.review_report(_known_project(store, parts[2]), _checked_id(parts[4]))
            if report is None:
                raise ApiError(
                    HTTPStatus.NOT_FOUND,
                    "report_not_stored",
                    f"Review {parts[4]} was recorded before change reports were kept; only its summary exists.",
                )
            self._send_json(report)
            return

        if method == "GET" and len(parts) == 5 and parts[:2] == ["api", "projects"] and parts[3] == "artifacts":
            self._send_artifact(_known_project(store, parts[2]), parts[4])
            return

        if len(parts) == 6 and parts[:2] == ["api", "projects"] and parts[3] == "models":
            action = parts[5]
            model_key = _checked_id(parts[4])
            project_id = _checked_id(parts[2]) if action == "baseline" else _known_project(store, parts[2])
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
            self._send_json(_viewer_manifest(store, _known_project(store, parts[1]), _checked_id(parts[2])))
            return

        raise ApiError(HTTPStatus.NOT_FOUND, "not_found", f"No such endpoint: {method} {path}")

    # ----- api handlers ------------------------------------------------------

    def _send_artifact(self, project_id: str, name: str) -> None:
        sha = name.removesuffix(".ifc")
        if not _SHA_RE.match(sha):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_artifact", f"Not an artifact sha256: {name}")
        if not self.server.store.has_artifact(project_id, sha):
            raise ApiError(HTTPStatus.NOT_FOUND, "artifact_not_found", f"Unknown artifact {sha}")
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
        self._require_content_type(*_UPLOAD_TYPES)
        actor = _required_actor(params.get("actor"))
        source_name = params.get("filename", "")
        if not source_name.strip():
            raise ApiError(
                HTTPStatus.BAD_REQUEST, "filename_required", "A delivery file name is required: name the uploaded IFC file."
            )
        policy = _policy(params.get("policy")) if kind == "review" else None
        length = self._content_length()
        if length == 0:
            raise ApiError(HTTPStatus.BAD_REQUEST, "empty_upload", "Empty upload: send the IFC bytes as the request body.")
        limit = self.server.max_upload_bytes
        if length > limit:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "upload_too_large",
                f"The file is larger than this server's upload limit of {limit / 1_000_000:,.0f} MB "
                "(set with athar review --max-upload-mb).",
                limit_bytes=limit,
            )
        with tempfile.TemporaryDirectory(prefix="athar-review-") as tmp:
            candidate = Path(tmp) / "upload.ifc"
            with open(candidate, "wb") as out:
                self._body_started = True
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise ApiError(HTTPStatus.BAD_REQUEST, "upload_truncated", "The upload ended early.")
                    out.write(chunk)
                    remaining -= len(chunk)
            if kind == "baseline":
                event = self.server.store.set_baseline(
                    project_id,
                    model_key,
                    candidate,
                    actor=actor,
                    source_name=source_name,
                    replace=params.get("replace") == "true",
                )
            else:
                event = self.server.store.review(
                    project_id,
                    model_key,
                    candidate,
                    actor=actor,
                    policy=policy,
                    source_name=source_name,
                )
        self._send_json(event)

    def _post_resolution(self, project_id: str, model_key: str, action: str) -> None:
        self._require_content_type("application/json")
        length = self._content_length()
        if length > _MAX_JSON_BYTES:
            raise ApiError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "body_too_large", "The JSON body is too large.")
        try:
            self._body_started = True
            body = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_json", f"Invalid JSON body: {exc}") from exc
        if not isinstance(body, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_json", "The JSON body must be an object.")
        review_id = body.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            raise ApiError(HTTPStatus.BAD_REQUEST, "review_id_required", "review_id is required.")
        review_id = _checked_id(review_id)
        actor = _required_actor(body.get("actor"))
        note = body.get("note")
        if note is not None and not isinstance(note, str):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_note", "note must be a string.")
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
                "ui_not_built",
                "No built UI available: run `make viewer-build` or set ATHAR_VIEWER_DIST.",
            )
        relative = path.lstrip("/") or "review.html"
        candidate = (dist / relative).resolve()
        try:
            candidate.relative_to(dist.resolve())
        except ValueError as exc:
            raise ApiError(HTTPStatus.NOT_FOUND, "not_found", "Not found.") from exc
        if not candidate.is_file():
            if "." not in relative:
                candidate = dist / "review.html"
            if not candidate.is_file():
                raise ApiError(HTTPStatus.NOT_FOUND, "not_found", f"Not found: {path}")
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
    if value in {".", ".."} or _ID_RE.fullmatch(value) is None:
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_identifier", f"Invalid identifier: {value!r}")
    return value


def _known_project(store: BaselineStore, value: str) -> str:
    project_id = _checked_id(value)
    if not any(meta["project_id"] == project_id for meta in store.list_projects()):
        raise ApiError(HTTPStatus.NOT_FOUND, "project_not_found", f"No project {project_id} in this store.")
    return project_id


def _required_actor(actor: Any) -> str:
    if not isinstance(actor, str) or not actor.strip():
        raise ApiError(
            HTTPStatus.BAD_REQUEST, "actor_required", "A reviewer name is required: say who performs this action."
        )
    return actor


def _policy(name: str | None) -> dict[str, Any] | None:
    if not name:
        return None
    try:
        return resolve_policy(name)
    except ValueError as exc:
        raise ApiError(HTTPStatus.BAD_REQUEST, "unknown_policy", str(exc)) from exc


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
        raise ApiError(HTTPStatus.NOT_FOUND, "review_not_found", f"No review {review_id} in {project_id}.")
    if store.review_report(project_id, review_id) is None:
        raise ApiError(HTTPStatus.NOT_FOUND, "report_not_stored", f"No stored report for review {review_id}.")
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
