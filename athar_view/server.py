"""Localhost file server for the visual diff viewer hand-off.

Perfetto-style: the CLI serves only the two IFC files + the diff report on
127.0.0.1 with CORS headers for the viewer origin, so model data never
leaves the machine. In offline mode the same server additionally serves a
built viewer SPA from an explicit path, a packaged bundle, or local viewer/dist.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from athar.engine import diff_bundles
from athar_git.cache import load_or_build_bundle

from athar_view import __version__

MANIFEST_SCHEMA_VERSION = 2

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


def build_report_bytes(
    old_path: str | os.PathLike[str],
    new_path: str | os.PathLike[str],
    *,
    cache_dir: str | os.PathLike[str] | None = None,
) -> bytes:
    """Diff two IFC files through the persistent bundle cache."""
    old_result = load_or_build_bundle(old_path, cache_dir=cache_dir)
    new_result = load_or_build_bundle(new_path, cache_dir=cache_dir)
    report = diff_bundles(old_result.bundle, new_result.bundle)
    return json.dumps(report).encode("utf-8")


def find_dist(explicit: str | os.PathLike[str] | None = None) -> Path | None:
    """Locate a built viewer SPA: explicit arg → $ATHAR_VIEWER_DIST →
    wheel-packaged athar_view/static → local repo viewer/dist."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("ATHAR_VIEWER_DIST")
    if env:
        candidates.append(Path(env))
    package_dir = Path(__file__).resolve().parent
    candidates.append(package_dir / "static")
    candidates.append(package_dir.parent / "viewer" / "dist")
    for candidate in candidates:
        if (candidate / "index.html").is_file():
            return candidate
    return None


@dataclass
class ChainStep:
    """One consecutive pair in a revision chain, by index into the file list."""

    old_idx: int
    new_idx: int
    label: str


class ViewerHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        old_path: Path | None = None,
        new_path: Path | None = None,
        report_bytes: bytes | None = None,
        chain_files: list[Path] | None = None,
        chain_steps: list[ChainStep] | None = None,
        active_step: int = 0,
        cache_dir: str | os.PathLike[str] | None = None,
        dist_dir: Path | None,
        allowed_origin: str = "*",
    ) -> None:
        super().__init__(address, ViewerRequestHandler)
        self.old_path = old_path
        self.new_path = new_path
        self.report_bytes = report_bytes
        self.chain_files = chain_files
        self.chain_steps = chain_steps
        self.active_step = active_step
        self.cache_dir = cache_dir
        self.dist_dir = dist_dir
        self.allowed_origin = allowed_origin
        self._report_cache: dict[int, bytes] = {}
        self._report_lock = threading.Lock()

    @property
    def is_chain(self) -> bool:
        return self.chain_steps is not None

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def manifest_bytes(self) -> bytes:
        if self.is_chain:
            return json.dumps(self._chain_manifest()).encode("utf-8")
        manifest = {
            "athar_viewer_manifest": 1,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generator": f"athar_view/{__version__}",
            "old": {"name": self.old_path.name, "url": "/files/old.ifc"},
            "new": {"name": self.new_path.name, "url": "/files/new.ifc"},
            "report": {"url": "/files/report.json"},
        }
        return json.dumps(manifest).encode("utf-8")

    def _chain_manifest(self) -> dict:
        assert self.chain_files is not None and self.chain_steps is not None
        steps = []
        for i, step in enumerate(self.chain_steps):
            old_file = self.chain_files[step.old_idx]
            new_file = self.chain_files[step.new_idx]
            steps.append(
                {
                    "label": step.label,
                    "old": {"name": old_file.name, "url": f"/files/model/{step.old_idx}.ifc"},
                    "new": {"name": new_file.name, "url": f"/files/model/{step.new_idx}.ifc"},
                    "report": {"url": f"/files/report/{i}.json"},
                }
            )
        return {
            "athar_viewer_manifest": 1,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generator": f"athar_view/{__version__}",
            "steps": steps,
            "active_step": self.active_step,
        }

    def chain_report_bytes(self, step_idx: int) -> bytes | None:
        """Diff one chain step on demand, caching the result. Files shared by
        adjacent steps still hit the persistent bundle cache underneath."""
        if self.chain_steps is None or self.chain_files is None:
            return None
        if not 0 <= step_idx < len(self.chain_steps):
            return None
        with self._report_lock:
            cached = self._report_cache.get(step_idx)
            if cached is not None:
                return cached
            step = self.chain_steps[step_idx]
            data = build_report_bytes(
                self.chain_files[step.old_idx],
                self.chain_files[step.new_idx],
                cache_dir=self.cache_dir,
            )
            self._report_cache[step_idx] = data
            return data


class ViewerRequestHandler(BaseHTTPRequestHandler):
    server: ViewerHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        pass

    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", self.server.allowed_origin)
        self.send_header("Vary", "Origin")
        self.send_header("Cross-Origin-Resource-Policy", "cross-origin")
        # Chrome Private Network Access: a public (hosted) origin fetching
        # 127.0.0.1 preflights with Access-Control-Request-Private-Network.
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Cache-Control", "no-store")

    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib naming
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        self._respond(head_only=True)

    def do_GET(self) -> None:  # noqa: N802
        self._respond(head_only=False)

    def _respond(self, *, head_only: bool) -> None:
        path, _, query = self.path.partition("?")
        if path == "/" and self.server.dist_dir is not None and "src=" not in query:
            # This server holds exactly one loaded diff; the bare root would
            # show the SPA's empty drag-drop landing instead of it.
            self.send_response(HTTPStatus.TEMPORARY_REDIRECT)
            self._cors_headers()
            self.send_header("Location", f"/?src={urllib.parse.quote(self.server.origin, safe='')}")
            self.end_headers()
            return
        resolved = self._resolve(path)
        if resolved is None:
            self.send_response(HTTPStatus.NOT_FOUND)
            self._cors_headers()
            self.end_headers()
            return
        body, content_type = resolved
        self.send_response(HTTPStatus.OK)
        self._cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _resolve(self, path: str) -> tuple[bytes, str] | None:
        server = self.server
        if path == "/manifest.json":
            return server.manifest_bytes(), _CONTENT_TYPES[".json"]
        if server.is_chain:
            resolved = self._resolve_chain(server, path)
            if resolved is not None:
                return resolved
        else:
            if path == "/files/old.ifc":
                return server.old_path.read_bytes(), _CONTENT_TYPES[".ifc"]
            if path == "/files/new.ifc":
                return server.new_path.read_bytes(), _CONTENT_TYPES[".ifc"]
            if path == "/files/report.json":
                return server.report_bytes, _CONTENT_TYPES[".json"]
        if server.dist_dir is not None:
            return self._resolve_static(server.dist_dir, path)
        return None

    @staticmethod
    def _resolve_chain(server: ViewerHTTPServer, path: str) -> tuple[bytes, str] | None:
        assert server.chain_files is not None
        if path.startswith("/files/model/") and path.endswith(".ifc"):
            try:
                idx = int(path[len("/files/model/") : -len(".ifc")])
            except ValueError:
                return None
            if 0 <= idx < len(server.chain_files):
                return server.chain_files[idx].read_bytes(), _CONTENT_TYPES[".ifc"]
            return None
        if path.startswith("/files/report/") and path.endswith(".json"):
            try:
                idx = int(path[len("/files/report/") : -len(".json")])
            except ValueError:
                return None
            data = server.chain_report_bytes(idx)
            if data is not None:
                return data, _CONTENT_TYPES[".json"]
            return None
        return None

    @staticmethod
    def _resolve_static(dist_dir: Path, path: str) -> tuple[bytes, str] | None:
        relative = path.lstrip("/") or "index.html"
        candidate = (dist_dir / relative).resolve()
        try:
            candidate.relative_to(dist_dir.resolve())
        except ValueError:
            return None
        if not candidate.is_file():
            return None
        content_type = _CONTENT_TYPES.get(candidate.suffix, "application/octet-stream")
        return candidate.read_bytes(), content_type


def make_server(
    old_path: str | os.PathLike[str],
    new_path: str | os.PathLike[str],
    report_bytes: bytes,
    *,
    dist_dir: str | os.PathLike[str] | None = None,
    port: int = 0,
    allowed_origin: str = "*",
) -> ViewerHTTPServer:
    return ViewerHTTPServer(
        ("127.0.0.1", port),
        old_path=Path(old_path),
        new_path=Path(new_path),
        report_bytes=report_bytes,
        dist_dir=Path(dist_dir) if dist_dir is not None else None,
        allowed_origin=allowed_origin,
    )


def build_chain_steps(files: list[Path]) -> list[ChainStep]:
    """Consecutive pairs over an ordered file list: (0,1), (1,2), ..."""
    return [
        ChainStep(old_idx=i, new_idx=i + 1, label=f"{files[i].stem} → {files[i + 1].stem}")
        for i in range(len(files) - 1)
    ]


def make_chain_server(
    files: list[str | os.PathLike[str]],
    *,
    cache_dir: str | os.PathLike[str] | None = None,
    active_step: int = 0,
    dist_dir: str | os.PathLike[str] | None = None,
    port: int = 0,
    allowed_origin: str = "*",
) -> ViewerHTTPServer:
    chain_files = [Path(f) for f in files]
    return ViewerHTTPServer(
        ("127.0.0.1", port),
        chain_files=chain_files,
        chain_steps=build_chain_steps(chain_files),
        active_step=active_step,
        cache_dir=cache_dir,
        dist_dir=Path(dist_dir) if dist_dir is not None else None,
        allowed_origin=allowed_origin,
    )
