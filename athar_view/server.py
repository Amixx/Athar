"""Localhost file server for the visual diff viewer hand-off.

Perfetto-style: the CLI serves only the two IFC files + the diff report on
127.0.0.1 with CORS headers for the viewer origin, so model data never
leaves the machine. In offline mode the same server additionally serves a
built viewer SPA from an explicit path, a packaged bundle, or local viewer/dist.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from athar.engine import diff_bundles
from athar_git.cache import load_or_build_bundle

from athar_view import __version__

MANIFEST_SCHEMA_VERSION = 1

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


class ViewerHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        old_path: Path,
        new_path: Path,
        report_bytes: bytes,
        dist_dir: Path | None,
        allowed_origin: str = "*",
    ) -> None:
        super().__init__(address, ViewerRequestHandler)
        self.old_path = old_path
        self.new_path = new_path
        self.report_bytes = report_bytes
        self.dist_dir = dist_dir
        self.allowed_origin = allowed_origin

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def manifest_bytes(self) -> bytes:
        manifest = {
            "athar_viewer_manifest": 1,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generator": f"athar_view/{__version__}",
            "old": {"name": self.old_path.name, "url": "/files/old.ifc"},
            "new": {"name": self.new_path.name, "url": "/files/new.ifc"},
            "report": {"url": "/files/report.json"},
        }
        return json.dumps(manifest).encode("utf-8")


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
