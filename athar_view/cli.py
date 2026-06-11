"""`athar view OLD NEW` — visual diff in the browser.

Two delivery modes, same SPA:

- Hosted hand-off: when a hosted viewer URL is configured (--viewer-url or
  ATHAR_VIEWER_URL) and reachable, the CLI serves only the IFC pair + report
  on 127.0.0.1 and opens <hosted>?src=http://127.0.0.1:PORT. Model data
  never leaves the machine.
- Offline/bundled (--offline, and the automatic fallback when no hosted URL
  is configured or it is unreachable): the same localhost server also
  serves the built SPA from viewer/dist (or $ATHAR_VIEWER_DIST, or the
  wheel-packaged athar_view/static).
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path

from athar_view.server import ViewerHTTPServer, build_report_bytes, find_dist, make_server

SIZE_WARN_BYTES = 50 * 1024 * 1024


@dataclass
class ViewSession:
    server: ViewerHTTPServer
    url: str
    mode: str  # "hosted" | "offline"
    messages: list[str] = field(default_factory=list)


def build_view_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="athar view",
        description="Open a visual old/new IFC diff in the browser.",
    )
    parser.add_argument("old", help="Path to old IFC file")
    parser.add_argument("new", help="Path to new IFC file")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Serve the bundled viewer from localhost instead of handing off to a hosted viewer",
    )
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser")
    parser.add_argument("--port", type=int, default=0, help="Localhost port (default: ephemeral)")
    parser.add_argument(
        "--viewer-url",
        help="Hosted viewer origin to hand off to (default: $ATHAR_VIEWER_URL)",
    )
    parser.add_argument("--cache-dir", help="Override persistent signature cache directory")
    return parser


def _size_warnings(paths: list[Path], threshold: int = SIZE_WARN_BYTES) -> list[str]:
    messages = []
    for path in paths:
        size = path.stat().st_size
        if size > threshold:
            messages.append(
                f"Warning: {path.name} is {size / (1024 * 1024):.0f} MB "
                f"(viewer v1 targets ≤ {threshold // (1024 * 1024)} MB; expect a slow load)."
            )
    return messages


def _hosted_url(args: argparse.Namespace) -> str | None:
    if args.offline:
        return None
    url = args.viewer_url or os.environ.get("ATHAR_VIEWER_URL") or ""
    url = url.strip()
    return url or None


def _reachable(url: str, timeout: float = 3.0) -> bool:
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True  # responded — reachable, whatever the status
    except (urllib.error.URLError, OSError, ValueError):
        return False


def prepare_view(args: argparse.Namespace) -> ViewSession:
    old_path = Path(args.old)
    new_path = Path(args.new)
    for path in (old_path, new_path):
        if not path.is_file():
            raise FileNotFoundError(f"No such IFC file: {path}")

    messages = _size_warnings([old_path, new_path])
    report_bytes = build_report_bytes(old_path, new_path, cache_dir=args.cache_dir)

    hosted = _hosted_url(args)
    if hosted is not None and not _reachable(hosted):
        messages.append(f"Hosted viewer {hosted} is unreachable — falling back to offline mode.")
        hosted = None

    if hosted is not None:
        parsed = urllib.parse.urlsplit(hosted)
        allowed_origin = f"{parsed.scheme}://{parsed.netloc}"
        server = make_server(
            old_path,
            new_path,
            report_bytes,
            dist_dir=None,
            port=args.port,
            allowed_origin=allowed_origin,
        )
        separator = "&" if parsed.query else "?"
        url = f"{hosted}{separator}src={urllib.parse.quote(server.origin, safe='')}"
        return ViewSession(server=server, url=url, mode="hosted", messages=messages)

    dist_dir = find_dist()
    if dist_dir is None:
        raise FileNotFoundError(
            "No built viewer found. Build it with `make viewer-build` (requires bun), "
            "set ATHAR_VIEWER_DIST to a viewer build, or configure a hosted viewer "
            "via --viewer-url / ATHAR_VIEWER_URL."
        )
    server = make_server(
        old_path,
        new_path,
        report_bytes,
        dist_dir=dist_dir,
        port=args.port,
        allowed_origin="*",
    )
    url = f"{server.origin}/?src={urllib.parse.quote(server.origin, safe='')}"
    return ViewSession(server=server, url=url, mode="offline", messages=messages)


def run_view(args: argparse.Namespace) -> int:
    try:
        session = prepare_view(args)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    for message in session.messages:
        print(message)
    print(f"Serving IFC pair on {session.server.origin} ({session.mode} mode)")
    print(f"Viewer: {session.url}")
    if not args.no_open:
        webbrowser.open(session.url)
    print("Ctrl-C to stop.")
    try:
        session.server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        session.server.shutdown()
        session.server.server_close()
    return 0
