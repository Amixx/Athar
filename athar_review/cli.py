"""CLI entrypoint for the Athar Review UI (`athar review`)."""

from __future__ import annotations

import argparse
import sys
import webbrowser

from athar_store import BaselineStore
from athar_view.server import find_dist

from .server import DEFAULT_MAX_UPLOAD_MB, make_server


def build_review_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="athar review",
        description=(
            "Serve the Athar Review web app on this computer. In a browser, a reviewer "
            "drops a delivered IFC model, reads what changed against the approved "
            "baseline, and approves or rejects it. Every decision is recorded in the "
            "baseline store. The server listens on 127.0.0.1 only and prints its URL "
            "on start."
        ),
        epilog=(
            "The store lives in the directory given by --root, else $ATHAR_STORE_ROOT, "
            "else ~/.athar/store. Manage it from the command line with `athar store`."
        ),
    )
    parser.add_argument("--root", help="Store directory (default: $ATHAR_STORE_ROOT, else ~/.athar/store)")
    parser.add_argument("--port", type=int, default=0, help="Port on 127.0.0.1 (default: any free port)")
    parser.add_argument("--dist", help="Path to a built viewer dist (default: auto-detect)")
    parser.add_argument(
        "--max-upload-mb",
        type=int,
        default=DEFAULT_MAX_UPLOAD_MB,
        metavar="MB",
        help=(
            f"Largest IFC file accepted for upload, in megabytes (default: {DEFAULT_MAX_UPLOAD_MB:,}); "
            "larger uploads are refused with HTTP 413"
        ),
    )
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser")
    return parser


def run_review(args: argparse.Namespace) -> int:
    if args.max_upload_mb <= 0:
        print("athar review: --max-upload-mb must be a positive number of megabytes", file=sys.stderr)
        return 2
    dist_dir = find_dist(args.dist)
    if dist_dir is None:
        print(
            "Warning: no built UI found (run `make viewer-build`); serving the JSON API only.",
            file=sys.stderr,
            flush=True,
        )
    store = BaselineStore(args.root)
    server = make_server(
        store, dist_dir=dist_dir, port=args.port, max_upload_bytes=args.max_upload_mb * 1_000_000
    )
    print(f"Athar Review: {server.origin}/", flush=True)
    print(f"Store: {store.root}", flush=True)
    if not args.no_open:
        webbrowser.open(server.origin)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
