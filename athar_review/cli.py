"""CLI entrypoint for the Athar Review UI (`athar review`)."""

from __future__ import annotations

import argparse
import webbrowser

from athar_store import BaselineStore
from athar_view.server import find_dist

from .server import make_server


def build_review_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="athar review",
        description="Serve the local review UI over the baseline store",
    )
    parser.add_argument("--root", help="Store root directory (default: $ATHAR_STORE_ROOT or ~/.athar/store)")
    parser.add_argument("--port", type=int, default=0, help="Port to bind on 127.0.0.1 (default: ephemeral)")
    parser.add_argument("--dist", help="Path to a built viewer dist (default: auto-detect)")
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser")
    return parser


def run_review(args: argparse.Namespace) -> int:
    dist_dir = find_dist(args.dist)
    if dist_dir is None:
        print("Warning: no built UI found (run `make viewer-build`); serving the JSON API only.")
    store = BaselineStore(args.root)
    server = make_server(store, dist_dir=dist_dir, port=args.port)
    print(f"Athar Review on {server.origin} (store: {store.root})")
    if not args.no_open:
        webbrowser.open(server.origin)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
