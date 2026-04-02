"""Minimal CLI for the Athar core engine."""

from __future__ import annotations

import argparse
import json
import sys

from athar.engine import diff_files, stream_diff_files


def main() -> None:
    parser = argparse.ArgumentParser(prog="athar-core")
    parser.add_argument("old", nargs="?", help="Path to old IFC file")
    parser.add_argument("new", nargs="?", help="Path to new IFC file")
    parser.add_argument(
        "--stream",
        choices=["none", "ndjson", "chunked_json"],
        default="none",
        help="Output streaming mode",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for --stream chunked_json",
    )
    parser.add_argument(
        "--matcher-radius-m",
        type=float,
        default=0.5,
        help="Spatial fallback radius in meters (Phase 1 default: 0.5m)",
    )
    args = parser.parse_args()
    if not args.old or not args.new:
        parser.error("the following arguments are required: old, new")

    matcher_policy = {"spatial_radius_m": args.matcher_radius_m}
    try:
        if args.stream != "none":
            for line in stream_diff_files(
                args.old,
                args.new,
                matcher_policy=matcher_policy,
                mode=args.stream,
                chunk_size=args.chunk_size,
            ):
                print(line)
            return

        result = diff_files(
            args.old,
            args.new,
            matcher_policy=matcher_policy,
        )
        json.dump(result, sys.stdout, indent=2)
        print()
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
