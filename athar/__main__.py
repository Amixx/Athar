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
    args = parser.parse_args()
    if not args.old or not args.new:
        parser.error("the following arguments are required: old, new")

    try:
        if args.stream != "none":
            for line in stream_diff_files(
                args.old,
                args.new,
                mode=args.stream,
                chunk_size=args.chunk_size,
            ):
                print(line)
            return

        result = diff_files(args.old, args.new)
        json.dump(result, sys.stdout, indent=2)
        print()
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
