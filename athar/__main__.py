"""Minimal CLI for Athar core engine — for computers."""

import sys
import json
import argparse

from athar.differ import diff
from athar.diff_engine import diff_files, stream_diff_files, stream_diff_result
from athar.parser import parse

def main():
    parser = argparse.ArgumentParser(prog="athar-core")
    parser.add_argument("old", help="Path to old IFC file")
    parser.add_argument("new", help="Path to new IFC file")
    parser.add_argument(
        "--engine",
        choices=["legacy", "graph"],
        default="legacy",
        help="Diff engine to use (legacy or graph-based)",
    )
    parser.add_argument(
        "--profile",
        choices=["raw_exact", "semantic_stable"],
        default="semantic_stable",
        help="Canonicalization profile for graph engine",
    )
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

    try:
        if args.engine == "graph":
            if args.stream != "none":
                for line in stream_diff_files(
                    args.old,
                    args.new,
                    profile=args.profile,
                    mode=args.stream,
                    chunk_size=args.chunk_size,
                ):
                    print(line)
                return
            result = diff_files(args.old, args.new, profile=args.profile)
        else:
            old_model = parse(args.old)
            new_model = parse(args.new)
            result = diff(old_model, new_model)
        if args.stream == "none":
            json.dump(result, sys.stdout, indent=2)
            print()
        else:
            for line in stream_diff_result(result, mode=args.stream, chunk_size=args.chunk_size):
                print(line)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
