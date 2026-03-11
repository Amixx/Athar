"""Minimal CLI for Athar core engine — for computers."""

import sys
import json
import argparse

from athar.differ import diff
from athar.diff_engine import diff_files
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
    args = parser.parse_args()

    try:
        if args.engine == "graph":
            result = diff_files(args.old, args.new, profile=args.profile)
        else:
            old_model = parse(args.old)
            new_model = parse(args.new)
            result = diff(old_model, new_model)
        json.dump(result, sys.stdout, indent=2)
        print()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
