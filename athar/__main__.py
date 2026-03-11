"""Minimal CLI for Athar core engine — for computers."""

import sys
import json
import argparse
from athar.parser import parse
from athar.differ import diff

def main():
    parser = argparse.ArgumentParser(prog="athar-core")
    parser.add_argument("old", help="Path to old IFC file")
    parser.add_argument("new", help="Path to new IFC file")
    args = parser.parse_args()

    try:
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
