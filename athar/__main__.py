"""Minimal CLI for Athar core engine — for computers."""

import sys
import json
import argparse

from athar.diff_engine import diff_files, stream_diff_files
from athar.guid_policy import GUID_POLICY_CHOICES, GUID_POLICY_FAIL_FAST
from athar.profile_policy import DEFAULT_PROFILE, SUPPORTED_PROFILES

def main():
    parser = argparse.ArgumentParser(prog="athar-core")
    parser.add_argument("old", help="Path to old IFC file")
    parser.add_argument("new", help="Path to new IFC file")
    parser.add_argument(
        "--profile",
        choices=SUPPORTED_PROFILES,
        default=DEFAULT_PROFILE,
        help="Canonicalization profile",
    )
    parser.add_argument(
        "--guid-policy",
        choices=GUID_POLICY_CHOICES,
        default=GUID_POLICY_FAIL_FAST,
        help="Policy for duplicate/invalid GlobalId handling",
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
        if args.stream != "none":
            for line in stream_diff_files(
                args.old,
                args.new,
                profile=args.profile,
                guid_policy=args.guid_policy,
                mode=args.stream,
                chunk_size=args.chunk_size,
            ):
                print(line)
            return

        result = diff_files(args.old, args.new, profile=args.profile, guid_policy=args.guid_policy)
        if args.stream == "none":
            json.dump(result, sys.stdout, indent=2)
            print()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
