"""Minimal CLI for Athar core engine — for computers."""

import sys
import json
import argparse
import os
from typing import Any

from athar.diff_engine import diff_files, stream_diff_files
from athar.diff_engine_markers import OWNER_INDEX_DISK_THRESHOLD_ENV
from athar.geometry_policy import GEOMETRY_POLICY_CHOICES, GEOMETRY_POLICY_STRICT_SYNTAX
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
        "--geometry-policy",
        choices=GEOMETRY_POLICY_CHOICES,
        default=GEOMETRY_POLICY_STRICT_SYNTAX,
        help="Geometry representation policy",
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
    parser.add_argument(
        "--timings",
        action="store_true",
        help="Include non-deterministic runtime timings in stats.timings_ms",
    )
    parser.add_argument(
        "--root-remap-guid-overlap-threshold",
        type=float,
        default=None,
        help="Override root remap GUID-overlap gate (0..1)",
    )
    parser.add_argument(
        "--root-remap-score-threshold",
        type=float,
        default=None,
        help="Override root remap scored-assignment threshold (0..1)",
    )
    parser.add_argument(
        "--root-remap-score-margin",
        type=float,
        default=None,
        help="Override root remap scored-assignment tie margin (0..1)",
    )
    parser.add_argument(
        "--root-remap-assignment-max",
        type=int,
        default=None,
        help="Override root remap assignment cap",
    )
    parser.add_argument(
        "--secondary-score-threshold",
        type=float,
        default=None,
        help="Override secondary matcher score threshold (0..1)",
    )
    parser.add_argument(
        "--secondary-score-margin",
        type=float,
        default=None,
        help="Override secondary matcher ambiguity margin (0..1)",
    )
    parser.add_argument(
        "--secondary-assignment-max",
        type=int,
        default=None,
        help="Override secondary matcher assignment cap",
    )
    parser.add_argument(
        "--secondary-depth2-max",
        type=int,
        default=None,
        help="Override iterative deepening depth-2 block limit",
    )
    parser.add_argument(
        "--secondary-depth3-max",
        type=int,
        default=None,
        help="Override iterative deepening depth-3 block limit",
    )
    parser.add_argument(
        "--secondary-unresolved-limit",
        type=int,
        default=None,
        help="Override unresolved-entity gate for secondary matcher",
    )
    parser.add_argument(
        "--owner-index-disk-threshold",
        type=int,
        default=None,
        help=(
            "Optional estimated owner-pair threshold for spilling rooted-owner index to disk "
            f"(sets {OWNER_INDEX_DISK_THRESHOLD_ENV} for this run; <=0 disables spill)"
        ),
    )
    args = parser.parse_args()
    if args.owner_index_disk_threshold is not None:
        os.environ[OWNER_INDEX_DISK_THRESHOLD_ENV] = str(max(0, args.owner_index_disk_threshold))
    matcher_policy = _matcher_policy_overrides(args)

    try:
        if args.stream != "none":
            for line in stream_diff_files(
                args.old,
                args.new,
                profile=args.profile,
                geometry_policy=args.geometry_policy,
                guid_policy=args.guid_policy,
                matcher_policy=matcher_policy,
                mode=args.stream,
                chunk_size=args.chunk_size,
                timings=args.timings,
            ):
                print(line)
            return

        result = diff_files(
            args.old,
            args.new,
            profile=args.profile,
            geometry_policy=args.geometry_policy,
            guid_policy=args.guid_policy,
            matcher_policy=matcher_policy,
            timings=args.timings,
        )
        if args.stream == "none":
            json.dump(result, sys.stdout, indent=2)
            print()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _matcher_policy_overrides(args: argparse.Namespace) -> dict[str, dict[str, Any]] | None:
    root_remap: dict[str, Any] = {}
    secondary_match: dict[str, Any] = {}

    if args.root_remap_guid_overlap_threshold is not None:
        root_remap["guid_overlap_threshold"] = args.root_remap_guid_overlap_threshold
    if args.root_remap_score_threshold is not None:
        root_remap["score_threshold"] = args.root_remap_score_threshold
    if args.root_remap_score_margin is not None:
        root_remap["score_margin"] = args.root_remap_score_margin
    if args.root_remap_assignment_max is not None:
        root_remap["assignment_max"] = args.root_remap_assignment_max

    if args.secondary_score_threshold is not None:
        secondary_match["score_threshold"] = args.secondary_score_threshold
    if args.secondary_score_margin is not None:
        secondary_match["score_margin"] = args.secondary_score_margin
    if args.secondary_assignment_max is not None:
        secondary_match["assignment_max"] = args.secondary_assignment_max
    if args.secondary_depth2_max is not None:
        secondary_match["depth2_max"] = args.secondary_depth2_max
    if args.secondary_depth3_max is not None:
        secondary_match["depth3_max"] = args.secondary_depth3_max
    if args.secondary_unresolved_limit is not None:
        secondary_match["unresolved_limit"] = args.secondary_unresolved_limit

    if not root_remap and not secondary_match:
        return None

    out: dict[str, dict[str, Any]] = {}
    if root_remap:
        out["root_remap"] = root_remap
    if secondary_match:
        out["secondary_match"] = secondary_match
    return out


if __name__ == "__main__":
    main()
