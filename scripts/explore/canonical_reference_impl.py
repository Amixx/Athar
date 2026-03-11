"""Reference implementation for canonical value normalization.

This is an executable spec for the low-level diff layer's value grammar.
It is intentionally small and schema-agnostic.
"""

from __future__ import annotations

import argparse
import json

from athar.canonical_values import (
    PROFILE_RAW_EXACT,
    canonical_bag,
    canonical_list,
    canonical_scalar,
    canonical_select,
    canonical_set,
    canonical_simple,
)


def build_examples(profile: str) -> dict[str, dict]:
    return {
        "scalar_real": canonical_scalar(1.25, profile=profile),
        "scalar_string": canonical_scalar("Wall-001", profile=profile),
        "list_ordered": canonical_list([3, 1, 2], profile=profile),
        "set_sorted": canonical_set({3, 1, 2}, profile=profile),
        "bag_multiplicity": canonical_bag([2, 1, 2, 1], profile=profile),
        "simple_wrapper": canonical_simple("IfcLabel", "Door A", profile=profile),
        "select_branch": canonical_select("IfcBoolean", True, profile=profile),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        default=PROFILE_RAW_EXACT,
        choices=[PROFILE_RAW_EXACT, "semantic_stable"],
    )
    args = parser.parse_args()

    payload = build_examples(args.profile)
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
