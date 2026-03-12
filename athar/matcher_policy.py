"""Matcher policy defaults and validation for remap/secondary stages."""

from __future__ import annotations

from typing import Any

from .matcher_graph_scoring import (
    SECONDARY_ASSIGNMENT_MAX,
    SECONDARY_DEEPENING_DEPTH2_MAX,
    SECONDARY_DEEPENING_DEPTH3_MAX,
    SECONDARY_SCORE_MARGIN,
    SECONDARY_SCORE_THRESHOLD,
)
from .root_remap import (
    GUID_OVERLAP_THRESHOLD,
    ROOT_REMAP_ASSIGNMENT_MAX,
    ROOT_REMAP_SCORE_MARGIN,
    ROOT_REMAP_SCORE_THRESHOLD,
)

ROOT_REMAP_POLICY_DEFAULTS = {
    "guid_overlap_threshold": GUID_OVERLAP_THRESHOLD,
    "score_threshold": ROOT_REMAP_SCORE_THRESHOLD,
    "score_margin": ROOT_REMAP_SCORE_MARGIN,
    "assignment_max": ROOT_REMAP_ASSIGNMENT_MAX,
}

SECONDARY_MATCH_POLICY_DEFAULTS = {
    "score_threshold": SECONDARY_SCORE_THRESHOLD,
    "score_margin": SECONDARY_SCORE_MARGIN,
    "assignment_max": SECONDARY_ASSIGNMENT_MAX,
    "depth2_max": SECONDARY_DEEPENING_DEPTH2_MAX,
    "depth3_max": SECONDARY_DEEPENING_DEPTH3_MAX,
}

DEFAULT_MATCHER_POLICY = {
    "root_remap": ROOT_REMAP_POLICY_DEFAULTS,
    "secondary_match": SECONDARY_MATCH_POLICY_DEFAULTS,
}

_SECTION_KEYS = {
    "root_remap": frozenset(ROOT_REMAP_POLICY_DEFAULTS),
    "secondary_match": frozenset(SECONDARY_MATCH_POLICY_DEFAULTS),
}


def default_matcher_policy() -> dict[str, dict[str, Any]]:
    """Return a mutable copy of matcher policy defaults."""
    return {
        "root_remap": dict(ROOT_REMAP_POLICY_DEFAULTS),
        "secondary_match": dict(SECONDARY_MATCH_POLICY_DEFAULTS),
    }


def resolve_matcher_policy(overrides: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    """Resolve matcher policy overrides against defaults with validation."""
    policy = default_matcher_policy()
    if overrides is None:
        return policy
    if not isinstance(overrides, dict):
        raise ValueError("matcher_policy must be a dictionary")

    unknown_sections = sorted(set(overrides) - set(_SECTION_KEYS))
    if unknown_sections:
        raise ValueError(f"Unknown matcher_policy section(s): {', '.join(unknown_sections)}")

    for section, section_overrides in overrides.items():
        if section_overrides is None:
            continue
        if not isinstance(section_overrides, dict):
            raise ValueError(f"matcher_policy.{section} must be a dictionary")
        unknown_fields = sorted(set(section_overrides) - _SECTION_KEYS[section])
        if unknown_fields:
            raise ValueError(
                f"Unknown matcher_policy.{section} field(s): {', '.join(unknown_fields)}"
            )
        policy[section].update(section_overrides)

    validate_matcher_policy(policy)
    return policy


def validate_matcher_policy(policy: dict[str, dict[str, Any]]) -> None:
    """Validate matcher policy payload."""
    if not isinstance(policy, dict):
        raise ValueError("matcher_policy must be a dictionary")
    for section, section_keys in _SECTION_KEYS.items():
        if section not in policy:
            raise ValueError(f"matcher_policy missing section: {section}")
        if not isinstance(policy[section], dict):
            raise ValueError(f"matcher_policy.{section} must be a dictionary")
        unknown = sorted(set(policy[section]) - section_keys)
        if unknown:
            raise ValueError(
                f"Unknown matcher_policy.{section} field(s): {', '.join(unknown)}"
            )

    _validate_ratio(policy["root_remap"]["guid_overlap_threshold"], "matcher_policy.root_remap.guid_overlap_threshold")
    _validate_ratio(policy["root_remap"]["score_threshold"], "matcher_policy.root_remap.score_threshold")
    _validate_ratio(policy["root_remap"]["score_margin"], "matcher_policy.root_remap.score_margin")
    _validate_positive_int(policy["root_remap"]["assignment_max"], "matcher_policy.root_remap.assignment_max")

    _validate_ratio(policy["secondary_match"]["score_threshold"], "matcher_policy.secondary_match.score_threshold")
    _validate_ratio(policy["secondary_match"]["score_margin"], "matcher_policy.secondary_match.score_margin")
    _validate_positive_int(policy["secondary_match"]["assignment_max"], "matcher_policy.secondary_match.assignment_max")
    _validate_positive_int(policy["secondary_match"]["depth2_max"], "matcher_policy.secondary_match.depth2_max")
    _validate_positive_int(policy["secondary_match"]["depth3_max"], "matcher_policy.secondary_match.depth3_max")
    if policy["secondary_match"]["depth3_max"] > policy["secondary_match"]["depth2_max"]:
        raise ValueError("matcher_policy.secondary_match.depth3_max must be <= depth2_max")


def _validate_ratio(value: Any, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number between 0 and 1")
    num = float(value)
    if num < 0.0 or num > 1.0:
        raise ValueError(f"{field} must be between 0 and 1")


def _validate_positive_int(value: Any, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer >= 1")
    if value < 1:
        raise ValueError(f"{field} must be >= 1")
