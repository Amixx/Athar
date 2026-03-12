"""Geometry representation policy controls for diff emission."""

from __future__ import annotations

GEOMETRY_POLICY_STRICT_SYNTAX = "strict_syntax"
GEOMETRY_POLICY_INVARIANT_PROBE = "invariant_probe"
GEOMETRY_POLICY_CHOICES = (
    GEOMETRY_POLICY_STRICT_SYNTAX,
    GEOMETRY_POLICY_INVARIANT_PROBE,
)


def validate_geometry_policy(policy: str) -> str:
    if policy not in GEOMETRY_POLICY_CHOICES:
        raise ValueError(f"Unknown geometry policy: {policy!r}")
    return policy

