"""Profile policy and volatility filtering for graph diffing."""

from __future__ import annotations

from typing import Any

from .canonical_values import PROFILE_RAW_EXACT, PROFILE_SEMANTIC_STABLE

SUPPORTED_PROFILES = (PROFILE_RAW_EXACT, PROFILE_SEMANTIC_STABLE)
DEFAULT_PROFILE = PROFILE_SEMANTIC_STABLE

_SEMANTIC_STABLE_VOLATILE_ATTRIBUTE_NAMES = frozenset({"OwnerHistory"})
_SEMANTIC_STABLE_VOLATILE_REF_PATH_PREFIXES = ("/OwnerHistory",)
_SEMANTIC_STABLE_VOLATILE_ENTITY_TYPES = frozenset({"IfcOwnerHistory"})


def validate_profile(profile: str) -> str:
    if profile not in SUPPORTED_PROFILES:
        raise ValueError(f"Unknown profile: {profile!r}")
    return profile


def entity_for_profile(entity: dict, *, profile: str) -> dict:
    validate_profile(profile)
    if profile != PROFILE_SEMANTIC_STABLE:
        return entity

    if entity.get("entity_type") in _SEMANTIC_STABLE_VOLATILE_ENTITY_TYPES:
        return {
            "entity_type": entity.get("entity_type"),
            "attributes": {},
            "refs": [],
        }

    attributes = {
        name: value
        for name, value in (entity.get("attributes") or {}).items()
        if name not in _SEMANTIC_STABLE_VOLATILE_ATTRIBUTE_NAMES
    }
    refs = [
        ref
        for ref in (entity.get("refs") or [])
        if not any(
            str(ref.get("path", "")).startswith(prefix)
            for prefix in _SEMANTIC_STABLE_VOLATILE_REF_PATH_PREFIXES
        )
    ]
    return {
        "entity_type": entity.get("entity_type"),
        "attributes": attributes,
        "refs": refs,
    }
