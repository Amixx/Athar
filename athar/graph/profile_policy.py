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

    entity_type = entity.get("entity_type")
    if entity_type in _SEMANTIC_STABLE_VOLATILE_ENTITY_TYPES:
        return {
            "entity_type": entity_type,
            "attributes": {},
            "refs": [],
        }

    attributes_in = entity.get("attributes") or {}
    refs_in = entity.get("refs") or []

    has_volatile_attribute = any(
        name in _SEMANTIC_STABLE_VOLATILE_ATTRIBUTE_NAMES
        for name in attributes_in
    )
    has_volatile_refs = any(
        str(ref.get("path", "")).startswith("/OwnerHistory")
        for ref in refs_in
    )
    if not has_volatile_attribute and not has_volatile_refs:
        # Fast path: no semantic-stable volatility present, keep original entity payload.
        return entity

    attributes = {
        name: value
        for name, value in attributes_in.items()
        if name not in _SEMANTIC_STABLE_VOLATILE_ATTRIBUTE_NAMES
    }
    refs = [
        ref
        for ref in refs_in
        if not str(ref.get("path", "")).startswith(_SEMANTIC_STABLE_VOLATILE_REF_PATH_PREFIXES)
    ]
    return {
        "entity_type": entity_type,
        "attributes": attributes,
        "refs": refs,
    }
