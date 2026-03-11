"""Canonical value normalization for the graph diff engine.

This module intentionally focuses on deterministic, schema-driven
canonicalization. Entity references must be handled by the caller.
"""

from __future__ import annotations

import json
import math
from typing import Any, Callable, Iterable

PROFILE_RAW_EXACT = "raw_exact"
PROFILE_SEMANTIC_STABLE = "semantic_stable"


class CanonicalizationError(ValueError):
    """Raised when a value cannot be canonically represented."""


def is_ifc_entity(value: Any) -> bool:
    """Best-effort detection of IfcOpenShell entity instances."""
    return hasattr(value, "is_a") and callable(value.is_a)


def canonical_float(
    value: float,
    *,
    profile: str,
    quantize: Callable[[float], float] | None = None,
) -> str:
    """Return canonical string for a float according to profile rules."""
    if not math.isfinite(value):
        raise CanonicalizationError(f"Non-finite float not supported: {value!r}")
    if value == 0.0:
        value = 0.0
    if profile == PROFILE_SEMANTIC_STABLE and quantize is not None:
        value = quantize(value)
        if not math.isfinite(value):
            raise CanonicalizationError(
                f"Quantizer produced non-finite float: {value!r}"
            )
        if value == 0.0:
            value = 0.0
    elif profile != PROFILE_RAW_EXACT and profile != PROFILE_SEMANTIC_STABLE:
        raise CanonicalizationError(f"Unknown profile: {profile!r}")
    return format(value, ".17g")


def canonical_scalar(
    value: Any,
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    """Canonicalize scalar values (null/bool/int/real/string)."""
    if is_ifc_entity(value):
        raise CanonicalizationError("Entity references must be handled separately")
    if value is None:
        return {"kind": "null"}
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, int):
        return {"kind": "int", "value": value}
    if isinstance(value, float):
        return {
            "kind": "real",
            "value": canonical_float(value, profile=profile, quantize=quantize),
        }
    if isinstance(value, str):
        return {"kind": "string", "value": value}
    raise CanonicalizationError(f"Unsupported scalar type: {type(value)!r}")


def canonical_simple(
    ifc_type: str,
    value: Any,
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    """Canonicalize wrapped simple types (IfcLabel, IfcIdentifier, etc.)."""
    return {
        "kind": "simple",
        "type": ifc_type,
        "value": canonical_value(value, profile=profile, quantize=quantize),
    }


def canonical_select(
    branch_type: str,
    value: Any,
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    """Canonicalize SELECT values with explicit branch type."""
    return {
        "kind": "select",
        "type": branch_type,
        "value": canonical_value(value, profile=profile, quantize=quantize),
    }


def _sorted_canonical_items(
    items: Iterable[Any],
    *,
    profile: str,
    quantize: Callable[[float], float] | None,
    item_canon: Callable[[Any], dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    canonicalized = [
        (canonical_value(item, profile=profile, quantize=quantize) if item_canon is None
         else item_canon(item))
        for item in items
    ]
    canonicalized.sort(key=canonical_string)
    return canonicalized


def canonical_list(
    items: Iterable[Any],
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
    item_canon: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Canonicalize LIST/ARRAY aggregates preserving order."""
    canonicalized = [
        (canonical_value(item, profile=profile, quantize=quantize) if item_canon is None
         else item_canon(item))
        for item in items
    ]
    return {"kind": "list", "items": canonicalized}


def canonical_set(
    items: Iterable[Any],
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
    item_canon: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Canonicalize SET aggregates with deterministic ordering."""
    return {
        "kind": "set",
        "items": _sorted_canonical_items(
            items, profile=profile, quantize=quantize, item_canon=item_canon
        ),
    }


def canonical_bag(
    items: Iterable[Any],
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
    item_canon: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Canonicalize BAG aggregates with multiplicity preserved."""
    return {
        "kind": "bag",
        "items": _sorted_canonical_items(
            items, profile=profile, quantize=quantize, item_canon=item_canon
        ),
    }


def canonical_value(
    value: Any,
    *,
    profile: str = PROFILE_RAW_EXACT,
    quantize: Callable[[float], float] | None = None,
) -> dict[str, Any]:
    """Best-effort canonicalization for scalar and ordered aggregates.

    This method is a convenience for the reference prototype. Schema-aware
    callers should use the explicit aggregate and wrapper helpers.
    """
    if isinstance(value, (list, tuple)):
        return canonical_list(value, profile=profile, quantize=quantize)
    if isinstance(value, (set, frozenset)):
        return canonical_set(value, profile=profile, quantize=quantize)
    return canonical_scalar(value, profile=profile, quantize=quantize)


def canonical_string(value: dict[str, Any]) -> str:
    """Stable JSON string for a canonical value."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
