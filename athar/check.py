"""CI policy gates over Athar delta reports."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any


ASPECTS = ("geometry", "data", "topology", "placement")
CHANGE_SCOPES = ("intrinsic", "transitive", "mixed")


_POLICY_PACK_DIR = Path(__file__).parent / "policies"


def load_policy(path: str | Path) -> dict[str, Any]:
    """Load and normalize a JSON policy file."""
    with open(path, encoding="utf-8") as fh:
        policy = json.load(fh)
    return normalize_policy(policy)


def normalize_policy(policy: Any) -> dict[str, Any]:
    """Validate a policy and return its normalized form.

    Every key is checked up front; an unknown key, a wrong value type, a
    negative limit, an unknown change scope or an unknown aspect raises
    ``ValueError`` naming the key. The normalized form is itself a valid policy,
    so normalizing it again returns an equal dict.
    """
    if not isinstance(policy, dict):
        raise ValueError("Policy must be a JSON object")
    unknown = sorted(set(policy) - set(_POLICY_KEYS))
    if unknown:
        allowed = ", ".join(_POLICY_KEYS)
        raise ValueError(f"Unknown policy key(s): {', '.join(unknown)}. Allowed keys: {allowed}")
    return {key: _POLICY_KEYS[key](policy[key], key) for key in _POLICY_KEYS if key in policy}


def _description(value: Any, key: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _flag(value: Any, key: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be true or false")
    return value


def _count_limit(value: Any, key: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _mm_limit(value: Any, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{key} must be a non-negative number")
    return float(value)


def _object(value: Any, key: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value


def _class_count_limits(value: Any, key: str) -> dict[str, int]:
    return {
        name: _count_limit(limit, f"{key}.{name}") for name, limit in sorted(_object(value, key).items())
    }


def _class_mm_limits(value: Any, key: str) -> dict[str, float]:
    return {name: _mm_limit(limit, f"{key}.{name}") for name, limit in sorted(_object(value, key).items())}


def _scope_limits(value: Any, key: str) -> dict[str, int]:
    limits = _object(value, key)
    unknown = sorted(set(limits) - set(CHANGE_SCOPES))
    if unknown:
        raise ValueError(f"Unknown change scope(s) in {key}: {', '.join(unknown)}. Allowed: {', '.join(CHANGE_SCOPES)}")
    return {scope: _count_limit(limits[scope], f"{key}.{scope}") for scope in CHANGE_SCOPES if scope in limits}


def _aspect_bans(value: Any, key: str) -> dict[str, Any]:
    bans = _object(value, key)
    unknown = sorted(set(bans) - set(ASPECTS))
    if unknown:
        raise ValueError(f"Unknown aspect(s) in {key}: {', '.join(unknown)}. Allowed: {', '.join(ASPECTS)}")
    return {aspect: _class_filter(bans[aspect], f"{key}.{aspect}") for aspect in ASPECTS if aspect in bans}


def _property_filters(value: Any, key: str) -> dict[str, Any]:
    filters = _object(value, key)
    for name in filters:
        if not name:
            raise ValueError(f"{key} has an empty property name")
    return {name: _class_filter(spec, f"{key}.{name}") for name, spec in sorted(filters.items())}


def _class_filter(value: Any, key: str) -> str | list[str]:
    if value == "*":
        return "*"
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and value and all(isinstance(item, str) for item in value):
        return sorted(set(value))
    raise ValueError(f"{key} must be '*', a class name, or a non-empty list of class names")


_POLICY_KEYS = {
    "description": _description,
    "forbid_schema_change": _flag,
    "max_added": _count_limit,
    "max_deleted": _count_limit,
    "max_modified": _count_limit,
    "max_added_by_class": _class_count_limits,
    "max_deleted_by_class": _class_count_limits,
    "max_modified_by_class": _class_count_limits,
    "max_modified_change_scope": _scope_limits,
    "forbid_site_placement_change": _flag,
    "max_placement_delta_mm": _mm_limit,
    "max_placement_delta_mm_by_class": _class_mm_limits,
    "forbid_aspect_changes": _aspect_bans,
    "forbid_property_removal": _class_filter,
    "forbid_property_value_change": _property_filters,
}


def builtin_policy_packs() -> dict[str, Path]:
    """Map each shipped policy pack name to its JSON file."""
    if not _POLICY_PACK_DIR.is_dir():
        return {}
    return {path.stem: path for path in sorted(_POLICY_PACK_DIR.glob("*.json"))}


def resolve_policy(name_or_path: str | Path) -> dict[str, Any]:
    """Load a policy from a file path, or by shipped pack name.

    A value that names an existing file is loaded directly; otherwise it is
    looked up among the builtin packs (see :func:`builtin_policy_packs`).
    """
    path = Path(name_or_path)
    if path.is_file():
        return load_policy(path)
    packs = builtin_policy_packs()
    if str(name_or_path) in packs:
        return load_policy(packs[str(name_or_path)])
    available = ", ".join(packs) or "(none)"
    raise ValueError(
        f"Policy {name_or_path!r} is not a file and not a builtin pack. Available packs: {available}"
    )


def load_report(path: str | Path) -> dict[str, Any]:
    """Load an Athar JSON report."""
    with open(path, encoding="utf-8") as fh:
        report = json.load(fh)
    if not isinstance(report, dict):
        raise ValueError("Report must be a JSON object")
    return report


def evaluate_report(report: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a report against a CI policy (see :func:`normalize_policy`).

    The policy format is deliberately small and JSON-only:

    - ``description``: free text label
    - ``forbid_schema_change``: bool, defaults true
    - ``max_added`` / ``max_deleted`` / ``max_modified``: integer limits
    - ``max_added_by_class`` / ``max_deleted_by_class`` /
      ``max_modified_by_class``: ``{"IfcWall": 0}``
    - ``max_modified_change_scope``: ``{"intrinsic": 0, "mixed": 0}``
    - ``forbid_site_placement_change``: bool
    - ``max_placement_delta_mm``: number, Euclidean translation delta limit
    - ``max_placement_delta_mm_by_class``: ``{"IfcSite": 0}``
    - ``forbid_aspect_changes``: ``{"data": ["IfcWall"], "placement": "*"}``
    - ``forbid_property_removal``: ``"*"``, a class string, or a list of classes
      — fail when a matched entity drops a property/quantity value
    - ``forbid_property_value_change``: ``{"FireRating": "*", "LoadBearing":
      ["IfcWall"]}`` — fail when a protected property's value changes

    Any other key is an error.
    """

    policy = normalize_policy(policy)
    violations: list[dict[str, Any]] = []
    _check_schema(report, policy, violations)
    _check_section_limits(report, policy, violations)
    _check_class_limits(report, policy, violations)
    _check_scope_limits(report, policy, violations)
    _check_site_placement(report, policy, violations)
    _check_placement_limits(report, policy, violations)
    _check_aspect_bans(report, policy, violations)
    _check_property_removal(report, policy, violations)
    _check_property_value_changes(report, policy, violations)

    return {
        "ok": not violations,
        "violations": violations,
        "summary": {
            "added": _section_count(report, "added"),
            "deleted": _section_count(report, "deleted"),
            "modified": _section_count(report, "modified"),
            "unchanged": _section_count(report, "unchanged"),
        },
    }


def _check_schema(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    if policy.get("forbid_schema_change", True) is False:
        return
    schemas = report.get("schemas") or {}
    old_schema = schemas.get("old")
    new_schema = schemas.get("new")
    if old_schema and new_schema and old_schema != new_schema:
        violations.append(
            {
                "code": "schema_changed",
                "message": f"Schema changed from {old_schema} to {new_schema}",
                "old": old_schema,
                "new": new_schema,
            }
        )


def _check_section_limits(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    for section in ("added", "deleted", "modified"):
        key = f"max_{section}"
        if key not in policy:
            continue
        limit = policy[key]
        actual = _section_count(report, section)
        if actual > limit:
            violations.append(_limit_violation(f"{section}_limit", key, actual, limit))


def _check_class_limits(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    for section in ("added", "deleted", "modified"):
        key = f"max_{section}_by_class"
        if key not in policy:
            continue
        counts = _section_class_counts(report, section)
        for class_name, limit in policy[key].items():
            actual = counts.get(class_name, 0)
            if actual > limit:
                violations.append(
                    _limit_violation(
                        f"{section}_class_limit",
                        key,
                        actual,
                        limit,
                        class_name=class_name,
                    )
                )


def _check_scope_limits(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    key = "max_modified_change_scope"
    if key not in policy:
        return
    counts = Counter(item.get("change_scope") for item in report.get("modified", []))
    for scope, limit in policy[key].items():
        actual = counts.get(scope, 0)
        if actual > limit:
            violations.append(
                _limit_violation(
                    "modified_change_scope_limit",
                    key,
                    actual,
                    limit,
                    scope=scope,
                )
            )


def _check_site_placement(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    if not policy.get("forbid_site_placement_change", False):
        return
    affected = [
        _modified_entity_ref(item)
        for item in report.get("modified", [])
        if _modified_class(item) == "IfcSite" and item.get("aspects", {}).get("placement") == "changed"
    ]
    if affected:
        violations.append(
            {
                "code": "site_placement_changed",
                "message": f"IfcSite placement changed for {len(affected)} entity(s)",
                "count": len(affected),
                "affected": affected[:20],
            }
        )


def _check_placement_limits(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    global_limit = policy.get("max_placement_delta_mm")
    class_limits = policy.get("max_placement_delta_mm_by_class", {})
    if global_limit is None and not class_limits:
        return

    offenders: list[dict[str, Any]] = []
    for item in report.get("modified", []):
        delta = item.get("aspects", {}).get("placement_delta_mm")
        norm = _placement_norm(delta)
        if norm is None:
            continue
        limit = class_limits.get(_modified_class(item), global_limit)
        if limit is None:
            continue
        if norm > limit:
            offender = _modified_entity_ref(item)
            offender["placement_delta_mm"] = round(norm, 3)
            offender["limit"] = limit
            offenders.append(offender)
    if offenders:
        violations.append(
            {
                "code": "placement_delta_limit",
                "message": f"Placement delta exceeded for {len(offenders)} entity(s)",
                "count": len(offenders),
                "affected": offenders[:20],
            }
        )


def _check_aspect_bans(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    for aspect, classes in policy.get("forbid_aspect_changes", {}).items():
        affected = [
            _modified_entity_ref(item)
            for item in report.get("modified", [])
            if item.get("aspects", {}).get(aspect) == "changed" and _class_matches(classes, _modified_class(item))
        ]
        if affected:
            violations.append(
                {
                    "code": "aspect_change_forbidden",
                    "message": f"{aspect} changes are forbidden for {len(affected)} entity(s)",
                    "aspect": aspect,
                    "count": len(affected),
                    "affected": affected[:20],
                }
            )


def _check_property_removal(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    classes = policy.get("forbid_property_removal")
    if classes is None:
        return
    affected: list[dict[str, Any]] = []
    for item in report.get("modified", []):
        if not _class_matches(classes, _modified_class(item)):
            continue
        removed = (item.get("property_deltas") or {}).get("removed") or []
        if not removed:
            continue
        ref = _modified_entity_ref(item)
        ref["removed_properties"] = [
            {"pset": entry.get("pset"), "name": entry.get("name"), "inherited": entry.get("inherited")}
            for entry in removed
        ]
        affected.append(ref)
    if affected:
        violations.append(
            {
                "code": "property_removed",
                "message": f"Properties removed from {len(affected)} entity(s)",
                "count": len(affected),
                "affected": affected[:20],
            }
        )


def _check_property_value_changes(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    for prop_name, classes in policy.get("forbid_property_value_change", {}).items():
        affected: list[dict[str, Any]] = []
        for item in report.get("modified", []):
            if not _class_matches(classes, _modified_class(item)):
                continue
            changed = (item.get("property_deltas") or {}).get("changed") or []
            matches = [e for e in changed if e.get("name") == prop_name]
            if not matches:
                continue
            ref = _modified_entity_ref(item)
            ref["property"] = prop_name
            ref["changes"] = [
                {
                    "pset": e.get("pset"),
                    "inherited": e.get("inherited"),
                    "old_value": e.get("old_value"),
                    "new_value": e.get("new_value"),
                }
                for e in matches
            ]
            affected.append(ref)
        if affected:
            violations.append(
                {
                    "code": "property_value_changed",
                    "message": f"Protected property {prop_name} changed on {len(affected)} entity(s)",
                    "property": prop_name,
                    "count": len(affected),
                    "affected": affected[:20],
                }
            )


def _section_count(report: dict[str, Any], section: str) -> int:
    stats = report.get("stats") or {}
    value = stats.get(section)
    if isinstance(value, int):
        return value
    rows = report.get(section, [])
    return len(rows) if isinstance(rows, list) else 0


def _section_class_counts(report: dict[str, Any], section: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for item in report.get(section, []):
        class_name = _modified_class(item) if section == "modified" else item.get("class")
        if class_name:
            counts[str(class_name)] += 1
    return counts


def _modified_class(item: dict[str, Any]) -> str | None:
    new = item.get("new") or {}
    old = item.get("old") or {}
    return new.get("class") or old.get("class")


def _modified_entity_ref(item: dict[str, Any]) -> dict[str, Any]:
    new = item.get("new") or {}
    old = item.get("old") or {}
    entity = new or old
    return {
        "class": entity.get("class"),
        "step_id": entity.get("step_id"),
        "guid": entity.get("guid"),
        "name": entity.get("name"),
    }


def _placement_norm(delta: Any) -> float | None:
    if not isinstance(delta, (list, tuple)) or len(delta) != 3:
        return None
    try:
        return math.sqrt(sum(float(value) ** 2 for value in delta))
    except (TypeError, ValueError):
        return None


def _class_matches(classes: str | list[str], class_name: str | None) -> bool:
    return classes == "*" or class_name in classes


def _limit_violation(
    code: str,
    policy_key: str,
    actual: int,
    limit: int,
    **extra: Any,
) -> dict[str, Any]:
    out = {
        "code": code,
        "message": f"{policy_key} exceeded: {actual} > {limit}",
        "policy": policy_key,
        "actual": actual,
        "limit": limit,
    }
    out.update(extra)
    return out
