"""CI policy gates over Athar delta reports."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any


ASPECTS = {"geometry", "data", "topology", "placement"}
SECTIONS = ("added", "deleted", "modified", "unchanged")


def load_policy(path: str | Path) -> dict[str, Any]:
    """Load a JSON policy file."""
    with open(path, encoding="utf-8") as fh:
        policy = json.load(fh)
    if not isinstance(policy, dict):
        raise ValueError("Policy must be a JSON object")
    return policy


def load_report(path: str | Path) -> dict[str, Any]:
    """Load an Athar JSON report."""
    with open(path, encoding="utf-8") as fh:
        report = json.load(fh)
    if not isinstance(report, dict):
        raise ValueError("Report must be a JSON object")
    return report


def evaluate_report(report: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a report against a CI policy.

    The policy format is deliberately small and JSON-only:

    - ``forbid_schema_change``: bool, defaults true
    - ``max_added`` / ``max_deleted`` / ``max_modified``: integer limits
    - ``max_added_by_class`` / ``max_deleted_by_class`` /
      ``max_modified_by_class``: ``{"IfcWall": 0}``
    - ``max_modified_change_scope``: ``{"intrinsic": 0, "mixed": 0}``
    - ``forbid_site_placement_change``: bool
    - ``max_placement_delta_mm``: number, Euclidean translation delta limit
    - ``max_placement_delta_mm_by_class``: ``{"IfcSite": 0}``
    - ``forbid_aspect_changes``: ``{"data": ["IfcWall"], "placement": "*"}`
    """

    violations: list[dict[str, Any]] = []
    _check_schema(report, policy, violations)
    _check_section_limits(report, policy, violations)
    _check_class_limits(report, policy, violations)
    _check_scope_limits(report, policy, violations)
    _check_site_placement(report, policy, violations)
    _check_placement_limits(report, policy, violations)
    _check_aspect_bans(report, policy, violations)

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
        limit = _as_int(policy[key], key)
        actual = _section_count(report, section)
        if actual > limit:
            violations.append(_limit_violation(f"{section}_limit", key, actual, limit))


def _check_class_limits(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    for section in ("added", "deleted", "modified"):
        key = f"max_{section}_by_class"
        limits = policy.get(key)
        if limits is None:
            continue
        if not isinstance(limits, dict):
            raise ValueError(f"{key} must be an object")
        counts = _section_class_counts(report, section)
        for class_name in sorted(limits):
            limit = _as_int(limits[class_name], f"{key}.{class_name}")
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
    limits = policy.get(key)
    if limits is None:
        return
    if not isinstance(limits, dict):
        raise ValueError(f"{key} must be an object")
    counts = Counter(item.get("change_scope") for item in report.get("modified", []))
    for scope in sorted(limits):
        limit = _as_int(limits[scope], f"{key}.{scope}")
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
    if class_limits and not isinstance(class_limits, dict):
        raise ValueError("max_placement_delta_mm_by_class must be an object")

    offenders: list[dict[str, Any]] = []
    for item in report.get("modified", []):
        delta = item.get("aspects", {}).get("placement_delta_mm")
        norm = _placement_norm(delta)
        if norm is None:
            continue
        class_name = _modified_class(item)
        limit_value = class_limits.get(class_name, global_limit)
        if limit_value is None:
            continue
        limit = _as_float(limit_value, f"placement limit for {class_name}")
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
    key = "forbid_aspect_changes"
    bans = policy.get(key)
    if bans is None:
        return
    if not isinstance(bans, dict):
        raise ValueError(f"{key} must be an object")

    for aspect in sorted(bans):
        if aspect not in ASPECTS:
            raise ValueError(f"Unsupported aspect in {key}: {aspect}")
        class_filter = _class_filter(bans[aspect], f"{key}.{aspect}")
        affected = [
            _modified_entity_ref(item)
            for item in report.get("modified", [])
            if item.get("aspects", {}).get(aspect) == "changed" and class_filter(_modified_class(item))
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


def _class_filter(value: Any, key: str):
    if value == "*":
        return lambda _class_name: True
    if isinstance(value, str):
        allowed = {value}
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        allowed = set(value)
    else:
        raise ValueError(f"{key} must be '*', a class string, or a list of class strings")
    return lambda class_name: class_name in allowed


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


def _as_int(value: Any, key: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _as_float(value: Any, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{key} must be a non-negative number")
    return float(value)
