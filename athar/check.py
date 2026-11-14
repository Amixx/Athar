"""CI policy gates over Athar delta reports."""

from __future__ import annotations

import functools
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

from ifcopenshell import ifcopenshell_wrapper

from athar.bottom.constants import SUPPORTED_SCHEMAS
from athar.bottom.schema_dict import canonical_class_name


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
    return _class_keyed(value, key, _count_limit)


def _class_mm_limits(value: Any, key: str) -> dict[str, float]:
    return _class_keyed(value, key, _mm_limit)


def _class_keyed(value: Any, key: str, parse) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, limit in _object(value, key).items():
        canonical = _class_name(name, key)
        if canonical in out:
            raise ValueError(f"{key} names {canonical} more than once")
        out[canonical] = parse(limit, f"{key}.{name}")
    return dict(sorted(out.items()))


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
        return [_class_name(value, key)]
    if isinstance(value, list) and value and all(isinstance(item, str) for item in value):
        return sorted({_class_name(item, key) for item in value})
    raise ValueError(f"{key} must be '*', a class name, or a non-empty list of class names")


def _class_name(name: Any, key: str) -> str:
    """Spell an IFC entity name the way reports do, case-insensitively.

    Reports carry the engine's canonical class (``IfcWallStandardCase`` is
    reported as ``IfcWall``), so a policy naming a collapsed class is applied to
    its canonical class.
    """
    if isinstance(name, str):
        for schema in SUPPORTED_SCHEMAS:
            try:
                declaration = ifcopenshell_wrapper.schema_by_name(schema).declaration_by_name(name)
            except RuntimeError:
                continue
            if isinstance(declaration, ifcopenshell_wrapper.entity):
                return canonical_class_name(declaration.name())
    raise ValueError(f"{key}: {name!r} is not an IFC entity in {' or '.join(SUPPORTED_SCHEMAS)}")


@functools.lru_cache(maxsize=None)
def _lineage(schema: str, class_name: str) -> tuple[str, ...]:
    """The class followed by its supertypes in ``schema``, nearest first."""
    try:
        declaration = ifcopenshell_wrapper.schema_by_name(schema).declaration_by_name(class_name)
    except RuntimeError as exc:
        raise ValueError(f"Report class {class_name!r} is not an entity in {schema}") from exc
    names = [class_name]
    while (declaration := declaration.supertype()) is not None:
        names.append(declaration.name())
    return tuple(names)


_POLICY_KEYS = {
    "description": _description,
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
    "forbid_property_addition": _property_filters,
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


def load_report(path: str | Path) -> Any:
    """Load an Athar JSON report; :func:`evaluate_report` validates it."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


_REPORT_SCHEMA_VERSION = 1
_SECTIONS = ("added", "deleted", "modified", "unchanged")
_ASPECT_STATES = ("changed", "unchanged")
_ENTITY_KEYS = ("class", "step_id", "guid", "name")


def _validate_report(report: Any, policy: dict[str, Any]) -> None:
    """Raise ``ValueError`` unless ``report`` carries everything ``policy`` reads."""
    if not isinstance(report, dict):
        raise ValueError("Report must be a JSON object")
    if report.get("engine") != "athar":
        raise ValueError("Report is not an Athar report: 'engine' must be \"athar\"")
    if not isinstance(report.get("canon_version"), str):
        raise ValueError("Report lacks 'canon_version'")
    audit = report.get("audit")
    if not isinstance(audit, dict) or audit.get("report_schema_version") != _REPORT_SCHEMA_VERSION:
        raise ValueError(f"Report 'audit.report_schema_version' must be {_REPORT_SCHEMA_VERSION}")
    schemas = report.get("schemas")
    if not isinstance(schemas, dict) or any(schemas.get(side) not in SUPPORTED_SCHEMAS for side in ("old", "new")):
        raise ValueError(f"Report 'schemas.old' and 'schemas.new' must each be one of {', '.join(SUPPORTED_SCHEMAS)}")
    if schemas["old"] != schemas["new"]:
        raise ValueError(
            f"Report compares {schemas['old']} with {schemas['new']}; Athar only diffs files of one schema"
        )
    stats = report.get("stats")
    if not isinstance(stats, dict):
        raise ValueError("Report lacks 'stats'")
    for section in _SECTIONS:
        rows = report.get(section)
        if not isinstance(rows, list):
            raise ValueError(f"Report lacks the '{section}' section")
        count = stats.get(section)
        if isinstance(count, bool) or not isinstance(count, int) or count != len(rows):
            raise ValueError(f"Report 'stats.{section}' must equal the {len(rows)} entries in '{section}'")
    for section in ("added", "deleted"):
        for index, item in enumerate(report[section]):
            _validate_entity(item, f"{section}[{index}]")
    for index, item in enumerate(report["modified"]):
        where = f"modified[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"Report '{where}' must be an object")
        _validate_entity(item.get("old"), f"{where}.old")
        _validate_entity(item.get("new"), f"{where}.new")
        aspects = item.get("aspects")
        if not isinstance(aspects, dict) or any(aspects.get(aspect) not in _ASPECT_STATES for aspect in ASPECTS):
            raise ValueError(f"Report '{where}.aspects' must give each of {', '.join(ASPECTS)} as changed or unchanged")
        delta = aspects.get("placement_delta_mm", "missing")
        if delta is not None and not _is_vector(delta):
            raise ValueError(f"Report '{where}.aspects.placement_delta_mm' must be null or three numbers")
        _validate_rule_detail(item, where, policy)


def _validate_entity(entity: Any, where: str) -> None:
    if not isinstance(entity, dict) or any(key not in entity for key in _ENTITY_KEYS):
        raise ValueError(f"Report '{where}' must be an entity with {', '.join(_ENTITY_KEYS)}")
    if not isinstance(entity["class"], str):
        raise ValueError(f"Report '{where}.class' must be a string")


def _validate_rule_detail(item: dict[str, Any], where: str, policy: dict[str, Any]) -> None:
    if "max_modified_change_scope" in policy and item.get("change_scope") not in CHANGE_SCOPES:
        raise ValueError(f"Policy rule max_modified_change_scope needs '{where}.change_scope', which the report lacks")
    property_rules = [key for key in _PROPERTY_RULES if key in policy]
    if not property_rules or item["aspects"]["data"] != "changed":
        return
    rule = property_rules[0]
    if not isinstance(item.get("data_delta"), list):
        raise ValueError(
            f"Policy rule {rule} needs the property detail of data-changed entities; "
            f"report '{where}' has no 'data_delta'"
        )
    deltas = item.get("property_deltas", {})
    if not isinstance(deltas, dict):
        raise ValueError(f"Policy rule {rule} needs '{where}.property_deltas' to be an object")
    for change, entries in deltas.items():
        value_keys = _PROPERTY_DELTA_VALUES.get(change)
        if value_keys is None or not isinstance(entries, list):
            raise ValueError(f"Policy rule {rule} cannot read '{where}.property_deltas.{change}'")
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or not isinstance(entry.get("pset"), str)
                or not isinstance(entry.get("name"), str)
                or not isinstance(entry.get("inherited"), bool)
                or any(key not in entry for key in value_keys)
            ):
                raise ValueError(
                    f"Policy rule {rule} needs pset, name, inherited and {', '.join(value_keys)} "
                    f"on every '{where}.property_deltas.{change}' entry"
                )


_PROPERTY_RULES = ("forbid_property_removal", "forbid_property_value_change", "forbid_property_addition")
_PROPERTY_DELTA_VALUES = {
    "changed": ("old_value", "new_value"),
    "added": ("new_value",),
    "removed": ("old_value",),
    "count_changed": ("old_count", "new_count"),
}


def _is_vector(value: Any) -> bool:
    return (
        isinstance(value, (list, tuple))
        and len(value) == 3
        and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in value)
    )


def evaluate_report(report: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a report against a CI policy (see :func:`normalize_policy`).

    The policy format is deliberately small and JSON-only:

    - ``description``: free text label
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
      ["IfcWall"]}`` — fail when a protected property's value changes or is
      removed (see :func:`_check_property_value_changes`)
    - ``forbid_property_addition``: same shape — fail when a matched entity
      gains a protected property

    Any other key is an error.
    """

    policy = normalize_policy(policy)
    _validate_report(report, policy)
    violations: list[dict[str, Any]] = []
    _check_section_limits(report, policy, violations)
    _check_class_limits(report, policy, violations)
    _check_scope_limits(report, policy, violations)
    _check_site_placement(report, policy, violations)
    _check_placement_limits(report, policy, violations)
    _check_aspect_bans(report, policy, violations)
    _check_property_removal(report, policy, violations)
    _check_property_value_changes(report, policy, violations)
    _check_property_additions(report, policy, violations)

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


def schema_mismatch_result(old_schema: str, new_schema: str) -> dict[str, Any]:
    """The failing verdict for a delivery whose schema differs from its baseline.

    No report exists for such a pair, so the verdict has no summary.
    """
    return {
        "ok": False,
        "violations": [
            {
                "code": "schema_mismatch",
                "message": f"Schema changed from {old_schema} to {new_schema}; the files cannot be diffed",
                "old": old_schema,
                "new": new_schema,
            }
        ],
    }


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
        schema = _report_schema(report)
        classes = [_item_class(item, section) for item in report[section]]
        for class_name, limit in policy[key].items():
            actual = sum(1 for item_class in classes if class_name in _lineage(schema, item_class))
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
    counts = Counter(item["change_scope"] for item in report["modified"])
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
        for item in report["modified"]
        if _modified_class(item) == "IfcSite" and item["aspects"]["placement"] == "changed"
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

    schema = _report_schema(report)
    offenders: list[dict[str, Any]] = []
    for item in report["modified"]:
        delta = item["aspects"]["placement_delta_mm"]
        if delta is None:
            continue
        norm = math.hypot(*delta)
        lineage = _lineage(schema, _modified_class(item))
        limit = next((class_limits[name] for name in lineage if name in class_limits), global_limit)
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
            for item in report["modified"]
            if item["aspects"][aspect] == "changed" and _class_matches(classes, report, item)
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
    for item in report["modified"]:
        if not _class_matches(classes, report, item):
            continue
        removed = item.get("property_deltas", {}).get("removed", [])
        if not removed:
            continue
        ref = _modified_entity_ref(item)
        ref["removed_properties"] = [
            {"pset": entry["pset"], "name": entry["name"], "inherited": entry["inherited"]} for entry in removed
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
    """Fail when a protected property's value on a matched entity may differ.

    Reads each modified entity's ``property_deltas``, keyed by (pset, name,
    inherited-from-type). A protected name violates the rule when an entry is
    ``changed``; when it is ``removed`` and no ``added`` entry of that name on
    the same entity carries an equal value (a move between psets, or between
    the occurrence and its type, with the value kept is not a change); or when
    it is in ``count_changed``. The report keeps one value per key, so for a
    key that occurs more than once on an entity a count difference is the only
    signal, and the rule fails on it without knowing which copy changed.
    """
    for prop_name, classes in policy.get("forbid_property_value_change", {}).items():
        affected: list[dict[str, Any]] = []
        for item in report["modified"]:
            if not _class_matches(classes, report, item):
                continue
            changes = _property_value_changes(item.get("property_deltas", {}), prop_name)
            if changes:
                affected.append({**_modified_entity_ref(item), "property": prop_name, "changes": changes})
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


def _check_property_additions(report: dict[str, Any], policy: dict[str, Any], violations: list[dict[str, Any]]) -> None:
    """Fail when a matched entity gains a protected property it did not carry.

    An ``added`` entry that pairs with a ``removed`` entry of the same name and
    an equal value is a move, not an addition.
    """
    for prop_name, classes in policy.get("forbid_property_addition", {}).items():
        affected: list[dict[str, Any]] = []
        for item in report["modified"]:
            if not _class_matches(classes, report, item):
                continue
            _, added = _unpaired_moves(item.get("property_deltas", {}), prop_name)
            if added:
                affected.append(
                    {
                        **_modified_entity_ref(item),
                        "property": prop_name,
                        "added": [_property_value(entry, "new_value") for entry in added],
                    }
                )
        if affected:
            violations.append(
                {
                    "code": "property_added",
                    "message": f"Protected property {prop_name} added to {len(affected)} entity(s)",
                    "property": prop_name,
                    "count": len(affected),
                    "affected": affected[:20],
                }
            )


def _property_value_changes(deltas: dict[str, list[dict[str, Any]]], prop_name: str) -> list[dict[str, Any]]:
    changes = [
        {"kind": "changed", **_property_value(entry, "old_value", "new_value")}
        for entry in deltas.get("changed", [])
        if entry["name"] == prop_name
    ]
    removed, added = _unpaired_moves(deltas, prop_name)
    if removed:
        changes += [{"kind": "removed", **_property_value(entry, "old_value")} for entry in removed]
        changes += [{"kind": "added", **_property_value(entry, "new_value")} for entry in added]
    changes += [
        {"kind": "count_changed", **_property_value(entry, "old_count", "new_count")}
        for entry in deltas.get("count_changed", [])
        if entry["name"] == prop_name
    ]
    return changes


def _unpaired_moves(
    deltas: dict[str, list[dict[str, Any]]], prop_name: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Removed and added entries of ``prop_name`` left after pairing equal values, in report order."""
    removed = [entry for entry in deltas.get("removed", []) if entry["name"] == prop_name]
    added = [entry for entry in deltas.get("added", []) if entry["name"] == prop_name]
    unpaired_removed = []
    for entry in removed:
        match = next((i for i, candidate in enumerate(added) if candidate["new_value"] == entry["old_value"]), None)
        if match is None:
            unpaired_removed.append(entry)
        else:
            del added[match]
    return unpaired_removed, added


def _property_value(entry: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {"pset": entry["pset"], "inherited": entry["inherited"], **{key: entry[key] for key in keys}}


def _section_count(report: dict[str, Any], section: str) -> int:
    return report["stats"][section]


def _item_class(item: dict[str, Any], section: str) -> str:
    return _modified_class(item) if section == "modified" else item["class"]


def _report_schema(report: dict[str, Any]) -> str:
    return report["schemas"]["new"]


def _modified_class(item: dict[str, Any]) -> str:
    return item["new"]["class"]


def _modified_entity_ref(item: dict[str, Any]) -> dict[str, Any]:
    return {key: item["new"][key] for key in _ENTITY_KEYS}


def _class_matches(classes: str | list[str], report: dict[str, Any], item: dict[str, Any]) -> bool:
    if classes == "*":
        return True
    return any(name in classes for name in _lineage(_report_schema(report), _modified_class(item)))


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
