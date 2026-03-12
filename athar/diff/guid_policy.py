"""GUID quality policy and deterministic disambiguation helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

GUID_POLICY_FAIL_FAST = "fail_fast"
GUID_POLICY_DISAMBIGUATE = "disambiguate"
GUID_POLICY_CHOICES = (GUID_POLICY_FAIL_FAST, GUID_POLICY_DISAMBIGUATE)


def validate_guid_policy(policy: str) -> str:
    if policy not in GUID_POLICY_CHOICES:
        raise ValueError(f"Unknown guid policy: {policy!r}")
    return policy


def guid_quality_index(entities: dict[int, dict]) -> dict[str, Any]:
    valid_steps_by_guid: dict[str, list[int]] = defaultdict(list)
    invalid_steps: list[int] = []

    for step_id, entity in entities.items():
        gid = entity.get("global_id")
        if gid is None:
            continue
        if _is_valid_guid(gid):
            valid_steps_by_guid[gid].append(step_id)
        else:
            invalid_steps.append(step_id)

    for steps in valid_steps_by_guid.values():
        steps.sort()
    invalid_steps.sort()

    duplicate_steps = {
        gid: steps
        for gid, steps in valid_steps_by_guid.items()
        if len(steps) > 1
    }
    return {
        "valid_steps_by_guid": dict(valid_steps_by_guid),
        "duplicate_steps": duplicate_steps,
        "invalid_steps": invalid_steps,
    }


def enforce_or_disambiguate_guid_policy(
    entities: dict[int, dict],
    *,
    policy: str,
    side: str,
) -> dict[str, Any]:
    validate_guid_policy(policy)
    index = guid_quality_index(entities)
    duplicates = index["duplicate_steps"]
    invalid_steps = index["invalid_steps"]

    if policy == GUID_POLICY_FAIL_FAST and (duplicates or invalid_steps):
        raise ValueError(_fail_fast_message(side, duplicates, invalid_steps))

    disambiguated: dict[int, dict[str, Any]] = {}
    if policy == GUID_POLICY_DISAMBIGUATE:
        for gid in sorted(duplicates):
            steps = duplicates[gid]
            for pos, step_id in enumerate(steps, start=1):
                disambiguated[step_id] = {
                    "entity_id": f"G!:{gid}#{pos}",
                    "reason": "duplicate_guid",
                    "guid": gid,
                    "ordinal": pos,
                }
        for pos, step_id in enumerate(invalid_steps, start=1):
            raw = entities.get(step_id, {}).get("global_id")
            raw_repr = str(raw) if raw is not None else "None"
            disambiguated[step_id] = {
                "entity_id": f"G!:INVALID#{pos}",
                "reason": "invalid_guid",
                "guid": raw_repr,
                "ordinal": pos,
            }

    return {
        "index": index,
        "disambiguated": disambiguated,
    }


def _is_valid_guid(gid: Any) -> bool:
    return isinstance(gid, str) and gid.strip() != ""


def _fail_fast_message(side: str, duplicates: dict[str, list[int]], invalid_steps: list[int]) -> str:
    duplicate_ids = sorted(duplicates)
    dup_summary = (
        f"duplicate GlobalId count={len(duplicate_ids)} sample={duplicate_ids[:3]}"
        if duplicate_ids
        else "duplicate GlobalId count=0"
    )
    invalid_summary = (
        f"invalid GlobalId count={len(invalid_steps)} sample_steps={invalid_steps[:5]}"
        if invalid_steps
        else "invalid GlobalId count=0"
    )
    return f"GUID policy violation ({side}): {dup_summary}; {invalid_summary}"
