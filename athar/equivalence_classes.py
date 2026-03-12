"""Deterministic C: class assignment for unresolved ambiguous partitions."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def apply_ambiguous_equivalence_classes(
    *,
    old_ids: dict[int, str],
    old_identity: dict[int, dict[str, Any]],
    new_ids: dict[int, str],
    new_identity: dict[int, dict[str, Any]],
    partitions: list[dict[str, Any]],
) -> None:
    """Assign deterministic C: IDs for unresolved ambiguous old/new partitions."""
    for partition in sorted(partitions, key=_partition_sort_key):
        old_steps = sorted(set(partition.get("old_steps", [])))
        new_steps = sorted(set(partition.get("new_steps", [])))
        if not old_steps or not new_steps:
            continue

        class_id = _class_id_for_partition(partition, old_steps, new_steps, old_ids, new_ids)
        matched_on = {
            "stage": "equivalence_class",
            "source_stage": partition.get("stage"),
            "reason": partition.get("reason"),
            "old_count": len(old_steps),
            "new_count": len(new_steps),
        }

        for step_id in old_steps:
            old_ids[step_id] = class_id
            old_identity[step_id] = {
                "match_method": "equivalence_class",
                "match_confidence": 0.0,
                "matched_on": matched_on,
            }

        for step_id in new_steps:
            new_ids[step_id] = class_id
            new_identity[step_id] = {
                "match_method": "equivalence_class",
                "match_confidence": 0.0,
                "matched_on": matched_on,
            }


def _partition_sort_key(partition: dict[str, Any]) -> tuple[str, str, str, str]:
    entity_type = str(partition.get("entity_type") or "")
    stage = str(partition.get("stage") or "")
    reason = str(partition.get("reason") or "")
    old_steps = ",".join(str(step) for step in sorted(set(partition.get("old_steps", []))))
    new_steps = ",".join(str(step) for step in sorted(set(partition.get("new_steps", []))))
    return (entity_type, stage, reason, f"{old_steps}|{new_steps}")


def _class_id_for_partition(
    partition: dict[str, Any],
    old_steps: list[int],
    new_steps: list[int],
    old_ids: dict[int, str],
    new_ids: dict[int, str],
) -> str:
    payload = {
        "entity_type": partition.get("entity_type"),
        "stage": partition.get("stage"),
        "reason": partition.get("reason"),
        "old_ids": sorted(old_ids.get(step, f"step:{step}") for step in old_steps),
        "new_ids": sorted(new_ids.get(step, f"step:{step}") for step in new_steps),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"C:{hashlib.sha256(blob.encode('utf-8')).hexdigest()}"
