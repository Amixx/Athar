"""Deterministic serializer for engine identity records."""

from __future__ import annotations

import json
from typing import Any


def serialize_records(records: list[dict[str, Any]]) -> str:
    """Serialize records deterministically (G:, then H:, then C:)."""
    ordered = sorted(records, key=_record_sort_key)
    lines = [json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
             for rec in ordered]
    return "\n".join(lines)


def _record_sort_key(record: dict[str, Any]) -> tuple[int, str]:
    entity_id = record.get("id", "")
    if entity_id.startswith("G:"):
        tier = 0
    elif entity_id.startswith("H:"):
        tier = 1
    elif entity_id.startswith("C:"):
        tier = 2
    else:
        tier = 3
    return (tier, entity_id)


def build_entity_record(
    *,
    entity_id: str,
    entity_type: str,
    attributes: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": entity_id,
        "entity_type": entity_type,
        "attributes": attributes,
    }


def build_class_record(
    *,
    class_id: str,
    entity_type: str,
    old_count: int,
    new_count: int,
    exemplar: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": class_id,
        "entity_type": entity_type,
        "old_count": old_count,
        "new_count": new_count,
        "exemplar": exemplar,
    }
