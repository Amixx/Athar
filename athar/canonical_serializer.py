"""Deterministic serializer for low-level identity records."""

from __future__ import annotations

import json
from typing import Any


def serialize_records(records: list[dict[str, Any]]) -> str:
    """Serialize records deterministically (sorted by id)."""
    ordered = sorted(records, key=lambda r: r.get("id", ""))
    lines = [json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
             for rec in ordered]
    return "\n".join(lines)


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
