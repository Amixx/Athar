"""Structural identity helpers for the low-level diff layer.

This provides a deterministic, GUID-free structural hash (H) seed that
ignores STEP IDs and inverse attributes. It is a correctness-first
baseline before WL refinement is introduced.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any


def structural_payload(entity: dict) -> dict:
    """Build a canonical payload for structural hashing."""
    attrs = _strip_ref_ids(entity.get("attributes", {}))
    edges = _edge_signature(entity.get("refs", []))
    return {
        "entity_type": entity.get("entity_type"),
        "attributes": attrs,
        "edges": edges,
    }


def structural_hash(entity: dict) -> str:
    """Compute a deterministic SHA-256 hash for an entity payload."""
    payload = structural_payload(entity)
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def compute_structural_hashes(graph: dict) -> dict[int, str]:
    """Compute structural hashes for every entity in the graph IR."""
    return {
        step_id: structural_hash(entity)
        for step_id, entity in graph.get("entities", {}).items()
    }


def _strip_ref_ids(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            return {"kind": "ref"}
        return {k: _strip_ref_ids(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_ref_ids(v) for v in value]
    return value


def _edge_signature(refs: list[dict]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        counts[(ref.get("path", ""), ref.get("target_type"))] += 1
    edges = [
        {"path": path, "target_type": target_type, "count": count}
        for (path, target_type), count in counts.items()
    ]
    edges.sort(key=lambda item: (item["path"], item["target_type"] or "", item["count"]))
    return edges
