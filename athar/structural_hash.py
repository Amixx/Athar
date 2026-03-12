"""Structural hashing helpers for the graph diff engine."""

from __future__ import annotations

from .graph_utils import edge_signature, sha256_json, strip_ref_ids


def structural_payload(entity: dict) -> dict:
    """Build a canonical payload for structural hashing."""
    attrs = strip_ref_ids(entity.get("attributes", {}))
    edges = edge_signature(entity.get("refs", []))
    return {
        "entity_type": entity.get("entity_type"),
        "attributes": attrs,
        "edges": edges,
    }


def structural_hash(entity: dict) -> str:
    """Compute a deterministic SHA-256 hash for an entity payload."""
    payload = structural_payload(entity)
    return sha256_json(payload)


def compute_structural_hashes(graph: dict) -> dict[int, str]:
    """Compute structural hashes for every entity in the graph IR."""
    return {
        step_id: structural_hash(entity)
        for step_id, entity in graph.get("entities", {}).items()
    }
