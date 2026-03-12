"""Structural hashing helpers for the graph diff engine."""

from __future__ import annotations

import hashlib
from collections import Counter
from typing import Any

from .graph_utils import edge_signature, strip_ref_ids


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
    hasher = hashlib.sha256()
    _hash_token(hasher, "entity_type")
    _hash_scalar(hasher, entity.get("entity_type"))

    _hash_token(hasher, "attributes")
    _hash_value_stripping_refs(hasher, entity.get("attributes", {}))

    _hash_token(hasher, "edges")
    _hash_edge_multiset(hasher, entity.get("refs", []))
    return hasher.hexdigest()


def compute_structural_hashes(graph: dict) -> dict[int, str]:
    """Compute structural hashes for every entity in the graph IR."""
    return {
        step_id: structural_hash(entity)
        for step_id, entity in graph.get("entities", {}).items()
    }


def _hash_edge_multiset(hasher: hashlib._Hash, refs: list[dict]) -> None:
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        counts[(ref.get("path", ""), ref.get("target_type"))] += 1
    hasher.update(b"E{")
    for path, target_type in sorted(counts, key=lambda item: (item[0], item[1] or "")):
        hasher.update(b"P")
        _hash_scalar(hasher, path)
        hasher.update(b"T")
        _hash_scalar(hasher, target_type)
        hasher.update(b"C")
        _hash_scalar(hasher, counts[(path, target_type)])
        hasher.update(b";")
    hasher.update(b"}")


def _hash_value_stripping_refs(hasher: hashlib._Hash, value: Any) -> None:
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            hasher.update(b"R")
            return
        hasher.update(b"{")
        for key in sorted(value):
            hasher.update(b"K")
            _hash_scalar(hasher, key)
            hasher.update(b"V")
            _hash_value_stripping_refs(hasher, value[key])
            hasher.update(b";")
        hasher.update(b"}")
        return
    if isinstance(value, list):
        hasher.update(b"[")
        for item in value:
            _hash_value_stripping_refs(hasher, item)
            hasher.update(b",")
        hasher.update(b"]")
        return
    _hash_scalar(hasher, value)


def _hash_token(hasher: hashlib._Hash, token: str) -> None:
    hasher.update(b"#")
    _hash_scalar(hasher, token)


def _hash_scalar(hasher: hashlib._Hash, value: Any) -> None:
    if value is None:
        hasher.update(b"N")
        return
    if isinstance(value, bool):
        hasher.update(b"B1" if value else b"B0")
        return
    if isinstance(value, int):
        hasher.update(b"I")
        hasher.update(str(value).encode("ascii"))
        hasher.update(b";")
        return
    if isinstance(value, float):
        hasher.update(b"F")
        hasher.update(repr(value).encode("ascii"))
        hasher.update(b";")
        return
    text = str(value)
    encoded = text.encode("utf-8")
    hasher.update(b"S")
    hasher.update(str(len(encoded)).encode("ascii"))
    hasher.update(b":")
    hasher.update(encoded)
