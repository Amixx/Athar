"""Structural identity helpers for the graph diff engine.

This provides a deterministic, GUID-free structural hash (H) seed that
ignores STEP IDs and inverse attributes. It is a correctness-first
baseline before WL refinement is introduced.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections import Counter
from typing import Any, Callable

_DEFAULT_WL_ROUNDS = 8
_WL_ROUND_HASH_AUTO = "auto"
_WL_ROUND_HASH_SHA256 = "sha256"
_WL_ROUND_HASH_XXH3 = "xxh3_64"
_WL_ROUND_HASH_BLAKE3 = "blake3"
_WL_ROUND_HASH_BLAKE2B64 = "blake2b_64"
_WL_ROUND_HASH_CHOICES = frozenset({
    _WL_ROUND_HASH_AUTO,
    _WL_ROUND_HASH_SHA256,
    _WL_ROUND_HASH_XXH3,
    _WL_ROUND_HASH_BLAKE3,
    _WL_ROUND_HASH_BLAKE2B64,
})

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


def wl_refine_colors(
    graph: dict,
    *,
    max_rounds: int | None = None,
    round_hash: str = _WL_ROUND_HASH_AUTO,
) -> dict[int, str]:
    """Weisfeiler-Lehman color refinement over the explicit forward graph."""
    entities = graph.get("entities", {})
    if not entities:
        return {}

    hasher = _resolve_wl_round_hasher(round_hash)
    adjacency = _build_adjacency(entities)
    colors = {step_id: structural_hash(entity) for step_id, entity in entities.items()}

    rounds = _DEFAULT_WL_ROUNDS if max_rounds is None else max_rounds

    for _ in range(rounds):
        next_colors: dict[int, str] = {}
        changed = 0
        for step_id, entity in entities.items():
            neighbor_sig = _neighbor_signature(adjacency.get(step_id, []), colors)
            payload = {
                "self": colors[step_id],
                "neighbors": neighbor_sig,
            }
            blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
            next_color = hasher(blob.encode("utf-8"))
            next_colors[step_id] = next_color
            if next_color != colors[step_id]:
                changed += 1
        colors = next_colors
        if changed == 0:
            break

    return colors


def _resolve_wl_round_hasher(name: str) -> Callable[[bytes], str]:
    if name not in _WL_ROUND_HASH_CHOICES:
        raise ValueError(f"Unknown WL round hash: {name!r}")

    if name == _WL_ROUND_HASH_AUTO:
        for candidate in (_WL_ROUND_HASH_XXH3, _WL_ROUND_HASH_BLAKE3, _WL_ROUND_HASH_BLAKE2B64):
            hasher = _resolve_optional_hasher(candidate)
            if hasher is not None:
                return hasher
        return _sha256_hexdigest

    if name == _WL_ROUND_HASH_SHA256:
        return _sha256_hexdigest

    hasher = _resolve_optional_hasher(name)
    if hasher is None:
        raise ValueError(f"WL round hash backend unavailable: {name!r}")
    return hasher


def _resolve_optional_hasher(name: str) -> Callable[[bytes], str] | None:
    if name == _WL_ROUND_HASH_XXH3:
        if importlib.util.find_spec("xxhash") is None:
            return None
        xxhash = importlib.import_module("xxhash")
        return xxhash.xxh3_64_hexdigest

    if name == _WL_ROUND_HASH_BLAKE3:
        if importlib.util.find_spec("blake3") is None:
            return None
        blake3 = importlib.import_module("blake3")
        return lambda data: blake3.blake3(data).hexdigest()

    if name == _WL_ROUND_HASH_BLAKE2B64:
        return _blake2b64_hexdigest

    return None


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _blake2b64_hexdigest(data: bytes) -> str:
    return hashlib.blake2b(data, digest_size=8).hexdigest()


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


def _build_adjacency(entities: dict[int, dict]) -> dict[int, list[tuple[str, str | None, int]]]:
    adjacency: dict[int, list[tuple[str, str | None, int]]] = {}
    for step_id, entity in entities.items():
        for ref in entity.get("refs", []):
            adjacency.setdefault(step_id, []).append(
                (ref.get("path", ""), ref.get("target_type"), ref.get("target"))
            )
    return adjacency


def _neighbor_signature(
    edges: list[tuple[str, str | None, int]],
    colors: dict[int, str],
) -> list[dict[str, Any]]:
    items = []
    for path, target_type, target_id in edges:
        items.append({
            "path": path,
            "target_type": target_type,
            "color": colors.get(target_id, "MISSING"),
        })
    items.sort(key=lambda item: (item["path"], item["target_type"] or "", item["color"]))
    return items
