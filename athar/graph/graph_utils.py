"""Shared graph primitives used across identity, matching, and diff modules.

Adjacency builders, value strippers, feature collectors, and scoring helpers
that form the stable substrate of the graph engine.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any


def sha256_json(payload: Any) -> str:
    """Deterministic SHA-256 hex digest over JSON-serialized payload."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def strip_ref_ids(value: Any) -> Any:
    """Replace ref targets with kind-only stubs for GUID/STEP-free comparison."""
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            return {"kind": "ref"}
        return {k: strip_ref_ids(v) for k, v in value.items()}
    if isinstance(value, list):
        return [strip_ref_ids(v) for v in value]
    return value


def edge_signature(
    refs: list[dict],
    *,
    skip_paths: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    """Deterministic sorted edge signature from a ref list.

    When *skip_paths* is given, refs whose ``path`` is in the set are ignored
    (used by root_remap to filter volatile refs like ``/OwnerHistory``).
    """
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        path = ref.get("path", "")
        if skip_paths and path in skip_paths:
            continue
        counts[(path, ref.get("target_type"))] += 1
    edges = [
        {"path": path, "target_type": target_type, "count": count}
        for (path, target_type), count in counts.items()
    ]
    edges.sort(key=lambda item: (item["path"], item["target_type"] or "", item["count"]))
    return edges


def build_adjacency(
    entities: dict[int, dict],
) -> dict[int, list[tuple[str, str | None, int]]]:
    """Forward adjacency: step_id -> [(path, target_type, target_step)]."""
    return _build_adjacency_python(entities)


def build_adjacency_maps(
    entities: dict[int, dict],
) -> tuple[
    dict[int, list[tuple[str, str | None, int]]],
    dict[int, list[tuple[str, str | None, int]]],
]:
    """Build forward + reverse adjacency with the shared graph reference builders."""
    adjacency = _build_adjacency_python(entities)
    return adjacency, _build_reverse_adjacency_python(entities, adjacency)


def _build_adjacency_python(
    entities: dict[int, dict],
) -> dict[int, list[tuple[str, str | None, int]]]:
    """Pure-Python forward adjacency builder."""
    adjacency: dict[int, list[tuple[str, str | None, int]]] = {}
    for step_id, entity in entities.items():
        edges: list[tuple[str, str | None, int]] = []
        for ref in entity.get("refs", []):
            target = ref.get("target")
            if target in entities:
                edges.append((ref.get("path", ""), ref.get("target_type"), target))
        edges.sort(key=lambda item: (item[0], item[1] or "", item[2]))
        adjacency[step_id] = edges
    return adjacency


def build_reverse_adjacency(
    entities: dict[int, dict],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
) -> dict[int, list[tuple[str, str | None, int]]]:
    """Reverse adjacency: target_step -> [(path, source_type, source_step)]."""
    return _build_reverse_adjacency_python(entities, adjacency)


def _build_reverse_adjacency_python(
    entities: dict[int, dict],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
) -> dict[int, list[tuple[str, str | None, int]]]:
    """Pure-Python reverse adjacency builder."""
    reverse: dict[int, list[tuple[str, str | None, int]]] = {step_id: [] for step_id in entities}
    for source_step, edges in adjacency.items():
        source_type = entities.get(source_step, {}).get("entity_type")
        for path, _target_type, target_step in edges:
            if target_step not in entities:
                continue
            reverse.setdefault(target_step, []).append((path, source_type, source_step))
    for step_id in reverse:
        reverse[step_id].sort(key=lambda item: (item[0], item[1] or "", item[2]))
    return reverse


def collect_attribute_paths(value: Any, *, base_path: str) -> set[str]:
    """Recursively collect all attribute paths from a canonical value tree."""
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{base_path}/{key}"
            paths.add(child_path)
            paths.update(collect_attribute_paths(child, base_path=child_path))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            child_path = f"{base_path}/{idx}"
            paths.add(child_path)
            paths.update(collect_attribute_paths(child, base_path=child_path))
    return paths


def collect_literal_tokens(value: Any) -> set[str]:
    """Recursively collect literal value tokens from a canonical value tree."""
    tokens: set[str] = set()
    if isinstance(value, dict):
        kind = value.get("kind")
        if kind in {"bool", "int", "real", "string"}:
            tokens.add(f"{kind}:{value.get('value')}")
        for child in value.values():
            tokens.update(collect_literal_tokens(child))
    elif isinstance(value, list):
        for child in value:
            tokens.update(collect_literal_tokens(child))
    return tokens


def jaccard(left: set[Any], right: set[Any]) -> float:
    """Jaccard similarity coefficient."""
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def degree_similarity(old_degree: int, new_degree: int) -> float:
    """Normalized degree similarity in [0, 1]."""
    max_degree = max(1, old_degree, new_degree)
    return 1.0 - (abs(old_degree - new_degree) / max_degree)
