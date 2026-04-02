"""Simplified WL-style topology gossip for Phase 1."""

from __future__ import annotations

import hashlib
from collections import defaultdict, deque

from .edge_policy import DOMAIN_SPATIAL, DOMAIN_TOPOLOGY, EDGE_CONTEXT, EDGE_INCLUDE
from .types import ClassifiedEdge, ParseResult


def compute_topology_hashes(
    parse_result: ParseResult,
    edges: list[ClassifiedEdge],
    merkle_hashes: dict[int, dict[str, str]],
    *,
    context_k: int = 1,
    spatial_k: int = 2,
) -> dict[int, str]:
    """Compute VH_Topology from context + spatial neighborhood gossip."""
    entities = parse_result.entities
    context_adj, spatial_adj = _build_adjacency(edges)

    seeds: dict[int, str] = {}
    for step_id, entity in entities.items():
        vh_geometry = merkle_hashes.get(step_id, {}).get("geometry", "")
        vh_data = merkle_hashes.get(step_id, {}).get("data", "")
        seeds[step_id] = _sha256(f"{entity.canonical_class}|{vh_geometry}|{vh_data}")

    out: dict[int, str] = {}
    for step_id in sorted(entities):
        context_neighbors = _neighbors_within_k(context_adj, step_id, context_k)
        spatial_neighbors = _neighbors_within_k(spatial_adj, step_id, spatial_k)
        tokens: list[str] = []
        for neigh in context_neighbors:
            if neigh == step_id:
                continue
            tokens.append(f"context:{seeds.get(neigh, '')}")
        for neigh in spatial_neighbors:
            if neigh == step_id:
                continue
            tokens.append(f"spatial:{seeds.get(neigh, '')}")
        tokens.sort()
        payload = "\x1f".join([f"self:{seeds[step_id]}", *tokens])
        out[step_id] = _sha256(payload)
    return out


def _build_adjacency(edges: list[ClassifiedEdge]) -> tuple[dict[int, set[int]], dict[int, set[int]]]:
    context_adj: dict[int, set[int]] = defaultdict(set)
    spatial_adj: dict[int, set[int]] = defaultdict(set)

    for edge in edges:
        if edge.classification == EDGE_CONTEXT and edge.domain == DOMAIN_TOPOLOGY:
            context_adj[edge.source_step].add(edge.target_step)
            context_adj[edge.target_step].add(edge.source_step)
        if edge.classification == EDGE_INCLUDE and edge.domain == DOMAIN_SPATIAL:
            spatial_adj[edge.source_step].add(edge.target_step)
            spatial_adj[edge.target_step].add(edge.source_step)

    return context_adj, spatial_adj


def _neighbors_within_k(adjacency: dict[int, set[int]], start: int, depth: int) -> set[int]:
    if depth <= 0:
        return {start}
    seen = {start}
    queue = deque([(start, 0)])
    while queue:
        node, d = queue.popleft()
        if d >= depth:
            continue
        for nxt in adjacency.get(node, set()):
            if nxt in seen:
                continue
            seen.add(nxt)
            queue.append((nxt, d + 1))
    return seen


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
