"""WL refinement and SCC-aware ambiguity fallback for graph identity."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import time
from collections import Counter
from typing import Any, Callable

from .graph_utils import build_adjacency, build_reverse_adjacency
from .structural_hash import structural_hash

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
_SCC_AMBIGUOUS_PARTITION_MAX = 128
_SCC_FALLBACK_REFINEMENT_ROUNDS = 4
_WL_PARTITION_STAGNANT_ROUNDS = 2
_WL_MAX_ROUNDS_ENV = "ATHAR_WL_MAX_ROUNDS"
_WL_ADAPTIVE_ROUND_CAPS = (
    (1_000_000, 3),
    (500_000, 4),
    (250_000, 5),
)


def wl_refine_colors(
    graph: dict,
    *,
    max_rounds: int | None = None,
    round_hash: str = _WL_ROUND_HASH_AUTO,
    initial_colors: dict[int, str] | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> dict[int, str]:
    """Weisfeiler-Lehman color refinement over the explicit forward graph."""
    entities = graph.get("entities", {})
    if not entities:
        return {}

    hasher_name, hasher = _resolve_wl_round_hasher(round_hash)
    adjacency = build_adjacency(entities)
    if initial_colors is None:
        colors = {step_id: structural_hash(entity) for step_id, entity in entities.items()}
    else:
        # Reuse precomputed structural hashes where available to avoid duplicate
        # per-entity hashing across identity precompute + WL initialization.
        colors = {step_id: initial_colors.get(step_id) or structural_hash(entity) for step_id, entity in entities.items()}
    path_bytes_cache: dict[str, bytes] = {}
    type_bytes_cache: dict[str, bytes] = {}
    color_bytes_cache: dict[str, bytes] = {}

    rounds = _resolve_wl_rounds(max_rounds=max_rounds, entity_count=len(entities))
    previous_class_count = len(set(colors.values()))
    stagnant_rounds = 0
    round_stats: list[dict[str, Any]] = []
    stop_reason = "max_rounds"

    for round_idx in range(1, rounds + 1):
        round_started = time.perf_counter()
        next_colors: dict[int, str] = {}
        changed = 0
        for step_id, _entity in entities.items():
            blob = _wl_round_payload(
                colors[step_id],
                adjacency.get(step_id, []),
                colors,
                path_bytes_cache=path_bytes_cache,
                type_bytes_cache=type_bytes_cache,
                color_bytes_cache=color_bytes_cache,
            )
            digest = hasher(blob)
            # Keep backend-native WL colors inside refinement rounds to avoid
            # per-round sha256 wrapping overhead for fast hash backends.
            next_color = digest
            next_colors[step_id] = next_color
            if next_color != colors[step_id]:
                changed += 1
        class_count = len(set(next_colors.values()))
        if class_count == previous_class_count:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        round_stats.append({
            "round": round_idx,
            "changed": changed,
            "class_count": class_count,
            "elapsed_ms": round((time.perf_counter() - round_started) * 1000.0, 3),
        })
        colors = next_colors
        previous_class_count = class_count
        if changed == 0:
            stop_reason = "no_color_change"
            break
        if stagnant_rounds >= _WL_PARTITION_STAGNANT_ROUNDS:
            stop_reason = "partition_stable"
            break

    # External IDs (H:/C:) stay sha256-derived for stable wire-level identity.
    if hasher_name != _WL_ROUND_HASH_SHA256:
        colors = _normalize_colors_to_sha256(colors)

    if diagnostics is not None:
        diagnostics.update({
            "backend": hasher_name,
            "external_color_backend": _WL_ROUND_HASH_SHA256,
            "configured_rounds": rounds,
            "executed_rounds": len(round_stats),
            "stop_reason": stop_reason,
            "entity_count": len(entities),
            "rounds": round_stats,
            "total_ms": round(sum(item["elapsed_ms"] for item in round_stats), 3),
        })
    return colors


def _normalize_colors_to_sha256(colors: dict[int, str]) -> dict[int, str]:
    normalized: dict[int, str] = {}
    cache: dict[str, str] = {}
    for step_id, color in colors.items():
        hashed = cache.get(color)
        if hashed is None:
            hashed = _sha256_hexdigest(color.encode("ascii"))
            cache[color] = hashed
        normalized[step_id] = hashed
    return normalized


def _wl_round_payload(
    self_color: str,
    edges: list[tuple[str, str | None, int]],
    colors: dict[int, str],
    *,
    path_bytes_cache: dict[str, bytes],
    type_bytes_cache: dict[str, bytes],
    color_bytes_cache: dict[str, bytes],
) -> bytes:
    payload = bytearray()
    payload.extend(_cached_bytes(self_color, color_bytes_cache))
    if not edges:
        return bytes(payload)

    neighbor_items: list[tuple[str, str, str]] = []
    append_item = neighbor_items.append
    for path, target_type, target_id in edges:
        append_item((path, target_type or "", colors.get(target_id, "MISSING")))
    neighbor_items.sort(key=lambda item: (item[0], item[1], item[2]))

    prev = neighbor_items[0]
    count = 1
    for item in neighbor_items[1:]:
        if item == prev:
            count += 1
            continue
        _append_neighbor_token(
            payload,
            prev,
            count,
            path_bytes_cache=path_bytes_cache,
            type_bytes_cache=type_bytes_cache,
            color_bytes_cache=color_bytes_cache,
        )
        prev = item
        count = 1
    _append_neighbor_token(
        payload,
        prev,
        count,
        path_bytes_cache=path_bytes_cache,
        type_bytes_cache=type_bytes_cache,
        color_bytes_cache=color_bytes_cache,
    )
    return bytes(payload)


def _append_neighbor_token(
    payload: bytearray,
    item: tuple[str, str, str],
    count: int,
    *,
    path_bytes_cache: dict[str, bytes],
    type_bytes_cache: dict[str, bytes],
    color_bytes_cache: dict[str, bytes],
) -> None:
    path, target_type, color = item
    payload.extend(b"\x1e")
    payload.extend(_cached_bytes(path, path_bytes_cache))
    payload.extend(b"\x1f")
    payload.extend(_cached_bytes(target_type, type_bytes_cache))
    payload.extend(b"\x1f")
    payload.extend(_cached_bytes(color, color_bytes_cache))
    payload.extend(b"\x1f")
    payload.extend(str(count).encode("ascii"))


def _cached_bytes(value: str, cache: dict[str, bytes]) -> bytes:
    encoded = cache.get(value)
    if encoded is not None:
        return encoded
    encoded = value.encode("utf-8")
    cache[value] = encoded
    return encoded


def wl_refine_with_scc_fallback(
    graph: dict,
    *,
    max_rounds: int | None = None,
    round_hash: str = _WL_ROUND_HASH_AUTO,
    initial_colors: dict[int, str] | None = None,
    max_partition_size: int = _SCC_AMBIGUOUS_PARTITION_MAX,
    refinement_rounds: int = _SCC_FALLBACK_REFINEMENT_ROUNDS,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[int, str], dict[int, str]]:
    """Run WL refinement and emit deterministic C: IDs for unresolved SCC ambiguity."""
    if max_partition_size <= 0:
        raise ValueError("max_partition_size must be > 0")
    if refinement_rounds < 0:
        raise ValueError("refinement_rounds must be >= 0")

    entities = graph.get("entities", {})
    if not entities:
        return {}, {}

    wl_diagnostics: dict[str, Any] = {}
    colors = wl_refine_colors(
        graph,
        max_rounds=max_rounds,
        round_hash=round_hash,
        initial_colors=initial_colors,
        diagnostics=wl_diagnostics,
    )
    adjacency = build_adjacency(entities)
    reverse_adjacency = build_reverse_adjacency(entities, adjacency)
    class_ids: dict[int, str] = {}

    sccs = _tarjan_scc(sorted(entities), adjacency)
    for scc in sccs:
        if len(scc) <= 1:
            continue
        by_color: dict[str, list[int]] = {}
        for step_id in scc:
            by_color.setdefault(colors.get(step_id, "MISSING"), []).append(step_id)
        for color in sorted(by_color):
            steps = sorted(by_color[color])
            if len(steps) <= 1:
                continue
            unresolved_groups = _bounded_partition_refinement(
                steps,
                entities=entities,
                adjacency=adjacency,
                reverse_adjacency=reverse_adjacency,
                colors=colors,
                max_partition_size=max_partition_size,
                refinement_rounds=refinement_rounds,
            )
            for group in unresolved_groups:
                if len(group) <= 1:
                    continue
                class_id = _ambiguous_class_id(group, entities=entities, colors=colors)
                for step_id in group:
                    class_ids[step_id] = class_id

    if diagnostics is not None:
        diagnostics.update({
            "wl": wl_diagnostics,
            "scc_count": len(sccs),
            "ambiguous_class_count": len(set(class_ids.values())),
            "ambiguous_entity_count": len(class_ids),
        })
    return colors, class_ids


def _resolve_wl_rounds(*, max_rounds: int | None, entity_count: int) -> int:
    if max_rounds is not None:
        if max_rounds < 0:
            raise ValueError("max_rounds must be >= 0")
        return max_rounds
    env_value = os.getenv(_WL_MAX_ROUNDS_ENV)
    if env_value is not None and env_value.strip() != "":
        try:
            parsed = int(env_value)
        except ValueError as exc:
            raise ValueError(f"{_WL_MAX_ROUNDS_ENV} must be an integer >= 0") from exc
        if parsed < 0:
            raise ValueError(f"{_WL_MAX_ROUNDS_ENV} must be >= 0")
        return parsed
    for threshold, cap in _WL_ADAPTIVE_ROUND_CAPS:
        if entity_count >= threshold:
            return cap
    return _DEFAULT_WL_ROUNDS


def _resolve_wl_round_hasher(name: str) -> tuple[str, Callable[[bytes], str]]:
    if name not in _WL_ROUND_HASH_CHOICES:
        raise ValueError(f"Unknown WL round hash: {name!r}")

    if name == _WL_ROUND_HASH_AUTO:
        for candidate in (_WL_ROUND_HASH_XXH3, _WL_ROUND_HASH_BLAKE3, _WL_ROUND_HASH_BLAKE2B64):
            hasher = _resolve_optional_hasher(candidate)
            if hasher is not None:
                return candidate, hasher
        return _WL_ROUND_HASH_SHA256, _sha256_hexdigest

    if name == _WL_ROUND_HASH_SHA256:
        return _WL_ROUND_HASH_SHA256, _sha256_hexdigest

    hasher = _resolve_optional_hasher(name)
    if hasher is None:
        raise ValueError(f"WL round hash backend unavailable: {name!r}")
    return name, hasher


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


def _tarjan_scc(
    nodes: list[int],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
) -> list[list[int]]:
    node_set = set(nodes)
    index = 0
    stack: list[int] = []
    on_stack: set[int] = set()
    indices: dict[int, int] = {}
    lowlink: dict[int, int] = {}
    sccs: list[list[int]] = []

    def strongconnect(v: int) -> None:
        nonlocal index
        indices[v] = index
        lowlink[v] = index
        index += 1
        stack.append(v)
        on_stack.add(v)

        for _path, _target_type, w in adjacency.get(v, []):
            if w not in node_set:
                continue
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            component: list[int] = []
            while True:
                w = stack.pop()
                on_stack.remove(w)
                component.append(w)
                if w == v:
                    break
            component.sort()
            sccs.append(component)

    for node in nodes:
        if node not in indices:
            strongconnect(node)

    sccs.sort(key=lambda comp: (len(comp), comp))
    return sccs


def _bounded_partition_refinement(
    steps: list[int],
    *,
    entities: dict[int, dict],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
    reverse_adjacency: dict[int, list[tuple[str, str | None, int]]],
    colors: dict[int, str],
    max_partition_size: int,
    refinement_rounds: int,
) -> list[list[int]]:
    ordered_steps = sorted(steps)
    if len(ordered_steps) > max_partition_size:
        return [ordered_steps]

    partitions = [ordered_steps]
    for _ in range(refinement_rounds):
        changed = False
        next_partitions: list[list[int]] = []
        for part in partitions:
            if len(part) <= 1:
                next_partitions.append(part)
                continue
            buckets: dict[str, list[int]] = {}
            for step_id in part:
                sig = _partition_local_signature(
                    step_id,
                    entities=entities,
                    adjacency=adjacency,
                    reverse_adjacency=reverse_adjacency,
                    colors=colors,
                )
                buckets.setdefault(sig, []).append(step_id)
            if len(buckets) == 1:
                next_partitions.append(part)
                continue
            changed = True
            for sig in sorted(buckets):
                next_partitions.append(sorted(buckets[sig]))
        partitions = next_partitions
        if not changed:
            break

    return [part for part in partitions if len(part) > 1]


def _partition_local_signature(
    step_id: int,
    *,
    entities: dict[int, dict],
    adjacency: dict[int, list[tuple[str, str | None, int]]],
    reverse_adjacency: dict[int, list[tuple[str, str | None, int]]],
    colors: dict[int, str],
) -> str:
    out_counts: Counter[tuple[str, str | None, str]] = Counter()
    for path, target_type, target_step in adjacency.get(step_id, []):
        out_counts[(path, target_type, colors.get(target_step, "MISSING"))] += 1
    in_counts: Counter[tuple[str, str | None, str]] = Counter()
    for path, source_type, source_step in reverse_adjacency.get(step_id, []):
        in_counts[(path, source_type, colors.get(source_step, "MISSING"))] += 1

    payload = {
        "entity_type": entities.get(step_id, {}).get("entity_type"),
        "out": [
            {"path": path, "type": edge_type, "color": color, "count": count}
            for (path, edge_type, color), count in sorted(
                out_counts.items(),
                key=lambda item: (
                    item[0][0],
                    item[0][1] or "",
                    item[0][2],
                    item[1],
                ),
            )
        ],
        "in": [
            {"path": path, "type": edge_type, "color": color, "count": count}
            for (path, edge_type, color), count in sorted(
                in_counts.items(),
                key=lambda item: (
                    item[0][0],
                    item[0][1] or "",
                    item[0][2],
                    item[1],
                ),
            )
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _ambiguous_class_id(
    steps: list[int],
    *,
    entities: dict[int, dict],
    colors: dict[int, str],
) -> str:
    entity_types = sorted({
        str(entities.get(step_id, {}).get("entity_type") or "")
        for step_id in steps
    })
    color_set = sorted({
        colors.get(step_id, "MISSING")
        for step_id in steps
    })
    edge_labels = sorted({
        (ref.get("path", ""), ref.get("target_type"))
        for step_id in steps
        for ref in entities.get(step_id, {}).get("refs", [])
    }, key=lambda item: (item[0], item[1] or ""))
    payload = {
        "entity_types": entity_types,
        "color_set": color_set,
        "edge_labels": [
            {"path": path, "target_type": target_type}
            for path, target_type in edge_labels
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"C:{hashlib.sha256(blob.encode('utf-8')).hexdigest()}"
