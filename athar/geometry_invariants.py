"""Coarse geometry invariants for representation-form policy checks."""

from __future__ import annotations

from collections import deque
from typing import Any

_GEOM_TOLERANCE = 1e-6
_MAX_VISITED_NODES = 50000


def representation_invariants_match(
    old_ent: dict[str, Any],
    new_ent: dict[str, Any],
    *,
    old_graph: dict[str, Any],
    new_graph: dict[str, Any],
) -> bool:
    old_seeds = _representation_seed_steps(old_ent)
    new_seeds = _representation_seed_steps(new_ent)
    if not old_seeds or not new_seeds:
        return False

    old_inv = _representation_invariants(old_graph, old_seeds)
    new_inv = _representation_invariants(new_graph, new_seeds)
    if old_inv is None or new_inv is None:
        return False
    return _invariants_close(old_inv, new_inv, tolerance=_GEOM_TOLERANCE)


def _representation_seed_steps(entity: dict[str, Any]) -> list[int]:
    seeds: list[int] = []
    for ref in entity.get("refs", []):
        path = str(ref.get("path", ""))
        target = ref.get("target")
        if isinstance(target, int) and path.startswith("/Representation"):
            seeds.append(target)
    return sorted(set(seeds))


def _representation_invariants(
    graph: dict[str, Any],
    seeds: list[int],
) -> dict[str, Any] | None:
    entities = graph.get("entities", {})
    points: list[tuple[float, float, float]] = []
    queue: deque[int] = deque(step for step in seeds if step in entities)
    visited: set[int] = set()

    while queue and len(visited) < _MAX_VISITED_NODES:
        step = queue.popleft()
        if step in visited:
            continue
        visited.add(step)
        entity = entities.get(step)
        if not entity:
            continue
        if entity.get("entity_type") == "IfcCartesianPoint":
            coord = _point_coordinates(entity)
            if coord is not None:
                points.append(coord)
        for ref in entity.get("refs", []):
            target = ref.get("target")
            if isinstance(target, int) and target in entities and target not in visited:
                queue.append(target)

    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    zs = [p[2] for p in points]
    n = float(len(points))
    return {
        "point_count": len(points),
        "bbox_min": (min(xs), min(ys), min(zs)),
        "bbox_max": (max(xs), max(ys), max(zs)),
        "centroid": (sum(xs) / n, sum(ys) / n, sum(zs) / n),
    }


def _point_coordinates(entity: dict[str, Any]) -> tuple[float, float, float] | None:
    coords = (
        entity.get("attributes", {})
        .get("Coordinates", {})
        .get("items", [])
    )
    values: list[float] = []
    for item in coords:
        if not isinstance(item, dict) or item.get("kind") != "real":
            continue
        raw = item.get("value")
        try:
            values.append(float(raw))
        except Exception:
            return None
    if not values:
        return None
    if len(values) == 1:
        return (values[0], 0.0, 0.0)
    if len(values) == 2:
        return (values[0], values[1], 0.0)
    return (values[0], values[1], values[2])


def _invariants_close(old: dict[str, Any], new: dict[str, Any], *, tolerance: float) -> bool:
    if old.get("point_count") != new.get("point_count"):
        return False
    for key in ("bbox_min", "bbox_max", "centroid"):
        old_vec = old.get(key)
        new_vec = new.get(key)
        if not (isinstance(old_vec, tuple) and isinstance(new_vec, tuple) and len(old_vec) == len(new_vec)):
            return False
        for old_v, new_v in zip(old_vec, new_vec):
            if abs(float(old_v) - float(new_v)) > tolerance:
                return False
    return True

