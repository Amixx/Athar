"""Placement and lightweight spatial feature extraction for Phase 1."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass

from .edge_policy import DOMAIN_GEOMETRY, DOMAIN_PLACEMENT, EDGE_INCLUDE
from .types import ClassifiedEdge, ParseResult

_PLACEMENT_SCALE = 1_000_000


@dataclass(frozen=True)
class SpatialFeature:
    placement: tuple[int, ...] | None
    centroid: tuple[float, float, float] | None
    aabb: tuple[float, float, float, float, float, float] | None


def build_spatial_features(parse_result: ParseResult, edges: list[ClassifiedEdge]) -> dict[int, SpatialFeature]:
    """Extract placement, centroid, and AABB for entities."""
    entities = parse_result.entities
    placement_adj = _build_adjacency(edges, domain=DOMAIN_PLACEMENT)
    geometry_adj = _build_adjacency(edges, domain=DOMAIN_GEOMETRY)

    out: dict[int, SpatialFeature] = {}
    transform_cache: dict[int, list[list[float]]] = {}
    for step_id, entity in entities.items():
        if not (entity.is_product or entity.is_spatial):
            continue
        matrix = _placement_matrix_for_entity(step_id, parse_result, placement_adj, transform_cache)
        placement_q = _quantize_matrix(matrix) if matrix is not None else None
        points = _collect_geometry_points(step_id, parse_result, geometry_adj)
        world_points = _transform_points(points, matrix) if matrix is not None else points
        centroid = _compute_centroid(world_points)
        aabb = _compute_aabb(world_points)
        if centroid is None and matrix is not None:
            centroid = (matrix[0][3], matrix[1][3], matrix[2][3])
        if aabb is None and centroid is not None:
            cx, cy, cz = centroid
            aabb = (cx, cy, cz, cx, cy, cz)
        out[step_id] = SpatialFeature(placement=placement_q, centroid=centroid, aabb=aabb)
    return out


def _build_adjacency(edges: list[ClassifiedEdge], *, domain: str) -> dict[int, list[int]]:
    out: dict[int, list[int]] = defaultdict(list)
    for edge in edges:
        if edge.classification != EDGE_INCLUDE:
            continue
        if edge.domain != domain:
            continue
        out[edge.source_step].append(edge.target_step)
    for neighbors in out.values():
        neighbors.sort()
    return out


def _placement_matrix_for_entity(
    step_id: int,
    parse_result: ParseResult,
    placement_adj: dict[int, list[int]],
    cache: dict[int, list[list[float]]],
) -> list[list[float]] | None:
    entities = parse_result.entities
    entity = entities.get(step_id)
    if entity is None:
        return None

    for ref in entity.refs:
        if ref.attr_name == "ObjectPlacement":
            return _resolve_local_placement(ref.target_step, parse_result, placement_adj, cache, seen=set())
    return None


def _resolve_local_placement(
    placement_step: int,
    parse_result: ParseResult,
    placement_adj: dict[int, list[int]],
    cache: dict[int, list[list[float]]],
    *,
    seen: set[int],
) -> list[list[float]] | None:
    if placement_step in cache:
        return cache[placement_step]
    if placement_step in seen:
        return None
    seen.add(placement_step)

    entities = parse_result.entities
    placement = entities.get(placement_step)
    if placement is None:
        return None

    rel_to_step = _first_ref_target(placement, "PlacementRelTo")
    relative_step = _first_ref_target(placement, "RelativePlacement")

    base = _identity_matrix()
    if rel_to_step is not None:
        parent = _resolve_local_placement(rel_to_step, parse_result, placement_adj, cache, seen=seen)
        if parent is not None:
            base = parent

    local = _axis2placement_matrix(relative_step, parse_result) if relative_step is not None else _identity_matrix()
    matrix = _matmul(base, local)
    cache[placement_step] = matrix
    return matrix


def _axis2placement_matrix(step_id: int, parse_result: ParseResult) -> list[list[float]]:
    entities = parse_result.entities
    placement = entities.get(step_id)
    if placement is None:
        return _identity_matrix()

    location = _point_from_ref(_first_ref_target(placement, "Location"), parse_result)
    z_axis = _direction_from_ref(_first_ref_target(placement, "Axis"), parse_result, default=(0.0, 0.0, 1.0))
    x_axis = _direction_from_ref(_first_ref_target(placement, "RefDirection"), parse_result, default=(1.0, 0.0, 0.0))
    z = _normalize(z_axis)
    x = _normalize(_orthogonalize(x_axis, z))
    y = _normalize(_cross(z, x))
    x = _normalize(_cross(y, z))
    tx, ty, tz = location

    return [
        [x[0], y[0], z[0], tx],
        [x[1], y[1], z[1], ty],
        [x[2], y[2], z[2], tz],
        [0.0, 0.0, 0.0, 1.0],
    ]


def _collect_geometry_points(step_id: int, parse_result: ParseResult, geometry_adj: dict[int, list[int]]) -> list[tuple[float, float, float]]:
    entities = parse_result.entities
    queue = deque([step_id])
    seen: set[int] = set()
    points: list[tuple[float, float, float]] = []
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        entity = entities.get(current)
        if entity is None:
            continue
        if entity.entity_type == "IfcCartesianPoint":
            parsed = _coords_from_point_entity(entity)
            if parsed is not None:
                points.append(parsed)
        for nxt in geometry_adj.get(current, []):
            if nxt not in seen:
                queue.append(nxt)
    return points


def _coords_from_point_entity(entity) -> tuple[float, float, float] | None:
    coords = entity.attributes.get("Coordinates")
    if not isinstance(coords, dict):
        return None
    items = coords.get("items")
    if not isinstance(items, list) or not items:
        return None
    raw = [_scalar_float(item) for item in items]
    if any(v is None for v in raw):
        return None
    xyz = [float(v) for v in raw if v is not None]
    while len(xyz) < 3:
        xyz.append(0.0)
    return (xyz[0], xyz[1], xyz[2])


def _point_from_ref(step_id: int | None, parse_result: ParseResult) -> tuple[float, float, float]:
    if step_id is None:
        return (0.0, 0.0, 0.0)
    entity = parse_result.entities.get(step_id)
    if entity is None:
        return (0.0, 0.0, 0.0)
    parsed = _coords_from_point_entity(entity)
    return parsed if parsed is not None else (0.0, 0.0, 0.0)


def _direction_from_ref(
    step_id: int | None,
    parse_result: ParseResult,
    *,
    default: tuple[float, float, float],
) -> tuple[float, float, float]:
    if step_id is None:
        return default
    entity = parse_result.entities.get(step_id)
    if entity is None:
        return default
    ratios = entity.attributes.get("DirectionRatios")
    if not isinstance(ratios, dict):
        return default
    items = ratios.get("items")
    if not isinstance(items, list) or not items:
        return default
    raw = [_scalar_float(item) for item in items]
    if any(v is None for v in raw):
        return default
    xyz = [float(v) for v in raw if v is not None]
    while len(xyz) < 3:
        xyz.append(0.0)
    return (xyz[0], xyz[1], xyz[2])


def _scalar_float(value) -> float | None:
    if not isinstance(value, dict):
        return None
    if value.get("kind") == "int":
        return float(value.get("value"))
    if value.get("kind") == "real_q":
        return float(value.get("value", 0)) / 1_000_000.0
    return None


def _compute_centroid(points: list[tuple[float, float, float]]) -> tuple[float, float, float] | None:
    if not points:
        return None
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    sz = sum(p[2] for p in points)
    n = float(len(points))
    return (sx / n, sy / n, sz / n)


def _compute_aabb(points: list[tuple[float, float, float]]) -> tuple[float, float, float, float, float, float] | None:
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    zs = [p[2] for p in points]
    return (min(xs), min(ys), min(zs), max(xs), max(ys), max(zs))


def _transform_points(
    points: list[tuple[float, float, float]],
    matrix: list[list[float]],
) -> list[tuple[float, float, float]]:
    return [_transform_point(matrix, point) for point in points]


def _transform_point(matrix: list[list[float]], point: tuple[float, float, float]) -> tuple[float, float, float]:
    x, y, z = point
    tx = matrix[0][0] * x + matrix[0][1] * y + matrix[0][2] * z + matrix[0][3]
    ty = matrix[1][0] * x + matrix[1][1] * y + matrix[1][2] * z + matrix[1][3]
    tz = matrix[2][0] * x + matrix[2][1] * y + matrix[2][2] * z + matrix[2][3]
    tw = matrix[3][0] * x + matrix[3][1] * y + matrix[3][2] * z + matrix[3][3]
    if abs(tw) > 1e-12 and abs(tw - 1.0) > 1e-12:
        return (tx / tw, ty / tw, tz / tw)
    return (tx, ty, tz)


def _quantize_matrix(matrix: list[list[float]]) -> tuple[int, ...]:
    q: list[int] = []
    for row in matrix:
        for value in row:
            q.append(int(round(value * _PLACEMENT_SCALE)))
    return tuple(q)


def _first_ref_target(entity, attr_name: str) -> int | None:
    for ref in entity.refs:
        if ref.attr_name == attr_name:
            return ref.target_step
    return None


def _identity_matrix() -> list[list[float]]:
    return [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]


def _matmul(a: list[list[float]], b: list[list[float]]) -> list[list[float]]:
    out = [[0.0] * 4 for _ in range(4)]
    for i in range(4):
        for j in range(4):
            out[i][j] = sum(a[i][k] * b[k][j] for k in range(4))
    return out


def _normalize(v: tuple[float, float, float]) -> tuple[float, float, float]:
    norm = math.sqrt(v[0] ** 2 + v[1] ** 2 + v[2] ** 2)
    if norm <= 1e-12:
        return (0.0, 0.0, 0.0)
    return (v[0] / norm, v[1] / norm, v[2] / norm)


def _cross(a: tuple[float, float, float], b: tuple[float, float, float]) -> tuple[float, float, float]:
    return (
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    )


def _orthogonalize(v: tuple[float, float, float], axis: tuple[float, float, float]) -> tuple[float, float, float]:
    dot = v[0] * axis[0] + v[1] * axis[1] + v[2] * axis[2]
    return (
        v[0] - dot * axis[0],
        v[1] - dot * axis[1],
        v[2] - dot * axis[2],
    )
