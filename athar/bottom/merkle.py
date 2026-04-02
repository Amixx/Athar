"""Unified bottom-up Merkle hashing for Phase 1 signatures."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict

from .edge_policy import DOMAIN_DATA, DOMAIN_GEOMETRY, DOMAIN_PLACEMENT, EDGE_INCLUDE
from .types import ClassifiedEdge, ParseResult, ParsedEntity

_GEOMETRY_ATTR_HINTS = {
    "Representation",
    "Representations",
    "RepresentationMaps",
    "Items",
    "Points",
    "Coordinates",
    "OuterCurve",
    "SweptArea",
    "BasisCurve",
    "Position",
    "MappedRepresentation",
    "MappingSource",
}
_PLACEMENT_ATTRS = {"ObjectPlacement", "PlacementRelTo", "RelativePlacement", "Location", "Axis", "RefDirection"}


def compute_merkle_hashes(
    parse_result: ParseResult,
    edges: list[ClassifiedEdge],
) -> dict[int, dict[str, str]]:
    """Return per-entity domain hashes for geometry/data."""
    entities = parse_result.entities
    adjacency = _build_include_adjacency(edges)
    out: dict[int, dict[str, str]] = {}
    cache: dict[str, dict[int, str]] = {DOMAIN_GEOMETRY: {}, DOMAIN_DATA: {}}
    visiting: dict[str, set[int]] = {DOMAIN_GEOMETRY: set(), DOMAIN_DATA: set()}

    for step_id in sorted(entities):
        geom = _hash_entity(
            step_id=step_id,
            domain=DOMAIN_GEOMETRY,
            entities=entities,
            adjacency=adjacency,
            cache=cache,
            visiting=visiting,
            parse_result=parse_result,
        )
        data = _hash_entity(
            step_id=step_id,
            domain=DOMAIN_DATA,
            entities=entities,
            adjacency=adjacency,
            cache=cache,
            visiting=visiting,
            parse_result=parse_result,
        )
        out[step_id] = {DOMAIN_GEOMETRY: geom, DOMAIN_DATA: data}
    return out


def _build_include_adjacency(edges: list[ClassifiedEdge]) -> dict[str, dict[int, list[tuple[str, int]]]]:
    adjacency: dict[str, dict[int, list[tuple[str, int]]]] = {
        DOMAIN_GEOMETRY: defaultdict(list),
        DOMAIN_DATA: defaultdict(list),
    }
    for edge in edges:
        if edge.classification != EDGE_INCLUDE:
            continue
        if edge.domain not in {DOMAIN_GEOMETRY, DOMAIN_DATA}:
            continue
        adjacency[edge.domain][edge.source_step].append((edge.label, edge.target_step))
    for by_source in adjacency.values():
        for links in by_source.values():
            links.sort(key=lambda item: (item[0], item[1]))
    return adjacency


def _hash_entity(
    *,
    step_id: int,
    domain: str,
    entities: dict[int, ParsedEntity],
    adjacency: dict[str, dict[int, list[tuple[str, int]]]],
    cache: dict[str, dict[int, str]],
    visiting: dict[str, set[int]],
    parse_result: ParseResult,
) -> str:
    cached = cache[domain].get(step_id)
    if cached is not None:
        return cached

    if step_id in visiting[domain]:
        parse_result.diagnostics.cycle_breaks += 1
        parse_result.diagnostics.warnings.append(
            f"Cycle detected in {domain} Merkle pass at step #{step_id}; back-edge ignored."
        )
        cycle_hash = _sha256_hex(f"cycle:{domain}:{step_id}".encode("utf-8"))
        cache[domain][step_id] = cycle_hash
        return cycle_hash

    entity = entities.get(step_id)
    if entity is None:
        missing = _sha256_hex(f"missing:{domain}:{step_id}".encode("utf-8"))
        cache[domain][step_id] = missing
        return missing

    visiting[domain].add(step_id)
    parts: list[str] = [f"class={entity.canonical_class}"]
    parts.extend(_attribute_parts(entity, domain=domain))

    child_entries: list[tuple[str, str]] = []
    for label, child_step in adjacency[domain].get(step_id, []):
        child_hash = _hash_entity(
            step_id=child_step,
            domain=domain,
            entities=entities,
            adjacency=adjacency,
            cache=cache,
            visiting=visiting,
            parse_result=parse_result,
        )
        child_entries.append((label, child_hash))
    child_entries.sort(key=lambda item: (item[0], item[1]))
    for label, child_hash in child_entries:
        parts.append(f"edge={label}:{child_hash}")

    payload = "\x1f".join(parts).encode("utf-8")
    digest = _sha256_hex(payload)
    cache[domain][step_id] = digest
    visiting[domain].remove(step_id)
    return digest


def _attribute_parts(entity: ParsedEntity, *, domain: str) -> list[str]:
    out: list[str] = []
    for attr_name in sorted(entity.attributes):
        if attr_name in {"GlobalId", "OwnerHistory"}:
            continue
        if not _attribute_matches_domain(attr_name, domain=domain):
            continue
        value = entity.attributes[attr_name]
        encoded = _encode_attr_value(value)
        if encoded is None:
            continue
        out.append(f"attr={attr_name}:{encoded}")
    return out


def _attribute_matches_domain(attr_name: str, *, domain: str) -> bool:
    if domain == DOMAIN_GEOMETRY:
        if attr_name in _PLACEMENT_ATTRS:
            return False
        return attr_name in _GEOMETRY_ATTR_HINTS or any(hint in attr_name for hint in ("Coord", "Direction", "Point"))
    if domain == DOMAIN_DATA:
        if attr_name in _PLACEMENT_ATTRS:
            return False
        if attr_name in _GEOMETRY_ATTR_HINTS:
            return False
        if attr_name == DOMAIN_PLACEMENT:
            return False
        return True
    return False


def _encode_attr_value(value) -> str | None:
    if isinstance(value, dict) and value.get("kind") == "ref":
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
