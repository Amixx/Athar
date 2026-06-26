"""Core-engine signature assembly pipeline."""

from __future__ import annotations

from collections import Counter, defaultdict

from .constants import CANON_VERSION
from .edge_policy import DOMAIN_DATA, DOMAIN_GEOMETRY, EDGE_INCLUDE, edge_stats, build_edge_set
from .merkle import _attribute_matches_domain, _encode_attr_value
from .merkle import compute_merkle_hashes
from .parser import parse_ifc
from .spatial import build_spatial_features
from .types import ClassifiedEdge, ParsedEntity, SignatureBundle, SignatureVector
from .wl_gossip import compute_topology_hashes


def build_signature_bundle(filepath: str) -> SignatureBundle:
    """Run bottom-layer extraction and return first-class signatures."""
    parsed = parse_ifc(filepath)
    edges = build_edge_set(parsed)
    merkle_hashes = compute_merkle_hashes(parsed, edges)
    topology_hashes = compute_topology_hashes(parsed, edges, merkle_hashes)
    spatial_features = build_spatial_features(parsed, edges)
    data_facts = build_data_facts(parsed.entities, edges)

    signatures: dict[int, SignatureVector] = {}
    for step_id, entity in sorted(parsed.entities.items()):
        if not (entity.is_product or entity.is_spatial):
            continue
        spatial = spatial_features.get(step_id)
        signatures[step_id] = SignatureVector(
            step_id=step_id,
            guid=entity.global_id,
            name=entity.name,
            entity_type=entity.entity_type,
            canonical_class=entity.canonical_class,
            vh_geometry=merkle_hashes.get(step_id, {}).get(DOMAIN_GEOMETRY, ""),
            vh_data=merkle_hashes.get(step_id, {}).get(DOMAIN_DATA, ""),
            vh_topology=topology_hashes.get(step_id, ""),
            placement=spatial.placement if spatial else None,
            centroid=spatial.centroid if spatial else None,
            aabb=spatial.aabb if spatial else None,
            canon_version=CANON_VERSION,
            data_facts=data_facts.get(step_id, ()),
        )

    return SignatureBundle(
        filepath=filepath,
        schema=parsed.schema,
        canon_version=CANON_VERSION,
        signatures=signatures,
        diagnostics=parsed.diagnostics,
        edge_stats=edge_stats(edges),
        property_index=parsed.property_index,
    )


def build_data_facts(
    entities: dict[int, ParsedEntity],
    edges: list[ClassifiedEdge],
) -> dict[int, tuple[tuple[str, str], ...]]:
    """Return compact, report-facing data facts for each signature root.

    The data Merkle hash intentionally remains the source of truth for change
    detection. These facts are a small explanatory side channel: scalar data
    attributes in the root's data include-closure, keyed without STEP ids so
    reports can say which value changed instead of only showing hash churn.
    """

    data_children: dict[int, list[int]] = defaultdict(list)
    for edge in edges:
        if edge.classification == EDGE_INCLUDE and edge.domain == DOMAIN_DATA:
            data_children[edge.source_step].append(edge.target_step)
    for children in data_children.values():
        children.sort(key=lambda step: (entities.get(step).canonical_class if entities.get(step) else "", step))

    out: dict[int, tuple[tuple[str, str], ...]] = {}
    for step_id, entity in sorted(entities.items()):
        if not (entity.is_product or entity.is_spatial):
            continue
        facts: list[tuple[str, str]] = []
        for current_step in _walk_data_closure(step_id, data_children):
            current = entities.get(current_step)
            if current is None:
                continue
            facts.extend(_entity_data_facts(current))
        out[step_id] = _dedupe_fact_paths(facts)
    return out


def _walk_data_closure(root_step: int, data_children: dict[int, list[int]]) -> list[int]:
    seen: set[int] = set()
    ordered: list[int] = []

    def visit(step_id: int) -> None:
        if step_id in seen:
            return
        seen.add(step_id)
        ordered.append(step_id)
        for child_step in data_children.get(step_id, []):
            visit(child_step)

    visit(root_step)
    return ordered


def _entity_data_facts(entity: ParsedEntity) -> list[tuple[str, str]]:
    prefix = _entity_fact_label(entity)
    out: list[tuple[str, str]] = []
    for attr_name in sorted(entity.attributes):
        if attr_name in {"GlobalId", "OwnerHistory"}:
            continue
        if not _attribute_matches_domain(attr_name, domain=DOMAIN_DATA):
            continue
        value = entity.attributes[attr_name]
        if _encode_attr_value(value) is None:
            continue
        out.append((f"{prefix}.{attr_name}", _human_value(value)))
    return out


def _entity_fact_label(entity: ParsedEntity) -> str:
    if entity.global_id:
        return f"{entity.canonical_class}[GlobalId={entity.global_id}]"
    return entity.canonical_class


def _human_value(value) -> str:
    if isinstance(value, dict):
        kind = value.get("kind")
        if kind == "null":
            return "<null>"
        if kind == "derived":
            return "<derived>"
        if kind == "string":
            return _human_value(value.get("value"))
        if kind == "simple":
            return f"{value.get('type')}: {_human_value(value.get('value'))}"
        if kind == "select":
            branch = value.get("type") or "select"
            return f"{branch}: {_human_value(value.get('value'))}"
        if kind in {"list", "array", "set", "bag"}:
            return "[" + ", ".join(_human_value(item) for item in value.get("items", ())) + "]"
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "<null>"
    return str(value)


def _dedupe_fact_paths(facts: list[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    facts.sort(key=lambda item: (item[0], item[1]))
    counts = Counter(path for path, _ in facts)
    seen: Counter[str] = Counter()
    out: list[tuple[str, str]] = []
    for path, value in facts:
        if counts[path] == 1:
            out.append((path, value))
            continue
        seen[path] += 1
        out.append((f"{path}[{seen[path]}]", value))
    return tuple(out)
