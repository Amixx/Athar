"""Phase 1 edge classification contract."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from .types import ClassifiedEdge, EntityRef, ParseResult, ParsedEntity

EDGE_INCLUDE = "include"
EDGE_CONTEXT = "context"
EDGE_IGNORE = "ignore"

DOMAIN_GEOMETRY = "geometry"
DOMAIN_DATA = "data"
DOMAIN_SPATIAL = "spatial"
DOMAIN_TOPOLOGY = "topology"
DOMAIN_PLACEMENT = "placement"

_PLACEMENT_ATTRS = {"ObjectPlacement", "PlacementRelTo", "RelativePlacement", "Location", "Axis", "RefDirection"}
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

_GEOMETRY_TARGET_PREFIXES = (
    "IfcShape",
    "IfcGeometric",
    "IfcCartesianPoint",
    "IfcPolyline",
    "IfcCurve",
    "IfcProfile",
    "IfcRepresentation",
    "IfcDirection",
    "IfcAxis2Placement",
)

@dataclass(frozen=True)
class RelationshipRule:
    relationship: str
    source_attr: str
    target_attr: str
    classification: str
    domain: str
    bidirectional: bool = False
    dynamic_domain: bool = False


# Machine-readable relationship policy contract for Phase 1.
EDGE_POLICY_TABLE: tuple[RelationshipRule, ...] = (
    RelationshipRule("IfcRelDefinesByProperties", "RelatedObjects", "RelatingPropertyDefinition", EDGE_INCLUDE, DOMAIN_DATA),
    RelationshipRule("IfcRelAssociatesMaterial", "RelatedObjects", "RelatingMaterial", EDGE_INCLUDE, DOMAIN_DATA),
    RelationshipRule("IfcRelContainedInSpatialStructure", "RelatedElements", "RelatingStructure", EDGE_INCLUDE, DOMAIN_SPATIAL),
    RelationshipRule("IfcRelAggregates", "RelatedObjects", "RelatingObject", EDGE_INCLUDE, DOMAIN_GEOMETRY, dynamic_domain=True),
    RelationshipRule("IfcRelVoidsElement", "RelatingBuildingElement", "RelatedOpeningElement", EDGE_INCLUDE, DOMAIN_GEOMETRY),
    RelationshipRule("IfcRelFillsElement", "RelatingOpeningElement", "RelatedBuildingElement", EDGE_CONTEXT, DOMAIN_TOPOLOGY, bidirectional=True),
    RelationshipRule("IfcRelConnectsPathElements", "RelatingElement", "RelatedElement", EDGE_CONTEXT, DOMAIN_TOPOLOGY, bidirectional=True),
    RelationshipRule("IfcRelConnectsElements", "RelatingElement", "RelatedElement", EDGE_CONTEXT, DOMAIN_TOPOLOGY, bidirectional=True),
    RelationshipRule("IfcRelDefinesByType", "RelatedObjects", "RelatingType", EDGE_CONTEXT, DOMAIN_TOPOLOGY),
)
_RULES_BY_REL: dict[str, tuple[RelationshipRule, ...]] = defaultdict(tuple)
for _rule in EDGE_POLICY_TABLE:
    _RULES_BY_REL[_rule.relationship] = (*_RULES_BY_REL[_rule.relationship], _rule)


def build_edge_set(parse_result: ParseResult) -> list[ClassifiedEdge]:
    """Classify explicit and relationship-projected edges."""
    entities = parse_result.entities
    edges: list[ClassifiedEdge] = []

    for entity in entities.values():
        edges.extend(_classify_direct_refs(entity))

    for rel in entities.values():
        if not rel.entity_type.startswith("IfcRel"):
            continue
        edges.extend(_project_relationship_edges(rel, entities))

    unique: dict[tuple[int, int, str, str, str], ClassifiedEdge] = {}
    for edge in edges:
        key = (edge.source_step, edge.target_step, edge.classification, edge.domain, edge.label)
        unique[key] = edge
    out = list(unique.values())
    out.sort(key=lambda e: (e.source_step, e.target_step, e.classification, e.domain, e.label))
    return out


def edge_stats(edges: list[ClassifiedEdge]) -> dict[str, int]:
    counts = defaultdict(int)
    for edge in edges:
        counts[f"{edge.classification}:{edge.domain}"] += 1
    return dict(sorted(counts.items()))


def _classify_direct_refs(entity: ParsedEntity) -> list[ClassifiedEdge]:
    out: list[ClassifiedEdge] = []
    if entity.entity_type.startswith("IfcRel"):
        return out
    for ref in entity.refs:
        classification, domain = _classify_direct_ref(ref)
        if classification == EDGE_IGNORE:
            continue
        out.append(
            ClassifiedEdge(
                source_step=ref.source_step,
                target_step=ref.target_step,
                classification=classification,
                domain=domain,
                label=f"{ref.source_type}.{ref.attr_name}",
            )
        )
    return out


def _classify_direct_ref(ref: EntityRef) -> tuple[str, str]:
    if ref.attr_name in _PLACEMENT_ATTRS:
        return EDGE_INCLUDE, DOMAIN_PLACEMENT
    if ref.attr_name in _GEOMETRY_ATTR_HINTS:
        return EDGE_INCLUDE, DOMAIN_GEOMETRY
    if ref.target_type.startswith(_GEOMETRY_TARGET_PREFIXES):
        return EDGE_INCLUDE, DOMAIN_GEOMETRY
    return EDGE_IGNORE, DOMAIN_DATA


def _project_relationship_edges(rel: ParsedEntity, entities: dict[int, ParsedEntity]) -> list[ClassifiedEdge]:
    rel_type = rel.entity_type
    by_attr = _refs_by_attr(rel)

    rules = _RULES_BY_REL.get(rel_type, ())
    if rules:
        out: list[ClassifiedEdge] = []
        for rule in rules:
            sources = by_attr.get(rule.source_attr, [])
            targets = by_attr.get(rule.target_attr, [])
            if rule.dynamic_domain:
                out.extend(
                    _cross_edges_dynamic_domain(
                        sources=sources,
                        targets=targets,
                        entities=entities,
                        classification=rule.classification,
                        default_domain=rule.domain,
                        label=rel_type,
                    )
                )
                continue
            if rule.bidirectional:
                out.extend(
                    _undirected_cross_edges(
                        left=sources,
                        right=targets,
                        classification=rule.classification,
                        domain=rule.domain,
                        label=rel_type,
                    )
                )
            else:
                out.extend(
                    _cross_edges(
                        sources=sources,
                        targets=targets,
                        classification=rule.classification,
                        domain=rule.domain,
                        label=rel_type,
                    )
                )
        return out

    if rel_type == "IfcRelSpaceBoundary":
        return []
    if rel_type == "IfcRelAssociatesDocument":
        return []
    if rel_type.startswith("IfcRelAssigns"):
        return []
    return []


def _refs_by_attr(rel: ParsedEntity) -> dict[str, list[EntityRef]]:
    out: dict[str, list[EntityRef]] = defaultdict(list)
    for ref in rel.refs:
        out[ref.attr_name].append(ref)
    return out


def _cross_edges(
    sources: list[EntityRef],
    targets: list[EntityRef],
    classification: str,
    domain: str,
    label: str,
) -> list[ClassifiedEdge]:
    out: list[ClassifiedEdge] = []
    for source in sources:
        for target in targets:
            out.append(
                ClassifiedEdge(
                    source_step=source.target_step,
                    target_step=target.target_step,
                    classification=classification,
                    domain=domain,
                    label=label,
                )
            )
    return out


def _cross_edges_dynamic_domain(
    *,
    sources: list[EntityRef],
    targets: list[EntityRef],
    entities: dict[int, ParsedEntity],
    classification: str,
    default_domain: str,
    label: str,
) -> list[ClassifiedEdge]:
    out: list[ClassifiedEdge] = []
    for source in sources:
        for target in targets:
            domain = default_domain
            target_entity = entities.get(target.target_step)
            if target_entity and target_entity.entity_type in {
                "IfcProject",
                "IfcSite",
                "IfcBuilding",
                "IfcBuildingStorey",
                "IfcSpace",
            }:
                domain = DOMAIN_SPATIAL
            out.append(
                ClassifiedEdge(
                    source_step=source.target_step,
                    target_step=target.target_step,
                    classification=classification,
                    domain=domain,
                    label=label,
                )
            )
    return out


def _undirected_cross_edges(
    left: list[EntityRef],
    right: list[EntityRef],
    classification: str,
    domain: str,
    label: str,
) -> list[ClassifiedEdge]:
    out = _cross_edges(left, right, classification, domain, label)
    out.extend(_cross_edges(right, left, classification, domain, label))
    return out
