"""Core data contracts for the Phase 1 bottom layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HashHex = str


@dataclass(frozen=True)
class EntityRef:
    """A typed reference discovered while parsing explicit IFC attributes."""

    source_step: int
    target_step: int
    source_type: str
    target_type: str
    attr_name: str
    path: str


@dataclass
class ParsedEntity:
    """Schema-aware parsed IFC instance with canonicalized attributes."""

    step_id: int
    entity_type: str
    canonical_class: str
    global_id: str | None
    attributes: dict[str, Any]
    refs: list[EntityRef]
    is_product: bool
    is_spatial: bool


@dataclass
class ParseDiagnostics:
    """Parse-time diagnostic counters."""

    dangling_refs: int = 0
    cycle_breaks: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass
class ParseResult:
    """Output of index + parser + link inversion stages."""

    filepath: str
    schema: str
    index: dict[int, int]
    entities: dict[int, ParsedEntity]
    incoming_refs: dict[int, list[EntityRef]]
    unit_context: dict[str, Any]
    diagnostics: ParseDiagnostics


@dataclass(frozen=True)
class ClassifiedEdge:
    """Semantic edge used by Merkle and WL stages."""

    source_step: int
    target_step: int
    classification: str
    domain: str
    label: str


@dataclass
class SignatureVector:
    """Phase 1 product-level signature vector."""

    step_id: int
    guid: str | None
    entity_type: str
    canonical_class: str
    vh_geometry: HashHex
    vh_data: HashHex
    vh_topology: HashHex
    placement: tuple[int, ...] | None
    centroid: tuple[float, float, float] | None
    aabb: tuple[float, float, float, float, float, float] | None
    canon_version: str


@dataclass
class SignatureBundle:
    """Bottom-layer output for a single IFC file."""

    filepath: str
    schema: str
    canon_version: str
    signatures: dict[int, SignatureVector]
    diagnostics: ParseDiagnostics
    edge_stats: dict[str, int]

