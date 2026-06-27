"""Core data contracts for the bottom layer."""

from __future__ import annotations

from dataclasses import dataclass, field


HashHex = str


@dataclass(frozen=True)
class PropertyEntry:
    """One extracted property or quantity value."""

    name: str
    value: str | int | float | bool | list[str]


@dataclass
class ParseDiagnostics:
    """Parse-time diagnostic counters."""

    dangling_refs: int = 0
    cycle_breaks: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass
class SignatureVector:
    """Product-level signature vector."""

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
    name: str | None = None
    data_facts: tuple[tuple[str, str], ...] = ()


@dataclass
class SignatureBundle:
    """Bottom-layer output for a single IFC file."""

    filepath: str
    schema: str
    canon_version: str
    signatures: dict[int, SignatureVector]
    diagnostics: ParseDiagnostics
    edge_stats: dict[str, int]
    property_index: dict[int, list[PropertyEntry]] | None = None
