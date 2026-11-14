"""Core data contracts for the bottom layer."""

from __future__ import annotations

from dataclasses import dataclass, field


HashHex = str


@dataclass(frozen=True)
class PropertyEntry:
    """One extracted property or quantity value.

    ``inherited`` is true for a definition on the type object. ``pset_guid``
    is the GlobalId of the defining property or quantity set. A length, area,
    volume, plane angle, time or mass value is converted from the file's unit
    to the SI unit named by ``unit``; other values are as written and
    ``unit`` is ``None``.
    """

    pset: str
    name: str
    value: str | int | float | bool | list[str]
    inherited: bool
    pset_guid: str | None = None
    unit: str | None = None


@dataclass(frozen=True)
class OrphanPropertySet:
    """A property set definition no object or type references.

    ``content_key`` is the set's canonical content without ``GlobalId`` and
    ``OwnerHistory``, so identical copies share a key.
    """

    step_id: int
    guid: str | None
    name: str | None
    entity_type: str
    content_key: str


@dataclass
class ParseDiagnostics:
    """Parse-time diagnostic counters."""

    dangling_refs: int = 0
    cycle_breaks: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass
class Coverage:
    """What the signatures of one file did not examine.

    ``unknown_relationships`` counts relationship instances whose class has no
    policy entry. ``unreached`` maps each class with instances no signature
    root reaches to ``(instance count, order-independent content digest)``.
    ``ignored_by_design`` counts instances excluded on purpose (provenance,
    ignored relationship classes).
    """

    unknown_relationships: dict[str, int] = field(default_factory=dict)
    unreached: dict[str, tuple[int, str]] = field(default_factory=dict)
    ignored_by_design: dict[str, int] = field(default_factory=dict)


@dataclass
class SignatureVector:
    """Signature vector of one root: a product, spatial element or the project.

    ``placement`` is the quantized world matrix. A chain through a placement
    the engine does not evaluate (``IfcGridPlacement``) has no matrix;
    ``placement_unresolved`` then holds a digest of the chain's content and
    ``centroid`` and ``aabb`` are ``None``.
    """

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
    placement_unresolved: HashHex | None = None


@dataclass
class SignatureBundle:
    """Bottom-layer output for a single IFC file."""

    filepath: str
    schema: str
    canon_version: str
    signatures: dict[int, SignatureVector]
    diagnostics: ParseDiagnostics
    edge_stats: dict[str, int]
    coverage: Coverage
    property_index: dict[int, list[PropertyEntry]] | None = None
    orphan_property_sets: tuple[OrphanPropertySet, ...] = ()
