"""Core data contracts for the bottom layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple


HashHex = str


@dataclass(frozen=True)
class PropertyEntry:
    """One extracted property or quantity value.

    ``inherited`` is true for a definition on the type object. ``pset_guid``
    is the GlobalId of the defining property or quantity set. A value of a
    measure with a unit type is converted from the file's unit, or the unit
    the property names, to the SI unit labelled by ``unit``; other values are
    as written and ``unit`` is ``None``. ``written`` is the value as the file
    writes it where that differs from ``value``.
    """

    pset: str
    name: str
    value: str | int | float | bool | list
    inherited: bool
    pset_guid: str | None = None
    unit: str | None = None
    written: str | int | float | bool | list | None = None


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


class RelationTarget(NamedTuple):
    """An entity a root names through a relationship.

    ``relation`` is the relationship class and ``role`` its attribute holding
    the named entity. ``step_id`` is a signature root of the same file or a
    key of ``SignatureBundle.related_entities``.
    """

    relation: str
    role: str
    step_id: int


class RelatedEntity(NamedTuple):
    """A relation target without a signature, such as a type object.

    ``content`` is a digest of the entity's content, ``GlobalId`` excluded.
    ``guid_shared`` is true when another entity of the file carries the same
    ``GlobalId``.
    """

    guid: str | None
    canonical_class: str
    name: str | None
    content: HashHex
    guid_shared: bool


@dataclass
class SignatureVector:
    """Signature vector of one root: a product, spatial element or the project.

    ``placement`` is the quantized world matrix. A chain through a placement
    the engine does not evaluate (``IfcGridPlacement``) has no matrix;
    ``placement_unresolved`` then holds a digest of the chain's content.

    ``local_placement`` is the quantized frame relative to the object
    placement of ``placement_parent``, the root (by step id) owning the
    nearest placement above this one that places a root; without such a root
    it is relative to the end of the local placement chain.
    """

    step_id: int
    guid: str | None
    entity_type: str
    canonical_class: str
    vh_geometry: HashHex
    vh_data: HashHex
    vh_topology: HashHex
    placement: tuple[int, ...] | None
    canon_version: str
    name: str | None = None
    data_facts: tuple[tuple[str, str], ...] = ()
    placement_unresolved: HashHex | None = None
    relation_targets: tuple[RelationTarget, ...] = ()
    local_placement: tuple[int, ...] | None = None
    placement_parent: int | None = None


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
    related_entities: dict[int, RelatedEntity] = field(default_factory=dict)
