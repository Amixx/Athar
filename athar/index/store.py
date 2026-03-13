"""GraphIR-backed in-memory indices for query-oriented access."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, NamedTuple
import warnings

import ifcopenshell
from ifcopenshell.util import schema as schema_util

from athar.graph.canonical_values import canonical_string
from athar.graph.types import EntityIR, GraphIR


class EdgeEntry(NamedTuple):
    """Labeled relationship edge with attribute provenance."""

    step_id: int
    attr_name: str
    in_aggregate: bool


class GraphIndex:
    """Acceleration indices derived from a parsed GraphIR."""

    def __init__(self, *, graph: GraphIR, schema: str | None = None) -> None:
        self._graph = graph
        self._schema = schema
        self._schema_definition = _load_schema(schema)

        self._by_type: dict[str, list[int]] = {}
        self._subtypes: dict[str, set[str]] = {}
        self._by_guid: dict[str, list[int]] = {}
        self._by_attr_key: dict[str, list[int]] = {}
        self._by_attr_nonnull_key: dict[str, list[int]] = {}
        self._by_attr_value: dict[tuple[str, str], list[int]] = {}
        self._forward: dict[int, list[EdgeEntry]] = {}
        self._reverse: dict[int, list[EdgeEntry]] = {}
        self._type_counts: dict[str, int] = {}
        self._total_entities = 0

        self._build()

    @classmethod
    def from_graph(cls, graph: GraphIR, *, schema: str | None = None) -> "GraphIndex":
        """Build indices from GraphIR, following plan schema precedence."""
        graph_schema = graph.get("metadata", {}).get("schema")
        if schema and graph_schema and schema != graph_schema:
            warnings.warn(
                (
                    "GraphIndex schema override does not match graph metadata "
                    f"({schema!r} != {graph_schema!r}); using explicit override"
                ),
                stacklevel=2,
            )
        resolved_schema = schema or graph_schema
        return cls(graph=graph, schema=resolved_schema)

    @property
    def graph(self) -> GraphIR:
        return self._graph

    @property
    def schema(self) -> str | None:
        return self._schema

    @property
    def total_entities(self) -> int:
        return self._total_entities

    def entities_of_type(self, entity_type: str, *, include_subtypes: bool = False) -> list[int]:
        """Return step ids with the requested declared type."""
        if not include_subtypes:
            return list(self._by_type.get(entity_type, ()))
        if self._schema_definition is None:
            raise ValueError("Subtype queries require a resolvable IFC schema")
        matching_types = self._subtypes_for(entity_type)
        if not matching_types:
            return []
        steps: set[int] = set()
        for declared_type in matching_types:
            steps.update(self._by_type.get(declared_type, ()))
        return sorted(steps)

    def type_counts(self) -> dict[str, int]:
        return dict(self._type_counts)

    def has_type(self, entity_type: str) -> bool:
        return entity_type in self._by_type

    def entity_by_guid(self, guid: str) -> int | None:
        steps = self._by_guid.get(guid, ())
        if len(steps) != 1:
            return None
        return steps[0]

    def unique_entity_by_guid(self, guid: str) -> int | None:
        return self.entity_by_guid(guid)

    def entities_by_guid(self, guid: str) -> list[int]:
        return list(self._by_guid.get(guid, ()))

    def all_guids(self) -> dict[str, list[int]]:
        return {guid: list(steps) for guid, steps in self._by_guid.items()}

    def entities_with_attribute(self, attr_name: str) -> list[int]:
        return list(self._by_attr_key.get(attr_name, ()))

    def entities_with_nonempty_attribute(self, attr_name: str) -> list[int]:
        return list(self._by_attr_nonnull_key.get(attr_name, ()))

    def entities_with_attribute_value(self, attr_name: str, value: dict[str, Any]) -> list[int]:
        key = (attr_name, canonical_string(value))
        return list(self._by_attr_value.get(key, ()))

    def sources_of(self, target_step: int) -> list[EdgeEntry]:
        return list(self._reverse.get(target_step, ()))

    def targets_of(self, source_step: int) -> list[EdgeEntry]:
        return list(self._forward.get(source_step, ()))

    def entity(self, step_id: int) -> EntityIR | None:
        return self._graph["entities"].get(step_id)

    def entities(self, step_ids: list[int]) -> list[EntityIR]:
        return [
            entity
            for step_id in step_ids
            if (entity := self.entity(step_id)) is not None
        ]

    def _build(self) -> None:
        entities = self._graph["entities"]
        by_type: dict[str, list[int]] = defaultdict(list)
        by_guid: dict[str, list[int]] = defaultdict(list)
        by_attr_key: dict[str, list[int]] = defaultdict(list)
        by_attr_nonnull_key: dict[str, list[int]] = defaultdict(list)
        by_attr_value: dict[tuple[str, str], list[int]] = defaultdict(list)
        forward: dict[int, list[EdgeEntry]] = {step_id: [] for step_id in entities}
        reverse: dict[int, list[EdgeEntry]] = {step_id: [] for step_id in entities}

        for step_id, entity in entities.items():
            self._total_entities += 1

            entity_type = entity.get("entity_type")
            if entity_type:
                by_type[entity_type].append(step_id)

            guid = entity.get("global_id")
            if isinstance(guid, str):
                by_guid[guid].append(step_id)

            for attr_name, value in entity.get("attributes", {}).items():
                by_attr_key[attr_name].append(step_id)
                if value.get("kind") != "null":
                    by_attr_nonnull_key[attr_name].append(step_id)
                by_attr_value[(attr_name, canonical_string(value))].append(step_id)

            for ref in entity.get("refs", []):
                target_step = ref.get("target")
                if not isinstance(target_step, int) or target_step not in entities:
                    continue
                attr_name, in_aggregate = _edge_provenance(ref.get("path", ""))
                edge = EdgeEntry(step_id=target_step, attr_name=attr_name, in_aggregate=in_aggregate)
                reverse_edge = EdgeEntry(step_id=step_id, attr_name=attr_name, in_aggregate=in_aggregate)
                forward[step_id].append(edge)
                reverse[target_step].append(reverse_edge)

        self._by_type = _sorted_multimap(by_type)
        self._type_counts = {entity_type: len(steps) for entity_type, steps in self._by_type.items()}
        self._by_guid = _sorted_multimap(by_guid)
        self._by_attr_key = _sorted_multimap(by_attr_key)
        self._by_attr_nonnull_key = _sorted_multimap(by_attr_nonnull_key)
        self._by_attr_value = {key: sorted(values) for key, values in by_attr_value.items()}
        self._forward = {
            step_id: sorted(edges, key=_edge_sort_key)
            for step_id, edges in forward.items()
        }
        self._reverse = {
            step_id: sorted(edges, key=_edge_sort_key)
            for step_id, edges in reverse.items()
        }

    def _subtypes_for(self, entity_type: str) -> set[str]:
        cached = self._subtypes.get(entity_type)
        if cached is not None:
            return cached
        if self._schema_definition is None:
            raise ValueError("Subtype queries require a resolvable IFC schema")
        try:
            declaration = self._schema_definition.declaration_by_name(entity_type)
        except RuntimeError:
            result: set[str] = set()
        else:
            result = {decl.name() for decl in schema_util.get_subtypes(declaration)}
        self._subtypes[entity_type] = result
        return result


def _edge_provenance(path: str) -> tuple[str, bool]:
    stripped = path.lstrip("/")
    if not stripped:
        return "", False
    segments = stripped.split("/")
    attr_name = segments[0].split("@", 1)[0]
    return attr_name, len(segments) > 1


def _load_schema(schema: str | None):
    if not schema:
        return None
    try:
        return ifcopenshell.schema_by_name(schema)
    except RuntimeError:
        return None


def _sorted_multimap(values: dict[Any, list[int]]) -> dict[Any, list[int]]:
    return {key: sorted(step_ids) for key, step_ids in values.items()}


def _edge_sort_key(edge: EdgeEntry) -> tuple[str, int, bool]:
    return (edge.attr_name, edge.step_id, edge.in_aggregate)
