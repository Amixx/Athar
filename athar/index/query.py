"""Thin query composition helpers over GraphIndex."""

from __future__ import annotations

from typing import Any

from athar.graph.types import EntityIR

from .store import GraphIndex


def query_step_ids(
    index: GraphIndex,
    *,
    entity_type: str | None = None,
    include_subtypes: bool = False,
    guid: str | None = None,
    attr_name: str | None = None,
    require_nonnull: bool = False,
    attr_value: dict[str, Any] | None = None,
) -> list[int]:
    """Compose common type/GUID/attribute filters against GraphIndex."""
    if attr_value is not None and attr_name is None:
        raise ValueError("attr_name is required when filtering by attr_value")

    candidate_sets: list[set[int]] = []

    if entity_type is not None:
        candidate_sets.append(set(index.entities_of_type(entity_type, include_subtypes=include_subtypes)))
    if guid is not None:
        candidate_sets.append(set(index.entities_by_guid(guid)))
    if attr_name is not None:
        if attr_value is not None:
            candidate_sets.append(set(index.entities_with_attribute_value(attr_name, attr_value)))
        elif require_nonnull:
            candidate_sets.append(set(index.entities_with_nonempty_attribute(attr_name)))
        else:
            candidate_sets.append(set(index.entities_with_attribute(attr_name)))

    if not candidate_sets:
        return sorted(index.graph["entities"])

    steps = candidate_sets[0]
    for other in candidate_sets[1:]:
        steps &= other
    return sorted(steps)


def query_entities(index: GraphIndex, **filters: Any) -> list[EntityIR]:
    """Return entities for the matching step ids."""
    return index.entities(query_step_ids(index, **filters))
