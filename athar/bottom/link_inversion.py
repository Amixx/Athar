"""Link inversion helpers for parsed IFC entities."""

from __future__ import annotations

from collections import defaultdict

from .types import EntityRef, ParsedEntity


def invert_entity_refs(entities: dict[int, ParsedEntity]) -> dict[int, list[EntityRef]]:
    """Build reverse edge map target_step -> inbound refs."""
    incoming: dict[int, list[EntityRef]] = defaultdict(list)
    for entity in entities.values():
        for ref in entity.refs:
            incoming[ref.target_step].append(ref)
    for refs in incoming.values():
        refs.sort(key=lambda r: (r.source_step, r.attr_name, r.path, r.target_type))
    return dict(incoming)

