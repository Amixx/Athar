"""Relationship/owner projection helpers for graph diff output."""

from __future__ import annotations

from typing import Any

_REPARENT_RELATION_TYPES = frozenset({
    "IfcRelContainedInSpatialStructure",
    "IfcRelAggregates",
    "IfcRelNests",
})


class RootedOwnerProjector:
    """Lazy rooted-owner projection.

    Owner closure is materialized only when a change asks for owner summaries.
    """

    def __init__(self, graph: dict, ids: dict[int, str]) -> None:
        self._graph = graph
        self._ids = ids
        self._owners_by_step: dict[int, set[str]] | None = None

    def owners_for_step(self, step_id: int) -> set[str]:
        owners = self._materialize()
        return owners.get(step_id, set())

    def owners_for_steps(self, step_ids: list[int]) -> set[str]:
        owners = self._materialize()
        merged: set[str] = set()
        for step_id in step_ids:
            merged.update(owners.get(step_id, set()))
        return merged

    def _materialize(self) -> dict[int, set[str]]:
        if self._owners_by_step is None:
            self._owners_by_step = compute_rooted_owner_index(self._graph, self._ids)
        return self._owners_by_step


def build_derived_markers(
    *,
    old_graph: dict,
    new_graph: dict,
    old_ids: dict[int, str],
    new_ids: dict[int, str],
    change_index: dict[str, list[str]],
) -> list[dict[str, Any]]:
    old_links = _extract_parent_links(old_graph, old_ids)
    new_links = _extract_parent_links(new_graph, new_ids)

    markers: list[dict[str, Any]] = []
    for key in sorted(set(old_links) & set(new_links)):
        relation_type, child_id = key
        old_link = old_links[key]
        new_link = new_links[key]
        old_parent_id = old_link["parent_id"]
        new_parent_id = new_link["parent_id"]
        if old_parent_id == new_parent_id:
            continue

        source_change_ids = sorted(set(
            change_index.get(old_link["relation_id"], [])
            + change_index.get(new_link["relation_id"], [])
            + change_index.get(child_id, [])
            + change_index.get(old_parent_id, [])
            + change_index.get(new_parent_id, [])
        ))
        markers.append({
            "marker_type": "REPARENT",
            "relation_type": relation_type,
            "child_id": child_id,
            "old_parent_id": old_parent_id,
            "new_parent_id": new_parent_id,
            "source_change_ids": source_change_ids,
        })
    return markers


def compute_rooted_owner_index(
    graph: dict,
    ids: dict[int, str],
) -> dict[int, set[str]]:
    entities = graph.get("entities", {})
    adjacency: dict[int, list[int]] = {}
    for step_id, entity in entities.items():
        targets = [
            ref.get("target")
            for ref in entity.get("refs", [])
            if ref.get("target") in entities
        ]
        adjacency[step_id] = sorted(set(targets))

    owners_by_step: dict[int, set[str]] = {step_id: set() for step_id in entities}
    root_steps = sorted(
        [step_id for step_id, entity_id in ids.items() if entity_id.startswith("G:")],
        key=lambda step_id: ids[step_id],
    )
    for root_step in root_steps:
        root_id = ids[root_step]
        stack = [root_step]
        seen: set[int] = set()
        while stack:
            step = stack.pop()
            if step in seen:
                continue
            seen.add(step)
            owners_by_step.setdefault(step, set()).add(root_id)
            for target in adjacency.get(step, []):
                if target not in seen:
                    stack.append(target)
    return owners_by_step


def summarize_rooted_owners(owner_ids: set[str], *, sample_size: int = 5) -> dict[str, Any]:
    ordered = sorted(owner_ids)
    return {"sample": ordered[:sample_size], "total": len(ordered)}


def _extract_parent_links(graph: dict, ids: dict[int, str]) -> dict[tuple[str, str], dict[str, str]]:
    links: dict[tuple[str, str], dict[str, str]] = {}
    ambiguous: set[tuple[str, str]] = set()

    for step_id, entity in graph.get("entities", {}).items():
        relation_type = entity.get("entity_type")
        if relation_type not in _REPARENT_RELATION_TYPES:
            continue
        relation_id = ids.get(step_id)
        if relation_id is None:
            continue

        parent_steps = sorted({
            ref.get("target")
            for ref in entity.get("refs", [])
            if str(ref.get("path", "")).startswith("/Relating")
            and ref.get("target") is not None
        })
        child_steps = sorted({
            ref.get("target")
            for ref in entity.get("refs", [])
            if str(ref.get("path", "")).startswith("/Related")
            and ref.get("target") is not None
        })
        if len(parent_steps) != 1 or not child_steps:
            continue

        parent_id = ids.get(parent_steps[0])
        if parent_id is None:
            continue

        for child_step in child_steps:
            child_id = ids.get(child_step)
            if child_id is None:
                continue
            key = (relation_type, child_id)
            existing = links.get(key)
            if existing and existing["parent_id"] != parent_id:
                ambiguous.add(key)
                continue
            links[key] = {
                "parent_id": parent_id,
                "relation_id": relation_id,
            }

    for key in ambiguous:
        links.pop(key, None)
    return links
