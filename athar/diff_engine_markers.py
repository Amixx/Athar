"""Relationship/owner projection helpers for graph diff output."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any

_REPARENT_RELATION_TYPES = frozenset({
    "IfcRelContainedInSpatialStructure",
    "IfcRelAggregates",
    "IfcRelNests",
})
OWNER_INDEX_DISK_THRESHOLD_ENV = "ATHAR_OWNER_INDEX_DISK_THRESHOLD"


class _DiskBackedOwnerIndex:
    """Disk-backed owner index for high-cardinality rooted-owner projections."""

    def __init__(self) -> None:
        fd, path = tempfile.mkstemp(prefix="athar_owner_index_", suffix=".sqlite3")
        os.close(fd)
        self._path = path
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=OFF")
        self._conn.execute("PRAGMA synchronous=OFF")
        self._conn.execute("CREATE TABLE owners(step_id INTEGER NOT NULL, owner_id TEXT NOT NULL, PRIMARY KEY(step_id, owner_id))")
        self._buffer: list[tuple[int, str]] = []
        self._closed = False

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def add_owner(self, step_id: int, owner_id: str) -> None:
        self._buffer.append((step_id, owner_id))
        if len(self._buffer) >= 4096:
            self._flush()

    def commit(self) -> None:
        self._flush()
        self._conn.commit()

    def owners_for_step(self, step_id: int) -> set[str]:
        rows = self._conn.execute(
            "SELECT owner_id FROM owners WHERE step_id = ? ORDER BY owner_id",
            (step_id,),
        ).fetchall()
        return {row[0] for row in rows}

    def owners_for_steps(self, step_ids: list[int]) -> set[str]:
        merged: set[str] = set()
        unique_ids = sorted(set(step_ids))
        if not unique_ids:
            return merged
        chunk_size = 800
        for i in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT DISTINCT owner_id FROM owners WHERE step_id IN ({placeholders})"
            rows = self._conn.execute(query, chunk).fetchall()
            merged.update(row[0] for row in rows)
        return merged

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._flush()
            self._conn.close()
        finally:
            try:
                os.unlink(self._path)
            except FileNotFoundError:
                pass

    def _flush(self) -> None:
        if not self._buffer:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO owners(step_id, owner_id) VALUES(?, ?)",
            self._buffer,
        )
        self._buffer.clear()


class RootedOwnerProjector:
    """Lazy rooted-owner projection.

    Owner closure is materialized only when a change asks for owner summaries.
    """

    def __init__(self, graph: dict, ids: dict[int, str]) -> None:
        self._graph = graph
        self._ids = ids
        self._owners_index: dict[int, set[str]] | _DiskBackedOwnerIndex | None = None

    def owners_for_step(self, step_id: int) -> set[str]:
        owners = self._materialize()
        if isinstance(owners, _DiskBackedOwnerIndex):
            return owners.owners_for_step(step_id)
        return owners.get(step_id, set())

    def owners_for_steps(self, step_ids: list[int]) -> set[str]:
        owners = self._materialize()
        if isinstance(owners, _DiskBackedOwnerIndex):
            return owners.owners_for_steps(step_ids)
        merged: set[str] = set()
        for step_id in step_ids:
            merged.update(owners.get(step_id, set()))
        return merged

    def close(self) -> None:
        owners = self._owners_index
        if isinstance(owners, _DiskBackedOwnerIndex):
            owners.close()
        self._owners_index = None

    def __del__(self) -> None:
        self.close()

    def _materialize(self) -> dict[int, set[str]] | _DiskBackedOwnerIndex:
        if self._owners_index is None:
            if _should_spill_owner_index(self._graph, self._ids):
                self._owners_index = compute_rooted_owner_index_disk(self._graph, self._ids)
            else:
                self._owners_index = compute_rooted_owner_index(self._graph, self._ids)
        return self._owners_index


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
    owners_by_step: dict[int, set[str]] = {step_id: set() for step_id in entities}
    for step_id, root_id in _iter_owner_pairs(graph, ids):
        owners_by_step.setdefault(step_id, set()).add(root_id)
    return owners_by_step


def compute_rooted_owner_index_disk(
    graph: dict,
    ids: dict[int, str],
) -> _DiskBackedOwnerIndex:
    owners = _DiskBackedOwnerIndex()
    owners.begin()
    for step_id, root_id in _iter_owner_pairs(graph, ids):
        owners.add_owner(step_id, root_id)
    owners.commit()
    return owners


def _should_spill_owner_index(graph: dict, ids: dict[int, str]) -> bool:
    threshold = owner_index_disk_threshold()
    if threshold <= 0:
        return False
    entities = graph.get("entities", {})
    root_count = sum(1 for entity_id in ids.values() if entity_id.startswith("G:"))
    if root_count <= 0:
        return False
    estimated_pairs = len(entities) * root_count
    return estimated_pairs >= threshold


def owner_index_disk_threshold() -> int:
    raw_threshold = os.environ.get(OWNER_INDEX_DISK_THRESHOLD_ENV, "0").strip()
    try:
        threshold = int(raw_threshold)
    except ValueError:
        return 0
    return max(0, threshold)


def _iter_owner_pairs(graph: dict, ids: dict[int, str]):
    entities = graph.get("entities", {})
    adjacency: dict[int, list[int]] = {}
    for step_id, entity in entities.items():
        targets = [
            ref.get("target")
            for ref in entity.get("refs", [])
            if ref.get("target") in entities
        ]
        adjacency[step_id] = sorted(set(targets))

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
            yield step, root_id
            for target in adjacency.get(step, []):
                if target not in seen:
                    stack.append(target)


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
