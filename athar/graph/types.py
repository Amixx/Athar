"""Graph-layer type contracts shared by graph and diff code."""

from __future__ import annotations

from typing import Any, TypedDict


class EntityRefIR(TypedDict, total=False):
    path: str
    target: int
    target_type: str | None


class EntityIR(TypedDict, total=False):
    entity_type: str | None
    global_id: str
    attributes: dict[str, Any]
    refs: list[EntityRefIR]


class GraphIR(TypedDict):
    metadata: dict[str, Any]
    entities: dict[int, EntityIR]
