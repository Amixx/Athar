"""Shared lightweight type contracts for graph diff modules."""

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


class IdentityInfo(TypedDict, total=False):
    match_method: str
    match_confidence: float
    matched_on: dict[str, Any] | None
    stability_tier: str


class ContextEntityItem(TypedDict, total=False):
    step_id: int
    entity: EntityIR
    identity: IdentityInfo
    profile_entity: EntityIR
    compare_entity: EntityIR
    profile_hash: str


class DiffContext(TypedDict, total=False):
    version: str
    profile: str
    geometry_policy: str
    old_graph: GraphIR
    new_graph: GraphIR
    old_ids: dict[int, str]
    new_ids: dict[int, str]
    old_by_id: dict[str, list[ContextEntityItem]]
    new_by_id: dict[str, list[ContextEntityItem]]
    old_owner_projector: Any
    new_owner_projector: Any
    schema_policy: dict[str, Any]
    identity_policy: dict[str, Any]
    stats: dict[str, Any]
