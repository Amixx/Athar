"""Compatibility adapter that routes legacy diff calls to the graph engine."""

from __future__ import annotations

from collections.abc import Mapping
from os import PathLike
from typing import Any

from .diff_engine import diff_files, diff_graphs
from .geometry_policy import GEOMETRY_POLICY_STRICT_SYNTAX
from .guid_policy import GUID_POLICY_FAIL_FAST
from .profile_policy import DEFAULT_PROFILE
from .types import GraphIR


def diff(
    old: str | PathLike[str] | GraphIR | Mapping[str, Any],
    new: str | PathLike[str] | GraphIR | Mapping[str, Any],
    *,
    profile: str = DEFAULT_PROFILE,
    geometry_policy: str = GEOMETRY_POLICY_STRICT_SYNTAX,
    guid_policy: str = GUID_POLICY_FAIL_FAST,
    matcher_policy: dict[str, dict[str, Any]] | None = None,
    timings: bool = False,
) -> dict:
    """Route legacy diff invocations through the current graph engine.

    Supported call styles:
    - file paths (str / path-like): delegates to ``diff_files``.
    - graph dictionaries: delegates to ``diff_graphs``.
    """
    old_path = _as_path(old)
    new_path = _as_path(new)
    if old_path is not None and new_path is not None:
        return diff_files(
            old_path,
            new_path,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=matcher_policy,
            timings=timings,
        )

    if old_path is None and new_path is None and _looks_like_graph(old) and _looks_like_graph(new):
        old_graph = _coerce_graph(old)
        new_graph = _coerce_graph(new)
        _backfill_schema(old_graph, new_graph)
        return diff_graphs(
            old_graph,
            new_graph,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=matcher_policy,
            timings=timings,
        )

    raise TypeError(
        "diff() expects either two IFC paths or two graph dictionaries with an 'entities' map"
    )


def _as_path(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, PathLike):
        return str(value)
    return None


def _looks_like_graph(value: object) -> bool:
    return isinstance(value, Mapping) and "entities" in value


def _coerce_graph(value: Mapping[str, Any]) -> GraphIR:
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    out_metadata = dict(metadata)
    out_metadata.setdefault("schema", None)
    return {
        "metadata": out_metadata,
        "entities": value["entities"],
    }


def _backfill_schema(old_graph: GraphIR, new_graph: GraphIR) -> None:
    old_schema = old_graph.get("metadata", {}).get("schema")
    new_schema = new_graph.get("metadata", {}).get("schema")
    if old_schema is None and new_schema is not None:
        old_graph["metadata"]["schema"] = new_schema
    elif new_schema is None and old_schema is not None:
        new_graph["metadata"]["schema"] = old_schema
