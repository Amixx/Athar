"""Streaming serialization helpers for diff output."""

from __future__ import annotations

from typing import Any

from .determinism import canonical_json


def stream_diff_result(
    result: dict[str, Any],
    *,
    mode: str = "ndjson",
    chunk_size: int = 1000,
):
    """Yield streamed JSON output for a completed diff result."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if mode == "ndjson":
        yield from _stream_ndjson(result)
        return
    if mode == "chunked_json":
        yield from _stream_chunked_json(result, chunk_size=chunk_size)
        return
    raise ValueError(f"Unknown stream mode: {mode}")


def json_line(payload: dict[str, Any]) -> str:
    return canonical_json(payload)


def _stream_ndjson(result: dict[str, Any]):
    header = _stream_header(result)
    base_changes = list(result.get("base_changes", []))
    derived_markers = list(result.get("derived_markers", []))

    yield json_line({"record_type": "header", **header})
    for idx, change in enumerate(base_changes):
        yield json_line({"record_type": "base_change", "index": idx, "change": change})
    for idx, marker in enumerate(derived_markers):
        yield json_line({"record_type": "derived_marker", "index": idx, "marker": marker})
    yield json_line({
        "record_type": "end",
        "base_change_count": len(base_changes),
        "derived_marker_count": len(derived_markers),
    })


def _stream_chunked_json(result: dict[str, Any], *, chunk_size: int):
    header = _stream_header(result)
    base_changes = list(result.get("base_changes", []))
    derived_markers = list(result.get("derived_markers", []))

    yield json_line({"chunk_type": "header", **header})
    for offset in range(0, len(base_changes), chunk_size):
        items = base_changes[offset: offset + chunk_size]
        yield json_line({
            "chunk_type": "base_changes",
            "offset": offset,
            "count": len(items),
            "items": items,
        })
    for offset in range(0, len(derived_markers), chunk_size):
        items = derived_markers[offset: offset + chunk_size]
        yield json_line({
            "chunk_type": "derived_markers",
            "offset": offset,
            "count": len(items),
            "items": items,
        })
    yield json_line({
        "chunk_type": "end",
        "base_change_count": len(base_changes),
        "derived_marker_count": len(derived_markers),
    })


def _stream_header(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": result.get("version"),
        "profile": result.get("profile"),
        "schema_policy": result.get("schema_policy"),
        "identity_policy": result.get("identity_policy"),
        "stats": result.get("stats"),
    }
