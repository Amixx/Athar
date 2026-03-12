"""Streaming serialization helpers for diff output."""

from __future__ import annotations

from typing import Any, Iterable

from .determinism import canonical_json


def stream_diff_result(
    result: dict[str, Any],
    *,
    mode: str = "ndjson",
    chunk_size: int = 1000,
):
    """Yield streamed JSON output for a completed diff result."""
    events = iter_result_events(result)
    yield from stream_diff_events(events, mode=mode, chunk_size=chunk_size)


def stream_diff_events(
    events: Iterable[dict[str, Any]],
    *,
    mode: str = "ndjson",
    chunk_size: int = 1000,
):
    """Yield streamed JSON output for header/base-change/marker events."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if mode == "ndjson":
        yield from _stream_events_ndjson(events)
        return
    if mode == "chunked_json":
        yield from _stream_events_chunked_json(events, chunk_size=chunk_size)
        return
    raise ValueError(f"Unknown stream mode: {mode}")


def json_line(payload: dict[str, Any]) -> str:
    return canonical_json(payload)


def iter_result_events(result: dict[str, Any]):
    """Yield normalized stream events for a completed diff result object."""
    header = _stream_header(result)
    yield {"event_type": "header", "header": header}
    for change in result.get("base_changes", []):
        yield {"event_type": "base_change", "change": change}
    for marker in result.get("derived_markers", []):
        yield {"event_type": "derived_marker", "marker": marker}


def _stream_events_ndjson(events: Iterable[dict[str, Any]]):
    base_count = 0
    marker_count = 0
    op_counts: dict[str, int] = {}

    for event in events:
        event_type = event.get("event_type")
        if event_type == "header":
            header = event.get("header", {})
            yield json_line({"record_type": "header", **header})
            continue
        if event_type == "base_change":
            change = event["change"]
            op = change.get("op")
            if isinstance(op, str):
                op_counts[op] = op_counts.get(op, 0) + 1
            yield json_line({"record_type": "base_change", "index": base_count, "change": change})
            base_count += 1
            continue
        if event_type == "derived_marker":
            marker = event["marker"]
            yield json_line({"record_type": "derived_marker", "index": marker_count, "marker": marker})
            marker_count += 1
            continue
        raise ValueError(f"Unknown stream event type: {event_type}")

    yield json_line({
        "record_type": "end",
        "base_change_count": base_count,
        "derived_marker_count": marker_count,
        "op_counts": {k: op_counts[k] for k in sorted(op_counts)},
    })


def _stream_events_chunked_json(events: Iterable[dict[str, Any]], *, chunk_size: int):
    base_count = 0
    marker_count = 0
    op_counts: dict[str, int] = {}
    base_offset = 0
    marker_offset = 0
    base_buffer: list[dict[str, Any]] = []
    marker_buffer: list[dict[str, Any]] = []

    def flush_base():
        nonlocal base_offset, base_buffer
        if not base_buffer:
            return []
        payload = {
            "chunk_type": "base_changes",
            "offset": base_offset,
            "count": len(base_buffer),
            "items": base_buffer,
        }
        base_offset += len(base_buffer)
        base_buffer = []
        return [payload]

    def flush_markers():
        nonlocal marker_offset, marker_buffer
        if not marker_buffer:
            return []
        payload = {
            "chunk_type": "derived_markers",
            "offset": marker_offset,
            "count": len(marker_buffer),
            "items": marker_buffer,
        }
        marker_offset += len(marker_buffer)
        marker_buffer = []
        return [payload]

    for event in events:
        event_type = event.get("event_type")
        if event_type == "header":
            header = event.get("header", {})
            yield json_line({"chunk_type": "header", **header})
            continue
        if event_type == "base_change":
            change = event["change"]
            op = change.get("op")
            if isinstance(op, str):
                op_counts[op] = op_counts.get(op, 0) + 1
            base_buffer.append(change)
            base_count += 1
            if len(base_buffer) >= chunk_size:
                for payload in flush_base():
                    yield json_line(payload)
            continue
        if event_type == "derived_marker":
            marker_buffer.append(event["marker"])
            marker_count += 1
            if len(marker_buffer) >= chunk_size:
                for payload in flush_markers():
                    yield json_line(payload)
            continue
        raise ValueError(f"Unknown stream event type: {event_type}")

    for payload in flush_base():
        yield json_line(payload)
    for payload in flush_markers():
        yield json_line(payload)

    yield json_line({
        "chunk_type": "end",
        "base_change_count": base_count,
        "derived_marker_count": marker_count,
        "op_counts": {k: op_counts[k] for k in sorted(op_counts)},
    })


def _stream_header(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": result.get("version"),
        "profile": result.get("profile"),
        "geometry_policy": result.get("geometry_policy"),
        "schema_policy": result.get("schema_policy"),
        "identity_policy": result.get("identity_policy"),
        "stats": result.get("stats"),
    }
