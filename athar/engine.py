"""Current Athar engine entrypoints."""

from __future__ import annotations

import json
import os
from collections import Counter
from typing import Iterator

from athar.bottom.constants import CANON_VERSION, DEFAULT_MATCH_RADIUS_M
from athar.bottom.signatures import build_signature_bundle
from athar.delta.report import build_delta_report
from athar.matcher.core import DEFAULT_SPATIAL_PROBE_LIMIT, match_signatures

_BUNDLE_CACHE: dict[tuple[str, int, int], object] = {}


def diff_files(
    old_path: str,
    new_path: str,
    *,
    matcher_policy: dict | None = None,
) -> dict:
    """Diff two IFC files with the current architecture."""

    old_bundle = _load_bundle(old_path)
    if old_path == new_path:
        new_bundle = old_bundle
    else:
        new_bundle = _load_bundle(new_path)
    _assert_schema_compatible(old_bundle.schema, new_bundle.schema)

    matches, unmatched_old, unmatched_new, matcher_diagnostics = match_signatures(
        old_bundle.signatures,
        new_bundle.signatures,
        radius_m=_matcher_radius_from_policy(matcher_policy),
        probe_limit=_probe_limit_from_policy(matcher_policy),
    )
    report = build_delta_report(old_bundle, new_bundle, matches, unmatched_old, unmatched_new)
    report["stats"]["guid_collisions"] = {
        "old": _guid_collision_count(old_bundle.signatures),
        "new": _guid_collision_count(new_bundle.signatures),
    }
    report["stats"]["matcher_diagnostics"] = matcher_diagnostics
    report["canon_version"] = CANON_VERSION
    return report


def stream_diff_files(
    old_path: str,
    new_path: str,
    *,
    matcher_policy: dict | None = None,
    mode: str = "ndjson",
    chunk_size: int = 1000,
) -> Iterator[str]:
    """Stream diff output as NDJSON or chunked JSON."""
    report = diff_files(
        old_path,
        new_path,
        matcher_policy=matcher_policy,
    )
    if mode == "ndjson":
        yield from _stream_ndjson(report)
        return
    if mode == "chunked_json":
        yield from _stream_chunked_json(report, chunk_size=max(1, int(chunk_size)))
        return
    raise ValueError(f"Unsupported stream mode: {mode!r}")


def _stream_ndjson(report: dict) -> Iterator[str]:
    yield json.dumps({"record_type": "header", "engine": report.get("engine"), "canon_version": report.get("canon_version")})
    for section in ("added", "deleted", "modified", "unchanged"):
        for item in report.get(section, []):
            yield json.dumps({"record_type": section, "item": item}, sort_keys=True)
    yield json.dumps({"record_type": "end", "stats": report.get("stats", {})}, sort_keys=True)


def _stream_chunked_json(report: dict, *, chunk_size: int) -> Iterator[str]:
    yield json.dumps({"chunk_type": "header", "engine": report.get("engine"), "canon_version": report.get("canon_version")})
    for section in ("added", "deleted", "modified", "unchanged"):
        rows = report.get(section, [])
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            yield json.dumps({"chunk_type": section, "items": chunk}, sort_keys=True)
    yield json.dumps({"chunk_type": "end", "stats": report.get("stats", {})}, sort_keys=True)


def _assert_schema_compatible(old_schema: str, new_schema: str) -> None:
    if old_schema != new_schema:
        raise ValueError(f"Schema mismatch: {old_schema} vs {new_schema}")


def _matcher_radius_from_policy(policy: dict | None) -> float:
    if not isinstance(policy, dict):
        return DEFAULT_MATCH_RADIUS_M
    raw = policy.get("spatial_radius_m")
    if raw is None:
        return DEFAULT_MATCH_RADIUS_M
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or raw <= 0:
        raise ValueError(f"matcher_policy.spatial_radius_m must be a positive number, got {raw!r}")
    return float(raw)


def _probe_limit_from_policy(policy: dict | None) -> int:
    if not isinstance(policy, dict):
        return DEFAULT_SPATIAL_PROBE_LIMIT
    raw = policy.get("spatial_probe_limit")
    if raw is None:
        return DEFAULT_SPATIAL_PROBE_LIMIT
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise ValueError(f"matcher_policy.spatial_probe_limit must be a positive int, got {raw!r}")
    return raw


def _guid_collision_count(signatures) -> int:
    counts = Counter(sig.guid for sig in signatures.values() if sig.guid)
    return sum(1 for _, value in counts.items() if value > 1)


def _load_bundle(path: str):
    stat = os.stat(path)
    key = (path, stat.st_mtime_ns, stat.st_size)
    cached = _BUNDLE_CACHE.get(key)
    if cached is not None:
        return cached
    bundle = build_signature_bundle(path)
    _BUNDLE_CACHE[key] = bundle
    if len(_BUNDLE_CACHE) > 32:
        oldest = next(iter(_BUNDLE_CACHE))
        _BUNDLE_CACHE.pop(oldest, None)
    return bundle
