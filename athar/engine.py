"""Current Athar engine entrypoints."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Iterator

from athar.bottom.constants import CANON_VERSION
from athar.bottom.signatures import build_signature_bundle
from athar.bottom.types import SignatureBundle
from athar.delta.report import build_delta_report
from athar.matcher.core import match_signatures

_BUNDLE_CACHE: dict[tuple[str, int, int], object] = {}


class SchemaMismatchError(ValueError):
    """Raised when the two inputs use different IFC schema versions.

    Athar compares files of the same schema only. Cross-schema comparison
    (e.g. IFC2X3 vs IFC4) is intentionally unsupported: the engine never
    translates between schema versions, so diffing across them would emit a
    flood of false add/delete churn rather than real changes. Callers should
    surface this as a clear "schema changed, cannot semantically diff" result.

    It subclasses ``ValueError`` so existing ``except ValueError`` / ``except
    Exception`` handlers keep working; new callers can catch it specifically
    and read the structured ``old_schema`` / ``new_schema`` fields.
    """

    def __init__(self, old_schema: str, new_schema: str) -> None:
        self.old_schema = old_schema
        self.new_schema = new_schema
        super().__init__(
            "Schema mismatch: cannot semantically diff across IFC schema "
            f"versions (old={old_schema}, new={new_schema}). Athar compares "
            "files of the same schema only."
        )


def diff_files(old_path: str, new_path: str, *, generated_at: str | None = None) -> dict:
    """Diff two IFC files with the current architecture."""

    old_bundle = _load_bundle(old_path)
    if old_path == new_path:
        new_bundle = old_bundle
    else:
        new_bundle = _load_bundle(new_path)
    return diff_bundles(old_bundle, new_bundle, generated_at=generated_at)


def diff_bundles(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    *,
    generated_at: str | None = None,
) -> dict:
    """Diff two pre-built signature bundles."""

    _assert_schema_compatible(old_bundle.schema, new_bundle.schema)

    matches, unmatched_old, unmatched_new, matcher_diagnostics = match_signatures(
        old_bundle.signatures,
        new_bundle.signatures,
    )
    report = build_delta_report(old_bundle, new_bundle, matches, unmatched_old, unmatched_new)
    report["stats"]["guid_collisions"] = {
        "old": _guid_collision_count(old_bundle.signatures),
        "new": _guid_collision_count(new_bundle.signatures),
    }
    report["stats"]["matcher_diagnostics"] = matcher_diagnostics
    report["canon_version"] = CANON_VERSION
    report["audit"] = _audit_metadata(old_bundle, new_bundle, generated_at=generated_at)
    return report


def stream_diff_files(
    old_path: str,
    new_path: str,
    *,
    mode: str = "ndjson",
    chunk_size: int = 1000,
    generated_at: str | None = None,
) -> Iterator[str]:
    """Stream diff output as NDJSON or chunked JSON."""
    report = diff_files(old_path, new_path, generated_at=generated_at)
    if mode == "ndjson":
        yield from _stream_ndjson(report)
        return
    if mode == "chunked_json":
        yield from _stream_chunked_json(report, chunk_size=max(1, int(chunk_size)))
        return
    raise ValueError(f"Unsupported stream mode: {mode!r}")


def _stream_ndjson(report: dict) -> Iterator[str]:
    yield json.dumps(
        {
            "record_type": "header",
            "engine": report.get("engine"),
            "canon_version": report.get("canon_version"),
            "audit": report.get("audit"),
        },
        sort_keys=True,
    )
    for section in ("added", "deleted", "modified", "unchanged"):
        for item in report.get(section, []):
            yield json.dumps({"record_type": section, "item": item}, sort_keys=True)
    yield json.dumps({"record_type": "end", "stats": report.get("stats", {})}, sort_keys=True)


def _stream_chunked_json(report: dict, *, chunk_size: int) -> Iterator[str]:
    yield json.dumps(
        {
            "chunk_type": "header",
            "engine": report.get("engine"),
            "canon_version": report.get("canon_version"),
            "audit": report.get("audit"),
        },
        sort_keys=True,
    )
    for section in ("added", "deleted", "modified", "unchanged"):
        rows = report.get(section, [])
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            yield json.dumps({"chunk_type": section, "items": chunk}, sort_keys=True)
    yield json.dumps({"chunk_type": "end", "stats": report.get("stats", {})}, sort_keys=True)


def _assert_schema_compatible(old_schema: str, new_schema: str) -> None:
    if old_schema != new_schema:
        raise SchemaMismatchError(old_schema, new_schema)


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


def generated_at_now_utc() -> str:
    """Return an archive timestamp for callers that opt into non-determinism."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _audit_metadata(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    *,
    generated_at: str | None,
) -> dict:
    audit = {
        "report_schema_version": 1,
        "producer": {
            "name": "athar",
            "version": _package_version(),
            "canon_version": CANON_VERSION,
        },
        "inputs": {
            "old": _input_metadata(old_bundle),
            "new": _input_metadata(new_bundle),
        },
    }
    if generated_at is not None:
        audit["generated_at"] = generated_at
    return audit


def _input_metadata(bundle: SignatureBundle) -> dict:
    metadata = {
        "path": bundle.filepath,
        "schema": bundle.schema,
    }
    try:
        stat = os.stat(bundle.filepath)
    except OSError:
        return metadata
    metadata["size_bytes"] = stat.st_size
    metadata["sha256"] = _file_sha256(bundle.filepath)
    return metadata


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _package_version() -> str:
    try:
        return metadata.version("athar")
    except metadata.PackageNotFoundError:
        return _pyproject_version()


def _pyproject_version() -> str:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    try:
        lines = pyproject.read_text(encoding="utf-8").splitlines()
    except OSError:
        return "unknown"
    in_project = False
    for line in lines:
        stripped = line.strip()
        if stripped == "[project]":
            in_project = True
            continue
        if in_project and stripped.startswith("["):
            return "unknown"
        if in_project and stripped.startswith("version"):
            _, _, value = stripped.partition("=")
            return value.strip().strip('"') or "unknown"
    return "unknown"
