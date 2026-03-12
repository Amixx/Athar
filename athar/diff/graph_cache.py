"""Binary cache for parsed GraphIR and precomputed identity state.

Caches the expensive parse + identity computation results keyed by file
content hash + profile.  Cache location: ``~/.cache/athar/`` (or
``$ATHAR_CACHE_DIR`` if set).  Content-hash keys mean the cache is
naturally invalidated when the file changes.

Serialization: pickle (internal-only, never used as a wire format).
"""

from __future__ import annotations

import os
import pickle
import time
from pathlib import Path
from typing import Any

import xxhash

from .types import GraphIR

_CACHE_VERSION = 1
_CACHE_DIR_ENV = "ATHAR_CACHE_DIR"
_CACHE_ENABLED_ENV = "ATHAR_CACHE"  # "0" to disable
_DEFAULT_CACHE_DIR = Path.home() / ".cache" / "athar"
_MAX_CACHE_ENTRIES = 64


def _cache_dir() -> Path:
    return Path(os.environ.get(_CACHE_DIR_ENV, str(_DEFAULT_CACHE_DIR)))


def _cache_enabled() -> bool:
    return os.environ.get(_CACHE_ENABLED_ENV, "1") != "0"


def content_hash(filepath: str) -> str | None:
    """Compute xxh3_128 hex digest of file bytes, or None on error."""
    try:
        h = xxhash.xxh3_128()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(1 << 20)  # 1 MiB
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _cache_key(file_hash: str, profile: str) -> str:
    return f"v{_CACHE_VERSION}_{file_hash}_{profile}"


def _cache_path(file_hash: str, profile: str) -> Path:
    return _cache_dir() / f"{_cache_key(file_hash, profile)}.pkl"


def load_cached(
    file_hash: str | None,
    profile: str,
) -> dict[str, Any] | None:
    """Load cached graph + identity state, or None on miss/error."""
    if file_hash is None or not _cache_enabled():
        return None
    path = _cache_path(file_hash, profile)
    if not path.exists():
        return None
    try:
        with open(path, "rb") as f:
            entry = pickle.load(f)
        if not isinstance(entry, dict):
            return None
        if entry.get("cache_version") != _CACHE_VERSION:
            path.unlink(missing_ok=True)
            return None
        return entry
    except Exception:
        # Corrupted cache — remove and continue
        path.unlink(missing_ok=True)
        return None


def save_cached(
    file_hash: str | None,
    profile: str,
    *,
    graph: GraphIR,
    identity_state: dict[str, Any],
) -> None:
    """Persist graph + identity state to disk cache."""
    if file_hash is None or not _cache_enabled():
        return
    cache_d = _cache_dir()
    try:
        cache_d.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    entry = {
        "cache_version": _CACHE_VERSION,
        "profile": profile,
        "file_hash": file_hash,
        "graph": graph,
        "identity_state": _serializable_identity_state(identity_state),
        "created_at": time.time(),
    }
    path = _cache_path(file_hash, profile)
    tmp_path = path.with_suffix(".tmp")
    try:
        with open(tmp_path, "wb") as f:
            pickle.dump(entry, f, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_path.rename(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        return
    _evict_if_needed(cache_d)


def _serializable_identity_state(state: dict[str, Any]) -> dict[str, Any]:
    """Extract the cacheable subset of precomputed identity state.

    We exclude the raw ``entities`` and ``id_entities`` references
    since those are reconstructable from the cached GraphIR.
    """
    return {
        k: v
        for k, v in state.items()
        if k not in ("entities", "id_entities")
    }


def restore_identity_state(
    cached: dict[str, Any],
    graph: GraphIR,
    *,
    profile: str,
) -> dict[str, Any]:
    """Reconstruct full precomputed identity state from cache + live graph.

    Re-attaches the ``entities`` and ``id_entities`` references that
    were stripped during serialization.
    """
    from ..graph.profile_policy import entity_for_profile

    identity = dict(cached["identity_state"])
    entities = graph.get("entities", {})
    identity["entities"] = entities

    # Reconstruct id_entities (profile-filtered view)
    if profile == "semantic_stable":
        id_entities = {
            step_id: entity_for_profile(entity, profile=profile)
            for step_id, entity in entities.items()
        }
    else:
        id_entities = entities
    identity["id_entities"] = id_entities
    return identity


def clear_cache() -> int:
    """Remove all cache files. Returns count of files removed."""
    cache_d = _cache_dir()
    if not cache_d.exists():
        return 0
    count = 0
    for p in cache_d.glob("v*_*.pkl"):
        p.unlink(missing_ok=True)
        count += 1
    return count


def _evict_if_needed(cache_d: Path) -> None:
    """Simple LRU eviction: keep at most _MAX_CACHE_ENTRIES files."""
    entries = sorted(cache_d.glob("v*_*.pkl"), key=lambda p: p.stat().st_mtime)
    while len(entries) > _MAX_CACHE_ENTRIES:
        oldest = entries.pop(0)
        oldest.unlink(missing_ok=True)
