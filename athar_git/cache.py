"""Persistent signature-bundle cache for Git-oriented diffs."""

from __future__ import annotations

import hashlib
import os
import pickle
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from athar.bottom.constants import CANON_VERSION
from athar.bottom.signatures import build_signature_bundle
from athar.bottom.types import SignatureBundle
from athar_git import __version__ as ATHAR_GIT_VERSION

CACHE_FORMAT_VERSION = 1


@dataclass(frozen=True)
class CacheResult:
    bundle: SignatureBundle
    key: str
    hit: bool
    path: Path


def default_cache_dir() -> Path:
    root = os.environ.get("ATHAR_CACHE_DIR")
    if root:
        return Path(root)
    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / "athar" / "signatures"
    return Path.home() / ".cache" / "athar" / "signatures"


def load_or_build_bundle(
    path: str | os.PathLike[str],
    *,
    key: str | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
) -> CacheResult:
    """Load a signature bundle from persistent cache or build and store it."""

    source_path = Path(path)
    cache_key = _cache_key(source_path, key)
    directory = Path(cache_dir) if cache_dir is not None else default_cache_dir()
    entry_path = _entry_path(directory, cache_key)

    cached = _read_entry(entry_path)
    if cached is not None:
        return CacheResult(bundle=cached, key=cache_key, hit=True, path=entry_path)

    bundle = build_signature_bundle(str(source_path))
    _write_entry(entry_path, bundle, cache_key)
    return CacheResult(bundle=bundle, key=cache_key, hit=False, path=entry_path)


def _cache_key(path: Path, supplied: str | None) -> str:
    normalized = (supplied or "").strip().lower()
    if _is_git_blob_oid(normalized):
        return f"git-{normalized}"
    return f"sha256-{_file_sha256(path)}"


def _is_git_blob_oid(value: str) -> bool:
    if len(value) not in (40, 64):
        return False
    if set(value) == {"0"}:
        return False
    return all(char in "0123456789abcdef" for char in value)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _entry_path(cache_dir: Path, key: str) -> Path:
    return cache_dir / CANON_VERSION / f"{key}.pickle"


def _read_entry(path: Path) -> SignatureBundle | None:
    try:
        with path.open("rb") as handle:
            payload: Any = pickle.load(handle)
    except (FileNotFoundError, OSError, pickle.PickleError, EOFError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("format_version") != CACHE_FORMAT_VERSION:
        return None
    if payload.get("canon_version") != CANON_VERSION:
        return None
    if payload.get("producer_version") != ATHAR_GIT_VERSION:
        return None
    bundle = payload.get("bundle")
    if not isinstance(bundle, SignatureBundle):
        return None
    if bundle.canon_version != CANON_VERSION:
        return None
    return bundle


def _write_entry(path: Path, bundle: SignatureBundle, key: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format_version": CACHE_FORMAT_VERSION,
        "canon_version": CANON_VERSION,
        "producer_version": ATHAR_GIT_VERSION,
        "key": key,
        "schema": bundle.schema,
        "bundle": bundle,
    }
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
