"""Fast entity fingerprinting for cross-file similarity seeding."""

from __future__ import annotations

import hashlib
from collections import Counter
from typing import Any, Protocol

try:
    from athar._native._core import native_entity_fingerprint as _NATIVE_ENTITY_FINGERPRINT
except Exception:
    _NATIVE_ENTITY_FINGERPRINT = None


class _HexHasher(Protocol):
    def update(self, data: bytes) -> None: ...

    def hexdigest(self) -> str: ...


_XXH3_128 = None
try:
    import xxhash as _xxhash  # type: ignore[import-not-found]

    _XXH3_128 = _xxhash.xxh3_128
except Exception:
    _XXH3_128 = None


def entity_text_fingerprint(entity: dict[str, Any]) -> str:
    """Fingerprint entity content while ignoring STEP ref targets."""
    if _NATIVE_ENTITY_FINGERPRINT is not None:
        return _NATIVE_ENTITY_FINGERPRINT(entity)
    return python_entity_text_fingerprint(entity)


def python_entity_text_fingerprint(entity: dict[str, Any]) -> str:
    """Pure-Python fallback for entity content fingerprinting."""
    hasher = _new_hasher()
    _hash_token(hasher, "entity_type")
    _hash_scalar(hasher, entity.get("entity_type"))

    _hash_token(hasher, "attributes")
    _hash_value_stripping_refs(hasher, entity.get("attributes", {}))

    _hash_token(hasher, "edges")
    _hash_edge_multiset(hasher, entity.get("refs", []))
    return hasher.hexdigest()


def _new_hasher() -> _HexHasher:
    if _XXH3_128 is not None:
        return _XXH3_128()
    return hashlib.blake2b(digest_size=16)


def _hash_edge_multiset(hasher: _HexHasher, refs: list[dict[str, Any]]) -> None:
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        counts[(ref.get("path", ""), ref.get("target_type"))] += 1
    hasher.update(b"E{")
    for path, target_type in sorted(counts, key=lambda item: (item[0], item[1] or "")):
        hasher.update(b"P")
        _hash_scalar(hasher, path)
        hasher.update(b"T")
        _hash_scalar(hasher, target_type)
        hasher.update(b"C")
        _hash_scalar(hasher, counts[(path, target_type)])
        hasher.update(b";")
    hasher.update(b"}")


def _hash_value_stripping_refs(hasher: _HexHasher, value: Any) -> None:
    if isinstance(value, dict):
        if value.get("kind") == "ref":
            hasher.update(b"R")
            return
        hasher.update(b"{")
        for key in sorted(value):
            hasher.update(b"K")
            _hash_scalar(hasher, key)
            hasher.update(b"V")
            _hash_value_stripping_refs(hasher, value[key])
            hasher.update(b";")
        hasher.update(b"}")
        return
    if isinstance(value, list):
        hasher.update(b"[")
        for item in value:
            _hash_value_stripping_refs(hasher, item)
            hasher.update(b",")
        hasher.update(b"]")
        return
    _hash_scalar(hasher, value)


def _hash_token(hasher: _HexHasher, token: str) -> None:
    hasher.update(b"#")
    _hash_scalar(hasher, token)


def _hash_scalar(hasher: _HexHasher, value: Any) -> None:
    if value is None:
        hasher.update(b"N")
        return
    if isinstance(value, bool):
        hasher.update(b"B1" if value else b"B0")
        return
    if isinstance(value, int):
        hasher.update(b"I")
        hasher.update(str(value).encode("ascii"))
        hasher.update(b";")
        return
    if isinstance(value, float):
        hasher.update(b"F")
        hasher.update(repr(value).encode("ascii"))
        hasher.update(b";")
        return
    text = str(value)
    encoded = text.encode("utf-8")
    hasher.update(b"S")
    hasher.update(str(len(encoded)).encode("ascii"))
    hasher.update(b":")
    hasher.update(encoded)
