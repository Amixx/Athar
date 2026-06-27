"""Optional native (Rust/PyO3) accelerator discovery for the bottom layer.

The native extension (`athar_native`, built from ``athar/_native``) ports the
two CPU-bound, pure-data signature stages — Merkle hashing and WL topology
gossip — to Rust. It is strictly optional: when the compiled module is absent
or explicitly disabled, the bottom layer runs its pure-Python implementations
and produces byte-identical signatures.

Set ``ATHAR_NO_NATIVE=1`` to force the pure-Python path (used by the parity
test and as an escape hatch).
"""

from __future__ import annotations

import os

_DISABLED = os.environ.get("ATHAR_NO_NATIVE") == "1"

try:
    if _DISABLED:
        raise ImportError("athar_native disabled via ATHAR_NO_NATIVE=1")
    import athar_native as _native  # type: ignore
except Exception:  # pragma: no cover - exercised only when the wheel is absent
    _native = None


def native() -> object | None:
    """Return the loaded native module, or ``None`` when unavailable."""
    return _native


def native_available() -> bool:
    """True when the native accelerator is importable and enabled."""
    return _native is not None
