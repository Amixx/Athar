"""Native (Rust/PyO3) signature pipeline discovery for the bottom layer.

The native extension (`athar_native`, built from ``athar/_native``) is the
bottom layer: it tokenizes the STEP file and runs the whole parse →
canonicalize → edges → Merkle → WL → spatial pipeline in Rust, returning only
product/spatial signatures. There is no pure-Python fallback — the native
module is required and is the sole source of the canonical form.

Build it with ``make native-build``; when it is missing, ``native()`` returns
``None`` and ``build_signature_bundle`` raises a clear error.
"""

from __future__ import annotations

try:
    import athar_native as _native  # type: ignore
except Exception:  # pragma: no cover - exercised only when the wheel is absent
    _native = None


def native() -> object | None:
    """Return the loaded native module, or ``None`` when it is not built."""
    return _native
