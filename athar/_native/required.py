"""Required native entrypoints with a clear installation error."""

from __future__ import annotations

try:
    from ._core import native_build_adjacency_maps
    from ._core import native_entity_fingerprint
    from ._core import native_wl_refine
except ImportError as exc:  # pragma: no cover - exercised only in broken installs.
    raise RuntimeError(
        "Athar requires the `athar._native._core` extension. "
        "Build/install it with `make native-dev` or "
        "`maturin develop --manifest-path athar/_native/Cargo.toml`."
    ) from exc

__all__ = [
    "native_build_adjacency_maps",
    "native_entity_fingerprint",
    "native_wl_refine",
]
