"""Delta-compressed, content-addressed artifact storage.

Each artifact is identified by the sha256 of its exact IFC bytes. A new
artifact may be stored as a zstd ``--patch-from`` delta against a base
artifact (typically the baseline it was reviewed against), so a lineage of
large exports costs one full snapshot plus small per-revision deltas.

Reconstruction is byte-exact by construction: every delta is decoded and
sha-verified at ingest time, before the pack commits to it. A delta that
fails verification, exceeds the chain-depth bound, or saves too little space
falls back to a full zstd snapshot; when the ``zstd`` executable is missing
the pack degrades to raw copies (the pre-delta layout, which stays readable
forever). Reading compressed artifacts requires ``zstd`` again.

On-disk, per sha256:

- ``<sha>.ifc``            raw bytes (legacy layout and no-zstd fallback)
- ``<sha>.ifc.zst``        full zstd snapshot
- ``<sha>.ifc.patch.zst``  zstd delta against ``base_sha256``
- ``<sha>.meta.json``      encoding, base, chain depth, logical/stored sizes
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

MAX_CHAIN_DEPTH = 8
_DELTA_ACCEPT_RATIO = 0.5
_ZSTD_LEVEL = "-9"
_META_SUFFIX = ".meta.json"


class PackError(Exception):
    """Raised when an artifact cannot be stored or reconstructed."""


def zstd_available() -> bool:
    return shutil.which("zstd") is not None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _window_log(*sizes: int) -> int:
    return min(30, max(24, *(size.bit_length() for size in sizes)))


def _zstd(*args: str) -> None:
    result = subprocess.run(["zstd", "-q", "-f", *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise PackError(f"zstd failed ({result.returncode}): {result.stderr.strip()}")


class ArtifactPack:
    """Content-addressed artifact files with optional zstd delta chains."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    # ----- lookup ------------------------------------------------------------

    def has(self, sha256: str) -> bool:
        return self.meta(sha256) is not None

    def meta(self, sha256: str) -> dict[str, Any] | None:
        """Stored metadata for one artifact, or None if absent.

        A bare ``<sha>.ifc`` without a sidecar (pre-delta layout) is reported
        as ``raw`` so old stores keep working unchanged.
        """
        meta_path = self._meta_path(sha256)
        if meta_path.is_file():
            with open(meta_path, encoding="utf-8") as handle:
                return json.load(handle)
        raw = self._raw_path(sha256)
        if raw.is_file():
            size = raw.stat().st_size
            return {
                "sha256": sha256,
                "encoding": "raw",
                "base_sha256": None,
                "chain_depth": 0,
                "size_bytes": size,
                "stored_bytes": size,
                "window_log": None,
            }
        return None

    def list_shas(self) -> list[str]:
        if not self.directory.is_dir():
            return []
        shas = {p.name[: -len(_META_SUFFIX)] for p in self.directory.glob(f"*{_META_SUFFIX}")}
        shas.update(p.stem for p in self.directory.glob("*.ifc"))
        return sorted(shas)

    def stats(self) -> dict[str, Any]:
        metas = [meta for sha in self.list_shas() if (meta := self.meta(sha)) is not None]
        by_encoding: dict[str, int] = {}
        for meta in metas:
            by_encoding[meta["encoding"]] = by_encoding.get(meta["encoding"], 0) + 1
        return {
            "artifacts": len(metas),
            "size_bytes": sum(meta["size_bytes"] for meta in metas),
            "stored_bytes": sum(meta["stored_bytes"] for meta in metas),
            "by_encoding": dict(sorted(by_encoding.items())),
        }

    # ----- ingest ------------------------------------------------------------

    def ingest(self, src: str | Path, *, base_sha256: str | None = None) -> dict[str, Any]:
        """Store a file's bytes, preferring a delta against ``base_sha256``.

        Idempotent: identical bytes ingest once, and the stored encoding of an
        existing artifact is never rewritten.
        """
        src = Path(src)
        if not src.is_file():
            raise PackError(f"artifact source not found: {src}")
        sha256 = _sha256_file(src)
        existing = self.meta(sha256)
        if existing is not None:
            return existing
        self.directory.mkdir(parents=True, exist_ok=True)
        size = src.stat().st_size
        meta = None
        if zstd_available():
            meta = self._try_ingest_delta(src, sha256, size, base_sha256)
            if meta is None:
                meta = self._try_ingest_full(src, sha256, size)
        if meta is None:
            shutil.copyfile(src, self._raw_path(sha256))
            meta = self._meta_dict(sha256, "raw", None, 0, size, size, None)
        self._write_meta(meta)
        return meta

    def _try_ingest_delta(
        self, src: Path, sha256: str, size: int, base_sha256: str | None
    ) -> dict[str, Any] | None:
        if base_sha256 is None or base_sha256 == sha256:
            return None
        base_meta = self.meta(base_sha256)
        if base_meta is None or base_meta["chain_depth"] >= MAX_CHAIN_DEPTH:
            return None
        delta = self._delta_path(sha256)
        window_log = _window_log(base_meta["size_bytes"], size)
        try:
            with self.open(base_sha256) as base_path, tempfile.TemporaryDirectory(
                prefix="athar-pack-", dir=self.directory
            ) as tmp:
                _zstd(
                    _ZSTD_LEVEL,
                    f"--long={window_log}",
                    f"--patch-from={base_path}",
                    "-o",
                    str(delta),
                    str(src),
                )
                if delta.stat().st_size >= size * _DELTA_ACCEPT_RATIO:
                    raise PackError("delta saves too little over a full snapshot")
                verify = Path(tmp) / "verify.ifc"
                _zstd(
                    "-d",
                    f"--long={window_log}",
                    f"--patch-from={base_path}",
                    "-o",
                    str(verify),
                    str(delta),
                )
                if _sha256_file(verify) != sha256:
                    raise PackError("delta round-trip hash mismatch")
        except PackError:
            delta.unlink(missing_ok=True)
            return None
        return self._meta_dict(
            sha256,
            "zstd-delta",
            base_sha256,
            base_meta["chain_depth"] + 1,
            size,
            delta.stat().st_size,
            window_log,
        )

    def _try_ingest_full(self, src: Path, sha256: str, size: int) -> dict[str, Any] | None:
        full = self._full_path(sha256)
        try:
            _zstd(_ZSTD_LEVEL, "-o", str(full), str(src))
            _zstd("-t", str(full))
        except PackError:
            full.unlink(missing_ok=True)
            return None
        return self._meta_dict(sha256, "zstd", None, 0, size, full.stat().st_size, None)

    # ----- read --------------------------------------------------------------

    def materialize(self, sha256: str, dest: str | Path) -> Path:
        """Write the artifact's exact original bytes to ``dest``."""
        dest = Path(dest)
        meta = self.meta(sha256)
        if meta is None:
            raise PackError(f"unknown artifact {sha256}")
        encoding = meta["encoding"]
        if encoding == "raw":
            shutil.copyfile(self._raw_path(sha256), dest)
            return dest
        if not zstd_available():
            raise PackError(f"artifact {sha256} is zstd-encoded but no `zstd` executable is available")
        if encoding == "zstd":
            _zstd("-d", "-o", str(dest), str(self._full_path(sha256)))
            return dest
        if encoding == "zstd-delta":
            with self.open(meta["base_sha256"]) as base_path:
                _zstd(
                    "-d",
                    f"--long={meta['window_log']}",
                    f"--patch-from={base_path}",
                    "-o",
                    str(dest),
                    str(self._delta_path(sha256)),
                )
            return dest
        raise PackError(f"artifact {sha256} has unknown encoding {encoding!r}")

    @contextmanager
    def open(self, sha256: str) -> Iterator[Path]:
        """Yield a readable path with the artifact's exact bytes.

        Raw artifacts are yielded in place; compressed ones are materialized
        into a temporary file that is removed on exit.
        """
        meta = self.meta(sha256)
        if meta is None:
            raise PackError(f"unknown artifact {sha256}")
        if meta["encoding"] == "raw":
            yield self._raw_path(sha256)
            return
        with tempfile.TemporaryDirectory(prefix="athar-pack-") as tmp:
            yield self.materialize(sha256, Path(tmp) / f"{sha256}.ifc")

    # ----- internals ---------------------------------------------------------

    def _raw_path(self, sha256: str) -> Path:
        return self.directory / f"{sha256}.ifc"

    def _full_path(self, sha256: str) -> Path:
        return self.directory / f"{sha256}.ifc.zst"

    def _delta_path(self, sha256: str) -> Path:
        return self.directory / f"{sha256}.ifc.patch.zst"

    def _meta_path(self, sha256: str) -> Path:
        return self.directory / f"{sha256}{_META_SUFFIX}"

    @staticmethod
    def _meta_dict(
        sha256: str,
        encoding: str,
        base_sha256: str | None,
        chain_depth: int,
        size_bytes: int,
        stored_bytes: int,
        window_log: int | None,
    ) -> dict[str, Any]:
        return {
            "sha256": sha256,
            "encoding": encoding,
            "base_sha256": base_sha256,
            "chain_depth": chain_depth,
            "size_bytes": size_bytes,
            "stored_bytes": stored_bytes,
            "window_log": window_log,
        }

    def _write_meta(self, meta: dict[str, Any]) -> None:
        with open(self._meta_path(meta["sha256"]), "w", encoding="utf-8") as handle:
            json.dump(meta, handle, indent=2, sort_keys=True)
            handle.write("\n")
