"""Delta-compressed, content-addressed artifact storage.

Each artifact is identified by the sha256 of its exact IFC bytes. A new
artifact may be stored as a zstd ``--patch-from`` delta against a base
artifact (typically the baseline it was reviewed against), so a lineage of
large exports costs one full snapshot plus small per-revision deltas.

Reconstruction is byte-exact by construction: every encoding is decoded and
sha-verified at ingest time, before the pack commits to it. A delta that
saves too little space or exceeds the chain-depth bound is stored as a full
zstd snapshot. Verification and compression failures raise PackError.
Writing artifacts and reading compressed ones require the external ``zstd``
executable (1.5+). Raw artifacts written by release 0.1.0 remain readable.

On-disk, per sha256:

- ``<sha>.ifc``            release 0.1.0 raw bytes without a sidecar
- ``<sha>.ifc.zst``        full zstd snapshot
- ``<sha>.ifc.patch.zst``  zstd delta against ``base_sha256``
- ``<sha>.meta.json``      encoding, base, chain depth, logical/stored sizes
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from ._io import atomic_file, mkdir_durable

PACK_FORMAT = 1
MAX_CHAIN_DEPTH = 8
_DELTA_ACCEPT_RATIO = 0.5
_ZSTD_LEVEL = "-9"
_META_SUFFIX = ".meta.json"


class PackError(Exception):
    """Raised when an artifact cannot be stored or reconstructed."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _scratch_path(directory: Path) -> Path:
    descriptor, name = tempfile.mkstemp(prefix=".athar-pack-", dir=directory)
    os.close(descriptor)
    return Path(name)


def _new_file_mode() -> int:
    # The umask can only be read by setting it; 0o077 is in force for that instant.
    umask = os.umask(0o077)
    os.umask(umask)
    return 0o666 & ~umask


def _window_log(*sizes: int) -> int:
    return min(30, max(24, *(size.bit_length() for size in sizes)))


def _zstd(*args: str) -> None:
    try:
        result = subprocess.run(["zstd", "-q", "-f", *args], capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise PackError(
            "Store packing requires the zstd executable (1.5+); "
            "install with `brew install zstd` or `apt install zstd`."
        ) from exc
    if result.returncode != 0:
        raise PackError(f"zstd failed ({result.returncode}): {result.stderr.strip()}")


class ArtifactPack:
    """Content-addressed artifact files with optional zstd delta chains."""

    def __init__(
        self, directory: str | Path, *, scratch_path: Callable[[Path], Path] = _scratch_path
    ) -> None:
        self.directory = Path(directory)
        self._scratch_path = scratch_path

    # ----- lookup ------------------------------------------------------------

    def has(self, sha256: str) -> bool:
        return self.meta(sha256) is not None

    def meta(self, sha256: str) -> dict[str, Any] | None:
        """Stored metadata for one artifact, or None if absent.

        A bare ``<sha>.ifc`` without a sidecar (release 0.1.0) is reported as
        ``raw``.
        """
        meta_path = self._meta_path(sha256)
        if meta_path.is_file():
            with open(meta_path, encoding="utf-8") as handle:
                meta = json.load(handle)
            if meta.get("format") != PACK_FORMAT:
                raise PackError(
                    f"artifact {sha256} has pack format {meta.get('format')!r}; "
                    f"this Athar reads format {PACK_FORMAT}"
                )
            encoding = meta.get("encoding")
            depth = meta.get("chain_depth")
            if (
                meta.get("sha256") != sha256
                or encoding not in ("zstd", "zstd-delta")
                or type(depth) is not int
                or not 0 <= depth <= MAX_CHAIN_DEPTH
                or (encoding == "zstd" and (depth != 0 or meta.get("base_sha256") is not None))
                or (encoding == "zstd-delta" and (depth == 0 or not isinstance(meta.get("base_sha256"), str)))
            ):
                raise PackError(f"artifact {sha256} has invalid pack metadata")
            return meta
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
            with self.open(sha256):
                pass
            return existing
        mkdir_durable(self.directory)
        size = src.stat().st_size
        meta = self._try_ingest_delta(src, sha256, size, base_sha256)
        if meta is None:
            meta = self._ingest_full(src, sha256, size)
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
        with self.open(base_sha256) as base_path, tempfile.TemporaryDirectory(
            prefix="athar-pack-", dir=self.directory
        ) as tmp:
            candidate = Path(tmp) / "delta.zst"
            _zstd(
                _ZSTD_LEVEL,
                f"--long={window_log}",
                f"--patch-from={base_path}",
                "-o",
                str(candidate),
                str(src),
            )
            if candidate.stat().st_size >= size * _DELTA_ACCEPT_RATIO:
                return None
            verify = Path(tmp) / "verify.ifc"
            _zstd(
                "-d",
                f"--long={window_log}",
                f"--patch-from={base_path}",
                "-o",
                str(verify),
                str(candidate),
            )
            if _sha256_file(verify) != sha256:
                raise PackError("delta round-trip hash mismatch")
            with atomic_file(delta) as temporary:
                os.replace(candidate, temporary)
        return self._meta_dict(
            sha256,
            "zstd-delta",
            base_sha256,
            base_meta["chain_depth"] + 1,
            size,
            delta.stat().st_size,
            window_log,
        )

    def _ingest_full(self, src: Path, sha256: str, size: int) -> dict[str, Any]:
        full = self._full_path(sha256)
        with atomic_file(full) as temporary:
            _zstd(_ZSTD_LEVEL, "-o", str(temporary), str(src))
            verify = self._scratch_path(self.directory)
            try:
                _zstd("-d", "-o", str(verify), str(temporary))
                if _sha256_file(verify) != sha256:
                    raise PackError("full snapshot round-trip hash mismatch")
            finally:
                verify.unlink(missing_ok=True)
        return self._meta_dict(sha256, "zstd", None, 0, size, full.stat().st_size, None)

    # ----- read --------------------------------------------------------------

    def materialize(self, sha256: str, dest: str | Path) -> Path:
        """Write the artifact's exact original bytes to ``dest``."""
        dest = Path(dest)
        meta = self.meta(sha256)
        if meta is None:
            raise PackError(f"unknown artifact {sha256}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        chain: list[dict[str, Any]] = []
        visited = {sha256}
        root = meta
        while root["encoding"] == "zstd-delta":
            chain.append(root)
            base = root["base_sha256"]
            if base in visited or len(chain) > MAX_CHAIN_DEPTH:
                raise PackError(f"artifact {sha256} has a cyclic or over-depth delta chain")
            visited.add(base)
            parent = self.meta(base)
            if parent is None:
                raise PackError(f"artifact {sha256} is missing delta base {base}")
            if root["chain_depth"] != parent["chain_depth"] + 1:
                raise PackError(f"artifact {sha256} has inconsistent chain depths")
            root = parent
        scratch = (self._scratch_path(dest.parent), self._scratch_path(dest.parent))
        try:
            active = scratch[0]
            if root["encoding"] == "raw":
                shutil.copyfile(self._raw_path(root["sha256"]), active)
            else:
                _zstd("-d", "-o", str(active), str(self._full_path(root["sha256"])))
            if _sha256_file(active) != root["sha256"]:
                raise PackError(f"artifact {root['sha256']} failed sha256 verification")
            for link in reversed(chain):
                output = scratch[1] if active == scratch[0] else scratch[0]
                _zstd(
                    "-d",
                    f"--long={link['window_log']}",
                    f"--patch-from={active}",
                    "-o", str(output),
                    str(self._delta_path(link["sha256"])),
                )
                if _sha256_file(output) != link["sha256"]:
                    raise PackError(f"artifact {link['sha256']} failed sha256 verification")
                active = output
            os.chmod(active, _new_file_mode())
            os.replace(active, dest)
        finally:
            for path in scratch:
                path.unlink(missing_ok=True)
        return dest

    @contextmanager
    def open(self, sha256: str) -> Iterator[Path]:
        """Yield a verified path with the artifact's exact bytes."""
        meta = self.meta(sha256)
        if meta is None:
            raise PackError(f"unknown artifact {sha256}")
        if meta["encoding"] == "raw":
            if _sha256_file(self._raw_path(sha256)) != sha256:
                raise PackError(f"artifact {sha256} failed sha256 verification")
            yield self._raw_path(sha256)
            return
        with tempfile.TemporaryDirectory(prefix="athar-pack-", dir=self.directory) as tmp:
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
            "format": PACK_FORMAT,
            "sha256": sha256,
            "encoding": encoding,
            "base_sha256": base_sha256,
            "chain_depth": chain_depth,
            "size_bytes": size_bytes,
            "stored_bytes": stored_bytes,
            "window_log": window_log,
        }

    def _write_meta(self, meta: dict[str, Any]) -> None:
        with atomic_file(self._meta_path(meta["sha256"])) as temporary:
            temporary.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
