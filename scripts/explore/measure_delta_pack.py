"""Measure athar_store delta-pack storage over a real revision chain.

Ingests a sequence of IFC revisions into a throwaway ``ArtifactPack`` (each
version delta-based on its predecessor, the same shape the baseline store
produces from review->approve chains), prints per-version encoding and stored
size, verifies the final version reconstructs byte-exactly, and reports the
total stored-vs-logical ratio.

Defaults to the real Revit round-trip IFC4 chain r2..r10; pass explicit file
paths to measure any other lineage.

Usage:
  python scripts/explore/measure_delta_pack.py [files...] [--keep DIR]
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from athar_store import ArtifactPack  # noqa: E402
from scripts.build_git_demo_repo import REVIT_VERSIONS, _resolve_source  # noqa: E402


def _mb(size_bytes: int) -> str:
    return f"{size_bytes / 1_000_000:.1f} MB"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("files", nargs="*", help="Revision files in lineage order (default: Revit r2..r10)")
    parser.add_argument("--keep", help="Build the pack in this directory instead of a temp dir")
    args = parser.parse_args()

    if args.files:
        files = [Path(f).resolve() for f in args.files]
    else:
        files = [_resolve_source(rel) for rel, _ in REVIT_VERSIONS]

    with tempfile.TemporaryDirectory(prefix="athar-pack-measure-") as tmp:
        pack_dir = Path(args.keep).resolve() if args.keep else Path(tmp) / "pack"
        pack = ArtifactPack(pack_dir)

        base_sha = None
        started = time.monotonic()
        for path in files:
            t0 = time.monotonic()
            meta = pack.ingest(path, base_sha256=base_sha)
            elapsed = time.monotonic() - t0
            print(
                f"{path.name:>10}  {meta['encoding']:>10}  depth {meta['chain_depth']}  "
                f"{_mb(meta['size_bytes'])} -> {_mb(meta['stored_bytes'])}  ({elapsed:.1f}s)"
            )
            base_sha = meta["sha256"]

        head = files[-1]
        restored = pack.materialize(base_sha, Path(tmp) / "restored.ifc")
        if _sha256(restored) != _sha256(head):
            raise SystemExit(f"round-trip mismatch reconstructing {head.name}")
        print(f"\nhead round-trip: {head.name} reconstructs byte-exactly")

        stats = pack.stats()
        ratio = stats["stored_bytes"] / stats["size_bytes"]
        print(
            f"total: {stats['artifacts']} versions, {_mb(stats['size_bytes'])} logical "
            f"-> {_mb(stats['stored_bytes'])} stored ({ratio:.1%}), "
            f"encodings {stats['by_encoding']}, {time.monotonic() - started:.1f}s"
        )
        if args.keep:
            print(f"pack kept at {pack_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
