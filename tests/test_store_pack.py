"""Delta-compressed artifact pack: exact round-trips, chain bounds, fallbacks.

The pack stores bytes, not IFC semantics, so most tests use deterministic
pseudo-random blobs; the store-level tests reuse the tiny synthetic IFC models
from test_store to prove the review/approve/export lineage works end-to-end on
delta-encoded artifacts.
"""

from __future__ import annotations

import hashlib
import json

import pytest

import athar_store.pack as pack_mod
from athar_store import ArtifactPack, BaselineStore, PackError
from athar_store.pack import MAX_CHAIN_DEPTH, zstd_available
from tests.test_store import _GUID_A, _GUID_B, _fixed_clock, _write_model

needs_zstd = pytest.mark.skipif(not zstd_available(), reason="zstd executable not available")


def _blob(seed: bytes, size: int) -> bytes:
    out = bytearray()
    digest = seed
    while len(out) < size:
        digest = hashlib.sha256(digest).digest()
        out += digest
    return bytes(out[:size])


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _revision(base: bytes, index: int) -> bytes:
    edited = bytearray(base)
    offset = 1000 * (index + 1)
    edited[offset : offset + 64] = _blob(index.to_bytes(4, "big"), 64)
    return bytes(edited) + _blob(b"tail" + index.to_bytes(4, "big"), 256)


@pytest.fixture
def pack(tmp_path):
    return ArtifactPack(tmp_path / "artifacts")


@pytest.fixture
def base_bytes():
    return _blob(b"base", 300_000)


def _ingest_bytes(pack, tmp_path, data: bytes, name: str, base_sha256=None):
    src = tmp_path / name
    src.write_bytes(data)
    return pack.ingest(src, base_sha256=base_sha256)


@needs_zstd
def test_delta_roundtrip_is_byte_exact_and_small(pack, tmp_path, base_bytes):
    rev = _revision(base_bytes, 0)
    base_meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    rev_meta = _ingest_bytes(pack, tmp_path, rev, "rev.bin", base_sha256=base_meta["sha256"])

    assert rev_meta["encoding"] == "zstd-delta"
    assert rev_meta["base_sha256"] == base_meta["sha256"]
    assert rev_meta["chain_depth"] == 1
    assert rev_meta["size_bytes"] == len(rev)
    # A near-identical revision must cost a small fraction of a full copy.
    assert rev_meta["stored_bytes"] < len(rev) * 0.2

    out = pack.materialize(rev_meta["sha256"], tmp_path / "out.bin")
    assert out.read_bytes() == rev


@needs_zstd
def test_ingest_is_idempotent_and_preserves_encoding(pack, tmp_path, base_bytes):
    first = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    again = _ingest_bytes(pack, tmp_path, base_bytes, "copy.bin", base_sha256="0" * 64)
    assert again == first
    assert pack.stats()["artifacts"] == 1


@needs_zstd
def test_chain_depth_is_bounded_by_full_snapshots(pack, tmp_path, base_bytes):
    versions = [base_bytes]
    for index in range(MAX_CHAIN_DEPTH + 2):
        versions.append(_revision(versions[-1], index))

    metas = [_ingest_bytes(pack, tmp_path, versions[0], "v0.bin")]
    for index, data in enumerate(versions[1:], start=1):
        metas.append(
            _ingest_bytes(pack, tmp_path, data, f"v{index}.bin", base_sha256=metas[-1]["sha256"])
        )

    assert max(meta["chain_depth"] for meta in metas) == MAX_CHAIN_DEPTH
    assert metas[MAX_CHAIN_DEPTH + 1]["encoding"] == "zstd"
    assert metas[MAX_CHAIN_DEPTH + 1]["chain_depth"] == 0

    deepest = metas[MAX_CHAIN_DEPTH]
    out = pack.materialize(deepest["sha256"], tmp_path / "deep.bin")
    assert out.read_bytes() == versions[MAX_CHAIN_DEPTH]


@needs_zstd
def test_unrelated_content_falls_back_to_full_snapshot(pack, tmp_path, base_bytes):
    base_meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    unrelated = _blob(b"unrelated", 300_000)
    meta = _ingest_bytes(pack, tmp_path, unrelated, "other.bin", base_sha256=base_meta["sha256"])
    assert meta["encoding"] == "zstd"
    out = pack.materialize(meta["sha256"], tmp_path / "out.bin")
    assert out.read_bytes() == unrelated


def test_without_zstd_pack_degrades_to_raw(pack, tmp_path, base_bytes, monkeypatch):
    monkeypatch.setattr(pack_mod, "zstd_available", lambda: False)
    meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    assert meta["encoding"] == "raw"
    assert (pack.directory / f"{meta['sha256']}.ifc").is_file()
    with pack.open(meta["sha256"]) as path:
        assert path.read_bytes() == base_bytes


def test_legacy_raw_layout_is_still_readable(pack, tmp_path, base_bytes):
    sha = _sha(base_bytes)
    pack.directory.mkdir(parents=True)
    (pack.directory / f"{sha}.ifc").write_bytes(base_bytes)

    meta = pack.meta(sha)
    assert meta is not None and meta["encoding"] == "raw"
    with pack.open(sha) as path:
        assert path.read_bytes() == base_bytes
    assert _ingest_bytes(pack, tmp_path, base_bytes, "same.bin")["encoding"] == "raw"
    assert pack.stats()["artifacts"] == 1


def test_unknown_artifact_raises(pack, tmp_path):
    with pytest.raises(PackError):
        pack.materialize("f" * 64, tmp_path / "out.bin")


@needs_zstd
def test_stats_report_logical_and_stored_bytes(pack, tmp_path, base_bytes):
    base_meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    rev = _revision(base_bytes, 0)
    _ingest_bytes(pack, tmp_path, rev, "rev.bin", base_sha256=base_meta["sha256"])

    stats = pack.stats()
    assert stats["artifacts"] == 2
    assert stats["size_bytes"] == len(base_bytes) + len(rev)
    assert 0 < stats["stored_bytes"] < stats["size_bytes"] + len(rev)
    assert stats["by_encoding"] == {"zstd": 1, "zstd-delta": 1}


@needs_zstd
def test_store_lineage_uses_deltas_and_exports_exact_bytes(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(candidate, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])

    store = BaselineStore(tmp_path / "store", clock=_fixed_clock())
    store.set_baseline("acme", "architecture", baseline, actor="alice")
    review = store.review("acme", "architecture", candidate, actor="bob")

    candidate_artifact = review["candidate_artifact"]
    assert candidate_artifact["encoding"] in ("zstd-delta", "zstd")
    meta_path = (
        store.root
        / "projects"
        / "acme"
        / "artifacts"
        / f"{candidate_artifact['sha256']}.meta.json"
    )
    assert json.loads(meta_path.read_text())["sha256"] == candidate_artifact["sha256"]

    store.approve("acme", "architecture", review["id"], actor="carol")
    exported = store.export_artifact(
        "acme", candidate_artifact["sha256"], tmp_path / "exported.ifc"
    )
    assert exported.read_bytes() == candidate.read_bytes()

    stats = store.artifact_stats("acme")
    assert stats["artifacts"] == 2
    assert stats["stored_bytes"] > 0
