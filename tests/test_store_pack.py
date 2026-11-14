"""Delta-compressed artifact pack: exact round-trips, chain bounds, verification.

The pack stores bytes, not IFC semantics, so most tests use deterministic
pseudo-random blobs; the store-level tests reuse the tiny synthetic IFC models
from test_store to prove the review/approve/export lineage works end-to-end on
delta-encoded artifacts.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from athar_store import ArtifactPack, BaselineStore, PackError
from athar_store.pack import MAX_CHAIN_DEPTH
from tests.test_store import _GUID_A, _GUID_B, _fixed_clock, _write_model


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


def test_ingest_is_idempotent_and_preserves_encoding(pack, tmp_path, base_bytes):
    first = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    again = _ingest_bytes(pack, tmp_path, base_bytes, "copy.bin", base_sha256="0" * 64)
    assert again == first
    assert pack.stats()["artifacts"] == 1


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


def test_unrelated_content_falls_back_to_full_snapshot(pack, tmp_path, base_bytes):
    base_meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    unrelated = _blob(b"unrelated", 300_000)
    meta = _ingest_bytes(pack, tmp_path, unrelated, "other.bin", base_sha256=base_meta["sha256"])
    assert meta["encoding"] == "zstd"
    assert sorted(path.name for path in pack.directory.iterdir()) == sorted(
        f"{sha}{suffix}"
        for sha in (base_meta["sha256"], meta["sha256"])
        for suffix in (".ifc.zst", ".meta.json")
    )
    out = pack.materialize(meta["sha256"], tmp_path / "out.bin")
    assert out.read_bytes() == unrelated


def test_missing_zstd_is_an_error_with_install_hint(pack, tmp_path, base_bytes, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
    with pytest.raises(PackError, match="requires the zstd executable"):
        _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    assert [path.name for path in pack.directory.iterdir()] == []


def test_source_changed_between_hash_and_compress_is_refused(
    pack, tmp_path, base_bytes, monkeypatch
):
    real_zstd = shutil.which("zstd")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "zstd"
    wrapper.write_text(
        "#!/bin/sh\n"
        'case " $* " in *" -d "*|*" -t "*) ;; *) for last; do :; done; printf changed >> "$last";; esac\n'
        f'exec "{real_zstd}" "$@"\n'
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    with pytest.raises(PackError, match="round-trip hash mismatch"):
        _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    assert pack.list_shas() == []


def test_raw_sidecar_is_not_a_pack_encoding(pack, tmp_path, base_bytes):
    meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    sidecar = pack.directory / f"{meta['sha256']}.meta.json"
    stored = json.loads(sidecar.read_text())
    stored.update(encoding="raw")
    sidecar.write_text(json.dumps(stored))
    (pack.directory / f"{meta['sha256']}.ifc").write_bytes(base_bytes)
    with pytest.raises(PackError, match="invalid pack metadata"):
        pack.materialize(meta["sha256"], tmp_path / "out.bin")


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


@pytest.mark.parametrize("damage", ["cycle", "depth", "identity"])
def test_corrupt_chain_metadata_is_refused(pack, tmp_path, base_bytes, damage):
    base = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    revision = _ingest_bytes(
        pack, tmp_path, _revision(base_bytes, 0), "rev.bin", base_sha256=base["sha256"]
    )
    sidecar = pack.directory / f"{revision['sha256']}.meta.json"
    meta = json.loads(sidecar.read_text())
    if damage == "cycle":
        meta["base_sha256"] = revision["sha256"]
    elif damage == "depth":
        meta["chain_depth"] = MAX_CHAIN_DEPTH + 1
    else:
        meta["sha256"] = base["sha256"]
    sidecar.write_text(json.dumps(meta))
    with pytest.raises(PackError, match="cyclic|invalid pack metadata"):
        pack.materialize(revision["sha256"], tmp_path / "out.bin")


def test_truncated_delta_at_odd_chain_position_is_pack_error(pack, tmp_path, base_bytes):
    first = _revision(base_bytes, 0)
    metas = [_ingest_bytes(pack, tmp_path, base_bytes, "v0.bin")]
    for index, data in enumerate((first, _revision(first, 1))):
        metas.append(
            _ingest_bytes(pack, tmp_path, data, f"v{index + 1}.bin", base_sha256=metas[-1]["sha256"])
        )
    assert [meta["chain_depth"] for meta in metas] == [0, 1, 2]
    delta = pack.directory / f"{metas[1]['sha256']}.ifc.patch.zst"
    delta.write_bytes(delta.read_bytes()[: delta.stat().st_size // 2])
    out = tmp_path / "out" / "deep.bin"
    with pytest.raises(PackError):
        pack.materialize(metas[2]["sha256"], out)
    assert [path.name for path in out.parent.iterdir() if path.name.startswith(".athar-pack-")] == []


@pytest.mark.parametrize("damaged_depth", [1, 2])
def test_failed_materialization_leaves_destination_untouched(
    pack, tmp_path, base_bytes, damaged_depth
):
    first = _revision(base_bytes, 0)
    metas = [_ingest_bytes(pack, tmp_path, base_bytes, "v0.bin")]
    for index, data in enumerate((first, _revision(first, 1))):
        metas.append(
            _ingest_bytes(pack, tmp_path, data, f"v{index + 1}.bin", base_sha256=metas[-1]["sha256"])
        )
    delta = pack.directory / f"{metas[damaged_depth]['sha256']}.ifc.patch.zst"
    delta.write_bytes(delta.read_bytes()[: delta.stat().st_size // 2])
    out = tmp_path / "out" / "export.ifc"
    out.parent.mkdir()
    out.write_bytes(b"previous export")
    with pytest.raises(PackError):
        pack.materialize(metas[damaged_depth]["sha256"], out)
    assert out.read_bytes() == b"previous export"
    assert [path.name for path in out.parent.iterdir()] == ["export.ifc"]


def test_corrupt_release_raw_artifact_is_refused(pack, tmp_path, base_bytes):
    sha = _sha(base_bytes)
    pack.directory.mkdir()
    (pack.directory / f"{sha}.ifc").write_bytes(b"truncated")
    with pytest.raises(PackError, match="sha256 verification"):
        with pack.open(sha):
            pytest.fail("corrupt bytes must not be yielded")
    with pytest.raises(PackError, match="sha256 verification"):
        pack.materialize(sha, tmp_path / "out.ifc")


def test_unknown_artifact_raises(pack, tmp_path):
    with pytest.raises(PackError):
        pack.materialize("f" * 64, tmp_path / "out.bin")


def test_stats_report_logical_and_stored_bytes(pack, tmp_path, base_bytes):
    base_meta = _ingest_bytes(pack, tmp_path, base_bytes, "base.bin")
    rev = _revision(base_bytes, 0)
    _ingest_bytes(pack, tmp_path, rev, "rev.bin", base_sha256=base_meta["sha256"])

    stats = pack.stats()
    assert stats["artifacts"] == 2
    assert stats["size_bytes"] == len(base_bytes) + len(rev)
    assert 0 < stats["stored_bytes"] < stats["size_bytes"] + len(rev)
    assert stats["by_encoding"] == {"zstd": 1, "zstd-delta": 1}


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


def test_deep_chain_materializes_through_two_files(tmp_path, base_bytes):
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    created: list[Path] = []

    def scratch_path(directory):
        descriptor, name = tempfile.mkstemp(dir=scratch_dir)
        os.close(descriptor)
        created.append(Path(name))
        return created[-1]

    pack = ArtifactPack(tmp_path / "artifacts", scratch_path=scratch_path)
    versions = [base_bytes]
    metas = [_ingest_bytes(pack, tmp_path, base_bytes, "v0.bin")]
    for index in range(MAX_CHAIN_DEPTH):
        versions.append(_revision(versions[-1], index))
        metas.append(
            _ingest_bytes(
                pack, tmp_path, versions[-1], f"v{index + 1}.bin", base_sha256=metas[-1]["sha256"]
            )
        )
    assert metas[-1]["chain_depth"] == MAX_CHAIN_DEPTH
    created.clear()
    dest = tmp_path / "deep.bin"
    pack.materialize(metas[-1]["sha256"], dest)
    assert len(created) == 2
    assert list(scratch_dir.iterdir()) == []
    assert dest.read_bytes() == versions[-1]


@pytest.mark.parametrize(("umask", "mode"), [(0o022, 0o644), (0o077, 0o600)])
def test_exported_artifact_mode_follows_umask(tmp_path, umask, mode):
    baseline = tmp_path / "baseline.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    store = BaselineStore(tmp_path / "store", clock=_fixed_clock())
    sha = store.set_baseline("acme", "architecture", baseline, actor="alice")["artifact"]["sha256"]

    previous = os.umask(umask)
    try:
        exported = store.export_artifact("acme", sha, tmp_path / "out" / "exported.ifc")
    finally:
        os.umask(previous)
    assert exported.stat().st_mode & 0o777 == mode
