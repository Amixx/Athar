"""Every event is evidence: who, which delivery, which Athar, when."""

from __future__ import annotations

import hashlib
import json
import re
from importlib import metadata

import pytest

from athar.bottom.constants import CANON_VERSION
from athar_store import BaselineStore, StoreError
from athar_store.cli import build_store_parser, run_store
from tests.test_store import _GUID_A, _GUID_B, _write_model


@pytest.fixture
def files(tmp_path):
    baseline = tmp_path / "delivery-2026-01.ifc"
    candidate = tmp_path / "delivery-2026-02.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(candidate, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])
    return {"baseline": baseline, "candidate": candidate}


@pytest.mark.parametrize("actor", ["", "   ", None])
def test_blank_actor_is_refused_and_records_nothing(tmp_path, files, actor):
    store = BaselineStore(tmp_path / "store")
    with pytest.raises(StoreError, match="actor is required"):
        store.set_baseline("acme", "arch", files["baseline"], actor=actor)
    store.set_baseline("acme", "arch", files["baseline"], actor="alice")
    with pytest.raises(StoreError, match="actor is required"):
        store.review("acme", "arch", files["candidate"], actor=actor)
    review = store.review("acme", "arch", files["candidate"], actor="bob")
    for decide in (store.approve, store.reject):
        with pytest.raises(StoreError, match="actor is required"):
            decide("acme", "arch", review["id"], actor=actor)
    assert [event["event"] for event in store.history("acme")] == ["baseline_set", "review"]


def test_cli_decisions_require_a_nonblank_actor(tmp_path, files, capsys):
    root = str(tmp_path / "store")
    store = BaselineStore(root)
    store.set_baseline("acme", "arch", files["baseline"], actor="alice")
    review = store.review("acme", "arch", files["candidate"], actor="bob")
    parser = build_store_parser()
    for action in ("approve", "reject"):
        with pytest.raises(SystemExit) as exit_info:
            parser.parse_args(["--root", root, action, "acme", "arch", review["id"]])
        assert exit_info.value.code == 2
        args = parser.parse_args(["--root", root, action, "acme", "arch", review["id"], "--actor", " "])
        assert run_store(args) == 1
        assert "actor is required" in capsys.readouterr().err


def test_events_record_delivery_name_versions_and_utc_time(tmp_path, files):
    store = BaselineStore(tmp_path / "store")
    baseline = store.set_baseline("acme", "arch", files["baseline"], actor=" alice ")
    review = store.review(
        "acme", "arch", files["candidate"], actor="bob", source_name="Tower_ARC_rev2.ifc"
    )
    approval = store.approve("acme", "arch", review["id"], actor="carol")

    assert baseline["actor"] == "alice"
    assert baseline["source_name"] == "delivery-2026-01.ifc"
    assert review["source_name"] == "Tower_ARC_rev2.ifc"
    assert approval["source_name"] == "Tower_ARC_rev2.ifc"
    for event in store.history("acme"):
        assert event["athar_version"] == metadata.version("athar-ifc")
        assert event["canon_version"] == CANON_VERSION
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", event["at"])


def test_pack_metadata_carries_a_checked_format_version(tmp_path, files):
    store = BaselineStore(tmp_path / "store")
    event = store.set_baseline("acme", "arch", files["baseline"], actor="alice")
    sha = event["artifact"]["sha256"]
    meta_path = tmp_path / "store" / "projects" / "acme" / "artifacts" / f"{sha}.meta.json"
    meta = json.loads(meta_path.read_text())
    assert meta["format"] == 1

    meta["format"] = 99
    meta_path.write_text(json.dumps(meta))
    with pytest.raises(StoreError, match="pack format 99"):
        store.export_artifact("acme", sha, tmp_path / "out.ifc")


def test_review_records_the_candidate_bytes_that_were_diffed(tmp_path, files):
    original = files["candidate"].read_bytes()

    def rewrite_candidate(step):
        if step == "candidate_compared":
            _write_model(files["candidate"], [(_GUID_B, "wall-B")])

    store = BaselineStore(tmp_path / "store", fault_hook=rewrite_candidate)
    store.set_baseline("acme", "arch", files["baseline"], actor="alice")
    review = store.review("acme", "arch", files["candidate"], actor="bob")
    assert files["candidate"].read_bytes() != original
    assert review["diff"]["stats"]["added"] == 1
    assert review["candidate_artifact"]["sha256"] == hashlib.sha256(original).hexdigest()
    exported = store.export_artifact(
        "acme", review["candidate_artifact"]["sha256"], tmp_path / "exported.ifc"
    )
    assert exported.read_bytes() == original
