"""HTTP status codes the review server maps store errors to."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

import pytest

from athar_review.server import make_server
from athar_store import BaselineStore
from athar_store.cli import build_store_parser, run_store
from tests.test_store import _GUID_A, _GUID_B, _write_model


@pytest.fixture
def seeded(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(candidate, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])
    store = BaselineStore(tmp_path / "store")
    store.set_baseline("acme", "architecture", baseline, actor="alice")
    review = store.review("acme", "architecture", candidate, actor="bob")
    return {"store": store, "review": review, "baseline": baseline, "candidate": candidate}


@pytest.fixture
def server(seeded):
    srv = make_server(seeded["store"], dist_dir=None)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def _post_status(server, path: str, data: bytes) -> tuple[int, dict]:
    request = urllib.request.Request(f"{server.origin}{path}", data=data, method="POST")
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def _decide(server, action: str, review_id: str, actor: str | None = "carol") -> tuple[int, dict]:
    body = json.dumps({"review_id": review_id, "actor": actor}).encode()
    return _post_status(server, f"/api/projects/acme/models/architecture/{action}", body)


def test_decision_on_decided_review_is_409(server, seeded):
    review_id = seeded["review"]["id"]
    status, approval = _decide(server, "approve", review_id)
    assert status == 200

    status, body = _decide(server, "reject", review_id)
    assert status == 409
    assert body["error"] == f"Review {review_id} is already approved by event {approval['id']}."


def test_missing_or_blank_actor_is_400(server, seeded):
    review_id = seeded["review"]["id"]
    for actor in (None, "  "):
        status, body = _decide(server, "approve", review_id, actor=actor)
        assert status == 400
        assert "actor is required" in body["error"]
    status, body = _post_status(
        server,
        "/api/projects/acme/models/architecture/review?filename=c.ifc",
        seeded["candidate"].read_bytes(),
    )
    assert status == 400
    assert "actor is required" in body["error"]
    assert [e["event"] for e in seeded["store"].history("acme")] == ["baseline_set", "review"]


def test_engine_refusal_is_422(server, seeded, tmp_path):
    import ifcopenshell

    older = tmp_path / "older.ifc"
    model = ifcopenshell.file(schema="IFC2X3")
    model.create_entity("IfcProject", GlobalId="0project0project0proj01", Name="older")
    model.write(str(older))
    for body in (older.read_bytes(), b"not an IFC file"):
        status, payload = _post_status(
            server,
            "/api/projects/acme/models/architecture/review?actor=dave&filename=d.ifc",
            body,
        )
        assert status == 422
        assert "candidate could not be compared" in payload["error"]
    status, payload = _post_status(
        server,
        "/api/projects/acme/models/structure/baseline?actor=dave&filename=s.ifc",
        b"not an IFC file",
    )
    assert status == 422
    assert "Cannot parse IFC file" in payload["error"]
    assert [e["event"] for e in seeded["store"].history("acme")] == ["baseline_set", "review"]


def test_unreadable_pack_metadata_is_a_store_error(server, seeded, capsys):
    store = seeded["store"]
    sha = seeded["review"]["candidate_artifact"]["sha256"]
    sidecar = store.root / "projects" / "acme" / "artifacts" / f"{sha}.meta.json"
    meta = json.loads(sidecar.read_text())
    sidecar.write_text(json.dumps({**meta, "format": 99}))
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{server.origin}/api/projects/acme/artifacts/{sha}.ifc")
    assert err.value.code == 400
    assert "pack format 99" in json.loads(err.value.read())["error"]
    args = build_store_parser().parse_args(["--root", str(store.root), "stats", "acme"])
    assert run_store(args) == 1
    assert "pack format 99" in capsys.readouterr().err


def test_report_of_a_non_review_event_is_404(server, seeded):
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{server.origin}/api/projects/acme/reviews/evt-000001")
    assert err.value.code == 404
    assert "not a review" in json.loads(err.value.read())["error"]


def test_missing_or_blank_filename_is_400(server, seeded):
    body = seeded["candidate"].read_bytes()
    for action in ("review", "baseline"):
        for query in ("actor=dave", "actor=dave&filename=", "actor=dave&filename=%20%20"):
            status, payload = _post_status(
                server, f"/api/projects/acme/models/structure/{action}?{query}", body
            )
            assert status == 400
            assert "filename is required" in payload["error"]
    assert [e["event"] for e in seeded["store"].history("acme")] == ["baseline_set", "review"]
