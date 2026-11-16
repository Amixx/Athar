"""Review UI server: JSON API over a seeded store, uploads, actions, and the
viewer-manifest bridge to persisted reports."""

from __future__ import annotations

import hashlib
import json
import threading
import urllib.error
import urllib.request

import pytest

from athar_review.server import make_server
from athar_store import BaselineStore
from tests.test_store import _GUID_A, _GUID_B, _fixed_clock, _write_model


@pytest.fixture
def seeded(tmp_path):
    baseline = tmp_path / "baseline.ifc"
    candidate = tmp_path / "candidate.ifc"
    _write_model(baseline, [(_GUID_A, "wall-A")])
    _write_model(candidate, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B")])

    store = BaselineStore(tmp_path / "store", clock=_fixed_clock())
    store.set_baseline("acme", "architecture", baseline, actor="alice")
    review = store.review("acme", "architecture", candidate, actor="bob")
    return {"store": store, "review": review, "baseline": baseline, "candidate": candidate}


@pytest.fixture
def server(seeded, tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "review.html").write_text("<html>review-ui</html>", encoding="utf-8")
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    srv = make_server(seeded["store"], dist_dir=dist)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def _get(server, path):
    with urllib.request.urlopen(f"{server.origin}{path}") as response:
        return response.status, response.read()


def _get_json(server, path):
    status, body = _get(server, path)
    assert status == 200
    return json.loads(body)


def _post(server, path, data, headers=None):
    request = urllib.request.Request(
        f"{server.origin}{path}", data=data, headers=headers or {}, method="POST"
    )
    with urllib.request.urlopen(request) as response:
        return response.status, json.loads(response.read())


def test_overview_lists_models_and_pending(server):
    overview = _get_json(server, "/api/overview")
    (project,) = overview["projects"]
    assert project["project_id"] == "acme"
    (model,) = project["models"]
    assert model["model_key"] == "architecture"
    assert model["pending_count"] == 1
    assert model["baseline"]["approved_by"] == "alice"


def test_events_and_report_endpoints(server, seeded):
    events = _get_json(server, "/api/projects/acme/events?model_key=architecture")["events"]
    assert [e["event"] for e in events] == ["baseline_set", "review"]

    review_id = seeded["review"]["id"]
    report = _get_json(server, f"/api/projects/acme/reviews/{review_id}")
    assert report["stats"]["added"] == 1

    status = urllib.request.urlopen(f"{server.origin}/api/projects/acme/events")
    assert status.status == 200


def test_artifact_download_is_byte_exact(server, seeded):
    sha = seeded["review"]["candidate_artifact"]["sha256"]
    status, body = _get(server, f"/api/projects/acme/artifacts/{sha}.ifc")
    assert status == 200
    assert hashlib.sha256(body).hexdigest() == sha


def test_viewer_manifest_bridges_to_stored_report(server, seeded):
    review_id = seeded["review"]["id"]
    manifest = _get_json(server, f"/viewer/acme/{review_id}/manifest.json")
    assert manifest["athar_viewer_manifest"] == 1
    assert manifest["report"]["url"] == f"/api/projects/acme/reviews/{review_id}"
    old_url = manifest["old"]["url"]
    status, body = _get(server, old_url)
    assert status == 200
    assert body.startswith(b"ISO-10303-21")


def test_upload_review_then_approve_flips_baseline(server, seeded, tmp_path):
    third = tmp_path / "third.ifc"
    _write_model(third, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B-variant")])

    status, event = _post(
        server,
        "/api/projects/acme/models/architecture/review?actor=carol&filename=third.ifc",
        third.read_bytes(),
        headers={"Content-Type": "application/x-step"},
    )
    assert status == 200
    assert event["event"] == "review"
    assert event["actor"] == "carol"
    assert len(event["conflicts"]) == 1

    status, approval = _post(
        server,
        "/api/projects/acme/models/architecture/approve",
        json.dumps({"review_id": event["id"], "actor": "carol", "note": "ok"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    assert status == 200
    assert approval["event"] == "approval"
    head = seeded["store"].current_baseline("acme", "architecture")
    assert head["artifact"]["sha256"] == event["candidate_artifact"]["sha256"]


def test_upload_baseline_creates_new_lineage(server, seeded):
    body = seeded["baseline"].read_bytes()
    status, event = _post(
        server,
        "/api/projects/acme/models/structure/baseline?actor=alice&filename=structure.ifc",
        body,
        headers={"Content-Type": "application/x-step"},
    )
    assert status == 200
    assert event["event"] == "baseline_set"
    overview = _get_json(server, "/api/overview")
    keys = {m["model_key"] for m in overview["projects"][0]["models"]}
    assert keys == {"architecture", "structure"}


def test_reject_leaves_baseline(server, seeded):
    before = seeded["store"].current_baseline("acme", "architecture")["artifact"]["sha256"]
    status, event = _post(
        server,
        "/api/projects/acme/models/architecture/reject",
        json.dumps({"review_id": seeded["review"]["id"], "actor": "alice"}).encode(),
    )
    assert status == 200
    assert event["event"] == "rejection"
    assert seeded["store"].current_baseline("acme", "architecture")["artifact"]["sha256"] == before


def test_static_serves_review_shell_and_guards_traversal(server):
    status, body = _get(server, "/")
    assert status == 200 and b"review-ui" in body
    status, body = _get(server, "/index.html")
    assert status == 200 and b"viewer" in body
    with pytest.raises(urllib.error.HTTPError) as err:
        _get(server, "/../secrets.txt")
    assert err.value.code in (400, 404)


def test_unknown_routes_and_bad_ids_are_client_errors(server):
    with pytest.raises(urllib.error.HTTPError) as err:
        _get(server, "/api/projects/acme/reviews/evt-999999")
    assert err.value.code == 404
    with pytest.raises(urllib.error.HTTPError) as err:
        _get(server, "/api/projects/..%2Fetc/events")
    assert err.value.code == 400
    with pytest.raises(urllib.error.HTTPError) as err:
        _get(server, "/api/projects/acme/artifacts/notasha.ifc")
    assert err.value.code == 400
