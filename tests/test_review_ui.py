"""Review UI server: JSON API over a seeded store, uploads, actions, and the
viewer-manifest bridge to persisted reports."""

from __future__ import annotations

import hashlib
import http.client
import json
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request

import pytest

from athar_review.server import make_server
from athar_store import BaselineStore
from tests.test_store import _GUID_A, _GUID_B, _fixed_clock, _write_model

DEMO_MOVED_DOORS = 40


def write_review_demo_pair(old_path, new_path) -> None:
    """Write a baseline and a revised delivery of a small door schedule.

    The revision moves DEMO_MOVED_DOORS doors by one translation, edits and adds
    door properties, deletes one door and adds one wall. The Playwright review
    spec generates its fixtures with this function.
    """
    import ifcopenshell.api
    import ifcopenshell.api.project
    import numpy as np

    def model(path, revised: bool) -> None:
        f = ifcopenshell.api.project.create_file(version="IFC4")
        project = ifcopenshell.api.run("root.create_entity", f, ifc_class="IfcProject", name="Riverside Tower")
        project.GlobalId = "0RiversideTower00000001"
        ifcopenshell.api.run("unit.assign_unit", f)
        storey = ifcopenshell.api.run("root.create_entity", f, ifc_class="IfcBuildingStorey", name="Level 1")
        storey.GlobalId = "0RiversideLevel10000001"
        ifcopenshell.api.run("aggregate.assign_object", f, relating_object=project, products=[storey])
        for index in range(DEMO_MOVED_DOORS + 6):
            if revised and index == DEMO_MOVED_DOORS + 5:
                continue
            door = ifcopenshell.api.run("root.create_entity", f, ifc_class="IfcDoor", name=f"D-{101 + index}")
            door.GlobalId = f"0RiversideDoor{index:08d}"
            matrix = np.eye(4)
            matrix[0][3] = index * 1.5
            if revised and index < DEMO_MOVED_DOORS:
                matrix[0][3] += 1.2
                matrix[1][3] -= 0.35
            ifcopenshell.api.run("geometry.edit_object_placement", f, product=door, matrix=matrix)
            ifcopenshell.api.run("spatial.assign_container", f, relating_structure=storey, products=[door])
            pset = ifcopenshell.api.run("pset.add_pset", f, product=door, name="Pset_DoorCommon")
            pset.GlobalId = f"0RiversidePset{index:08d}"
            properties = {"FireRating": "EI30", "HeadHeight": 2.1}
            if revised and index == DEMO_MOVED_DOORS:
                properties = {"FireRating": "EI60", "HeadHeight": 2.4}
            if revised and index == DEMO_MOVED_DOORS + 1:
                properties["AcousticRating"] = "Rw 32 dB"
            ifcopenshell.api.run("pset.edit_pset", f, pset=pset, properties=properties)
        if revised:
            wall = ifcopenshell.api.run("root.create_entity", f, ifc_class="IfcWall", name="W-201")
            wall.GlobalId = "0RiversideWall000000001"
            ifcopenshell.api.run("spatial.assign_container", f, relating_structure=storey, products=[wall])
        f.write(str(path))

    model(old_path, revised=False)
    model(new_path, revised=True)


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


@pytest.mark.parametrize(("project", "model"), [
    ("%2e%2e", "m"), ("%2e", "m"), ("safe%0a", "m"),
    ("safe", "%2e%2e"), ("safe", "m%0a"),
])
def test_percent_encoded_invalid_baseline_ids_are_client_errors(server, seeded, project, model):
    before = seeded["store"].list_projects()
    status, error = _error(_upload(
        server, f"/api/projects/{project}/models/{model}/baseline?actor=alice&filename=x.ifc",
        seeded["baseline"].read_bytes(),
    ))
    assert (status, error["code"]) == (400, "invalid_identifier")
    assert seeded["store"].list_projects() == before
    assert not (seeded["store"].root / "projects" / "project.json").exists()


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
        headers={"Content-Type": "application/json"},
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


def _raw(server, method, path, *, body=b"", headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=30)
    try:
        connection.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
        sent = {"Host": f"127.0.0.1:{server.server_address[1]}", **(headers or {})}
        for name, value in sent.items():
            if value is not None:
                connection.putheader(name, value)
        if "Content-Length" not in sent:
            connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def _approve_body(seeded, actor="carol"):
    return json.dumps({"review_id": seeded["review"]["id"], "actor": actor}).encode()


def _baseline_pointer_sha(seeded):
    return seeded["store"].current_baseline("acme", "architecture")["artifact"]["sha256"]


def test_server_listens_on_loopback_only(server):
    assert server.server_address[0] == "127.0.0.1"


@pytest.mark.parametrize("host", ["evil.example:80", "attacker.test", None, "127.0.0.1:1"])
def test_foreign_host_header_is_refused(server, host):
    status, _, _ = _raw(server, "GET", "/api/overview", headers={"Host": host})
    assert status == 403


def test_localhost_host_header_is_accepted(server):
    port = server.server_address[1]
    status, _, _ = _raw(server, "GET", "/api/overview", headers={"Host": f"localhost:{port}"})
    assert status == 200


@pytest.mark.parametrize("origin", ["https://evil.example", "null", "http://127.0.0.1:1"])
def test_cross_origin_decision_is_refused(server, seeded, origin):
    before = _baseline_pointer_sha(seeded)
    status, _, _ = _raw(
        server,
        "POST",
        "/api/projects/acme/models/architecture/approve",
        body=_approve_body(seeded),
        headers={"Content-Type": "application/json", "Origin": origin},
    )
    assert status == 403
    assert _baseline_pointer_sha(seeded) == before
    assert [e["event"] for e in seeded["store"].history("acme")] == ["baseline_set", "review"]


def test_cross_origin_upload_is_refused(server, seeded):
    status, _, _ = _raw(
        server,
        "POST",
        "/api/projects/acme/models/architecture/review?actor=mallory&filename=x.ifc",
        body=seeded["candidate"].read_bytes(),
        headers={"Content-Type": "application/x-step", "Origin": "https://evil.example"},
    )
    assert status == 403
    assert len(seeded["store"].history("acme")) == 2


@pytest.mark.parametrize(
    "content_type", [None, "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x"]
)
def test_decision_with_simple_request_content_type_is_refused(server, seeded, content_type):
    status, _, _ = _raw(
        server,
        "POST",
        "/api/projects/acme/models/architecture/approve",
        body=_approve_body(seeded),
        headers={"Content-Type": content_type},
    )
    assert status == 415
    assert len(seeded["store"].history("acme")) == 2


@pytest.mark.parametrize("content_type", [None, "text/plain", "multipart/form-data; boundary=x"])
def test_upload_with_simple_request_content_type_is_refused(server, seeded, content_type):
    status, _, _ = _raw(
        server,
        "POST",
        "/api/projects/acme/models/architecture/review?actor=carol&filename=x.ifc",
        body=seeded["candidate"].read_bytes(),
        headers={"Content-Type": content_type},
    )
    assert status == 415
    assert len(seeded["store"].history("acme")) == 2


def test_same_origin_decision_is_accepted_without_cors_headers(server, seeded):
    status, headers, body = _raw(
        server,
        "POST",
        "/api/projects/acme/models/architecture/approve",
        body=_approve_body(seeded),
        headers={"Content-Type": "application/json; charset=utf-8", "Origin": server.origin},
    )
    assert status == 200, body
    assert json.loads(body)["event"] == "approval"
    assert not [name for name in headers if name.lower().startswith("access-control-")]


def test_preflight_gets_no_cors_grant(server):
    status, headers, _ = _raw(
        server,
        "OPTIONS",
        "/api/projects/acme/models/architecture/approve",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )
    assert status >= 400
    assert not [name for name in headers if name.lower().startswith("access-control-")]


def _error(status_headers_body):
    status, _, body = status_headers_body
    return status, json.loads(body)["error"]


def _post_json(server, path, payload):
    return _raw(
        server, "POST", path, body=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )


def _upload(server, path, data):
    return _raw(server, "POST", path, body=data, headers={"Content-Type": "application/x-step"})


@pytest.mark.parametrize("length", ["1_000", "+5", "5 ", "-1", "nope", "²"])
def test_content_length_requires_ascii_digits(server, length):
    status, error = _error(_raw(
        server, "POST", "/api/projects/acme/models/architecture/approve",
        body=b"{}" + b" " * 998, headers={"Content-Type": "application/json", "Content-Length": length},
    ))
    assert (status, error["code"]) == (400, "invalid_length")


def test_truncated_upload_is_client_error(server):
    with socket.create_connection(server.server_address, timeout=10) as client:
        client.sendall((
            "POST /api/projects/acme/models/architecture/review?actor=bob&filename=x.ifc HTTP/1.0\r\n"
            f"Host: 127.0.0.1:{server.server_address[1]}\r\n"
            "Content-Type: application/x-step\r\nContent-Length: 100\r\n\r\nshort"
        ).encode())
        client.shutdown(socket.SHUT_WR)
        response = http.client.HTTPResponse(client)
        response.begin()
        assert response.status == 400
        assert json.loads(response.read())["error"]["code"] == "upload_truncated"


@pytest.mark.parametrize("body", [b"{broken", b"[]", b'"text"'])
def test_invalid_decision_json_is_client_error(server, body):
    status, error = _error(_raw(
        server, "POST", "/api/projects/acme/models/architecture/approve",
        body=body, headers={"Content-Type": "application/json"},
    ))
    assert (status, error["code"]) == (400, "invalid_json")


def test_non_utf8_decision_is_invalid_json(server):
    status, error = _error(_raw(
        server, "POST", "/api/projects/acme/models/architecture/approve",
        body=b'{"actor":"\xff"}', headers={"Content-Type": "application/json"},
    ))
    assert status == 400
    assert error["code"] == "invalid_json"


def test_unknown_project_is_404(server):
    for path in ("/api/projects/nope/events", "/api/projects/nope/reviews/evt-000001"):
        status, error = _error(_raw(server, "GET", path))
        assert (status, error["code"]) == (404, "project_not_found")
    status, error = _error(
        _post_json(server, "/api/projects/nope/models/architecture/approve", {"review_id": "evt-000002", "actor": "a"})
    )
    assert (status, error["code"]) == (404, "project_not_found")


def test_report_of_missing_or_non_review_event_is_404(server):
    for review_id in ("evt-000099", "evt-000001"):
        status, error = _error(_raw(server, "GET", f"/api/projects/acme/reviews/{review_id}"))
        assert (status, error["code"]) == (404, "review_not_found")
        assert review_id in error["message"]


@pytest.mark.parametrize("decided_by", ["approve", "reject"])
def test_decision_on_decided_review_is_409_naming_the_event(server, seeded, decided_by):
    review_id = seeded["review"]["id"]
    path = f"/api/projects/acme/models/architecture/{decided_by}"
    status, _, _ = _post_json(server, path, {"review_id": review_id, "actor": "alice"})
    assert status == 200
    decision_id = seeded["store"].history("acme")[-1]["id"]
    state = "approved" if decided_by == "approve" else "rejected"
    for action in ("approve", "reject"):
        status, error = _error(
            _post_json(server, f"/api/projects/acme/models/architecture/{action}", {"review_id": review_id, "actor": "bob"})
        )
        assert status == 409
        assert error["code"] == f"review_{state}"
        assert (error["state"], error["event_id"], error["review_id"]) == (state, decision_id, review_id)


def test_decision_on_stale_review_is_409(server, seeded, tmp_path):
    other = tmp_path / "other.ifc"
    _write_model(other, [(_GUID_A, "wall-A"), (_GUID_B, "wall-B-other")])
    second = seeded["store"].review("acme", "architecture", other, actor="carol")
    seeded["store"].approve("acme", "architecture", second["id"], actor="carol")
    status, error = _error(
        _post_json(
            server,
            "/api/projects/acme/models/architecture/approve",
            {"review_id": seeded["review"]["id"], "actor": "alice"},
        )
    )
    assert (status, error["code"], error["state"]) == (409, "review_stale", "stale")


@pytest.mark.parametrize("actor", [None, "", "   ", 7])
def test_decision_without_actor_is_400(server, seeded, actor):
    payload = {"review_id": seeded["review"]["id"]}
    if actor is not None:
        payload["actor"] = actor
    status, error = _error(_post_json(server, "/api/projects/acme/models/architecture/approve", payload))
    assert (status, error["code"]) == (400, "actor_required")
    assert len(seeded["store"].history("acme")) == 2


@pytest.mark.parametrize("query", ["filename=c.ifc", "actor=&filename=c.ifc", "actor=%20&filename=c.ifc"])
def test_upload_without_actor_is_400(server, seeded, query):
    status, error = _error(
        _upload(server, f"/api/projects/acme/models/architecture/review?{query}", seeded["candidate"].read_bytes())
    )
    assert (status, error["code"]) == (400, "actor_required")


def test_refused_first_baseline_does_not_create_project(server, seeded):
    before = seeded["store"].list_projects()
    status, error = _error(_upload(
        server, "/api/projects/new-project/models/m/baseline?actor=alice&filename=broken.ifc",
        b"not an IFC model",
    ))
    assert (status, error["code"]) == (422, "engine_refused")
    assert seeded["store"].list_projects() == before
    assert not (seeded["store"].root / "projects" / "new-project").exists()


def test_unparsable_upload_is_422(server):
    for kind in ("review", "baseline"):
        model = "architecture" if kind == "review" else "fresh"
        status, error = _error(
            _upload(server, f"/api/projects/acme/models/{model}/{kind}?actor=a&filename=bad.ifc", b"not an ifc file\n")
        )
        assert (status, error["code"]) == (422, "engine_refused"), kind


def test_schema_mismatch_upload_is_422(server, tmp_path):
    import ifcopenshell

    legacy = ifcopenshell.file(schema="IFC2X3")
    legacy.create_entity("IfcProject", GlobalId="0project0project0proj01", Name="legacy")
    path = tmp_path / "legacy.ifc"
    legacy.write(str(path))
    status, error = _error(
        _upload(server, "/api/projects/acme/models/architecture/review?actor=a&filename=legacy.ifc", path.read_bytes())
    )
    assert (status, error["code"]) == (422, "engine_refused")
    assert "IFC2X3" in error["message"]


def test_baseline_upload_over_existing_baseline_is_409(server, seeded):
    status, error = _error(
        _upload(
            server,
            "/api/projects/acme/models/architecture/baseline?actor=a&filename=b.ifc",
            seeded["baseline"].read_bytes(),
        )
    )
    assert (status, error["code"]) == (409, "baseline_exists")


def test_review_upload_without_baseline_is_409(server, seeded):
    status, error = _error(
        _upload(
            server,
            "/api/projects/acme/models/structure/review?actor=a&filename=c.ifc",
            seeded["candidate"].read_bytes(),
        )
    )
    assert (status, error["code"]) == (409, "no_baseline")


def test_unexpected_error_is_logged_500_with_generic_message(seeded, tmp_path, caplog):
    class BrokenStore(BaselineStore):
        def list_projects(self):
            raise RuntimeError("secret internal detail")

    srv = make_server(BrokenStore(seeded["store"].root), dist_dir=None)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        status, error = _error(_raw(srv, "GET", "/api/overview"))
    finally:
        srv.shutdown()
        srv.server_close()
    assert (status, error["code"]) == (500, "internal_error")
    assert "secret" not in error["message"]
    assert any("secret internal detail" in (r.exc_text or "") for r in caplog.records)


@pytest.fixture
def small_limit_server(seeded):
    srv = make_server(seeded["store"], dist_dir=None, max_upload_bytes=64)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def test_upload_over_the_limit_is_413_naming_the_limit(small_limit_server, seeded):
    status, error = _error(
        _upload(
            small_limit_server,
            "/api/projects/acme/models/architecture/review?actor=a&filename=c.ifc",
            seeded["candidate"].read_bytes(),
        )
    )
    assert (status, error["code"], error["limit_bytes"]) == (413, "upload_too_large", 64)
    assert "--max-upload-mb" in error["message"]
    assert len(seeded["store"].history("acme")) == 2


@pytest.mark.parametrize(('path', 'content_type', 'expected'), [
    ('/api/projects/acme/models/architecture/review?actor=bob&filename=x.ifc', 'application/x-step', (413, 'upload_too_large')),
    ('/api/projects/acme/models/architecture/review?actor=bob&filename=x.ifc', 'text/plain', (415, 'unsupported_media_type')),
    ('/api/projects/acme/models/architecture/review?filename=x.ifc', 'application/x-step', (400, 'actor_required')),
    ('/api/missing', 'application/x-step', (404, 'not_found')),
])
def test_large_refused_upload_returns_json(server, path, content_type, expected):
    server.max_upload_bytes = 3_000_000
    status, error = _error(_raw(
        server, 'POST', path, body=b'x' * 3_500_000,
        headers={'Content-Type': content_type},
    ))
    assert (status, error['code']) == expected


def test_refusal_above_drain_bound_does_not_wait_for_body(server):
    server.max_upload_bytes = 3_000_000
    status, error = _error(_raw(
        server, 'POST', '/api/projects/acme/models/architecture/review?actor=bob&filename=x.ifc',
        headers={'Content-Type': 'application/x-step', 'Content-Length': '4000001'},
    ))
    assert (status, error['code']) == (413, 'upload_too_large')


def test_overview_states_the_upload_limit(small_limit_server):
    status, _, body = _raw(small_limit_server, "GET", "/api/overview")
    assert status == 200 and json.loads(body)["max_upload_bytes"] == 64


def test_upload_without_content_length_is_411(server, seeded):
    connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=30)
    try:
        connection.putrequest(
            "POST", "/api/projects/acme/models/architecture/review?actor=a&filename=c.ifc", skip_host=True
        )
        connection.putheader("Host", f"127.0.0.1:{server.server_address[1]}")
        connection.putheader("Content-Type", "application/x-step")
        connection.putheader("Transfer-Encoding", "chunked")
        connection.endheaders()
        connection.send(b"0\r\n\r\n")
        response = connection.getresponse()
        assert response.status == 411
    finally:
        connection.close()


def test_client_file_name_is_recorded_never_used_as_a_path(server, seeded, tmp_path, monkeypatch):
    import tempfile

    scratch = tmp_path / "scratch" / "tmp"
    scratch.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    name = "../../escape.ifc"
    status, _, body = _upload(
        server,
        f"/api/projects/acme/models/architecture/review?actor=a&filename={urllib.parse.quote(name)}",
        seeded["candidate"].read_bytes(),
    )
    assert status == 200, body
    assert json.loads(body)["source_name"] == name
    assert not list(tmp_path.rglob("escape.ifc"))


def test_review_help_names_purpose_store_and_upload_limit():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "athar", "review", "--help"], capture_output=True, text=True, check=True
    )
    text = " ".join(result.stdout.split())
    assert "approves or rejects" in text
    assert "~/.athar/store" in text and "ATHAR_STORE_ROOT" in text
    assert "--max-upload-mb" in text and "megabytes" in text
    assert "127.0.0.1" in text


def test_review_prints_its_url_when_stdout_is_a_pipe(tmp_path):
    import queue
    import subprocess
    import sys

    process = subprocess.Popen(
        [sys.executable, "-m", "athar", "review", "--root", str(tmp_path / "store"), "--no-open", "--dist", str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    lines: queue.Queue[str] = queue.Queue()
    threading.Thread(target=lambda: [lines.put(line) for line in process.stdout], daemon=True).start()
    try:
        first = lines.get(timeout=60)
        assert first.startswith("Athar Review: http://127.0.0.1:")
        url = first.split(": ", 1)[1].strip()
        with urllib.request.urlopen(f"{url}api/overview", timeout=30) as response:
            assert json.loads(response.read())["projects"] == []
        assert lines.get(timeout=30).strip() == f"Store: {tmp_path / 'store'}"
    finally:
        process.terminate()
        process.wait(timeout=30)


def test_demo_pair_carries_property_deltas_and_a_cohort(server, tmp_path):
    old, new = tmp_path / "riverside-A.ifc", tmp_path / "riverside-B.ifc"
    write_review_demo_pair(old, new)
    status, _, _ = _upload(server, "/api/projects/riverside/models/arch/baseline?actor=a&filename=A.ifc", old.read_bytes())
    assert status == 200
    status, _, body = _upload(server, "/api/projects/riverside/models/arch/review?actor=b&filename=B.ifc", new.read_bytes())
    assert status == 200, body
    review_id = json.loads(body)["id"]
    report = _get_json(server, f"/api/projects/riverside/reviews/{review_id}")
    (cohort,) = report["stats"]["placement_cohorts"]
    assert (cohort["count"], cohort["delta_mm"]) == (DEMO_MOVED_DOORS, [1200.0, -350.0, 0.0])
    changed = {
        (delta["pset"], delta["name"]): (delta["old_value"], delta["new_value"])
        for item in report["modified"]
        for delta in (item.get("property_deltas") or {}).get("changed", [])
    }
    assert changed == {("Pset_DoorCommon", "FireRating"): ("EI30", "EI60"), ("Pset_DoorCommon", "HeadHeight"): (2.1, 2.4)}
    assert (report["stats"]["added"], report["stats"]["deleted"]) == (1, 1)
