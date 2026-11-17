"""Tests for the athar_view launcher: server endpoints, modes, difftool."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import shlex
import subprocess
import sys
import sysconfig
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import athar_git.cli as git_cli_mod
import athar_view.cli as view_cli_mod
from athar_view.cli import build_view_parser, prepare_view
from athar_view.server import (
    MANIFEST_SCHEMA_VERSION,
    build_chain_steps,
    build_report_bytes,
    find_dist,
    make_chain_server,
    make_server,
)
from tests.corpus import REPO_ROOT
from tests.corpus import VIEWER_NEW_IFC as NEW_IFC
from tests.corpus import VIEWER_OLD_IFC as OLD_IFC



@pytest.fixture(scope="module")
def report_bytes(tmp_path_factory) -> bytes:
    cache_dir = tmp_path_factory.mktemp("view-cache")
    return build_report_bytes(OLD_IFC, NEW_IFC, cache_dir=cache_dir)


def _serve(server) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def _get(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.status, dict(response.headers), response.read()


def _raw(server, method: str, path: str, headers: dict[str, str]):
    conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    try:
        conn.request(method, path, headers=headers)
        response = conn.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        conn.close()


def test_build_report_bytes_is_an_engine_report_and_populates_cache(tmp_path):
    cache_dir = tmp_path / "cache"
    payload = json.loads(build_report_bytes(OLD_IFC, NEW_IFC, cache_dir=cache_dir))
    assert payload["engine"] == "athar"
    assert payload["stats"]["added"] == 1
    assert payload["stats"]["deleted"] == 1
    assert list(cache_dir.rglob("*.pickle"))


def test_server_serves_manifest_and_files_under_token(report_bytes):
    server = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0)
    _serve(server)
    try:
        data = server.data_url
        assert data.startswith("http://127.0.0.1:")

        status, headers, body = _get(f"{data}/manifest.json")
        assert status == 200
        manifest = json.loads(body)
        assert manifest["athar_viewer_manifest"] == 1
        assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert manifest["old"] == {"name": "old.ifc", "url": "files/old.ifc"}
        assert manifest["new"] == {"name": "new.ifc", "url": "files/new.ifc"}
        assert manifest["report"] == {"url": "files/report.json"}
        assert "Access-Control-Allow-Origin" not in headers
        assert "Access-Control-Allow-Private-Network" not in headers
        assert headers["Cross-Origin-Resource-Policy"] == "same-origin"
        assert headers["Cache-Control"] == "no-store"

        status, _, body = _get(f"{data}/files/old.ifc")
        assert status == 200
        assert body == OLD_IFC.read_bytes()

        status, _, body = _get(f"{data}/files/report.json")
        assert status == 200
        assert json.loads(body)["engine"] == "athar"

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            _get(f"{data}/nope")
        assert excinfo.value.code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_server_refuses_missing_token_foreign_host_and_foreign_origin(report_bytes, tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    server = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=dist, port=0)
    _serve(server)
    token_path = f"/{server.token}/manifest.json"
    own = {"Host": server.host}
    try:
        assert len(server.token) >= 32
        assert _raw(server, "GET", token_path, own)[0] == 200
        assert _raw(server, "GET", token_path, {**own, "Origin": server.origin})[0] == 200

        for path in ("/manifest.json", "/files/old.ifc", "/files/report.json", "/wrong-token/manifest.json"):
            assert _raw(server, "GET", path, own)[0] == 404, path
        status, headers, body = _raw(server, "GET", "/", own)
        assert status == 200 and server.token.encode() not in body
        assert "Location" not in headers

        rebound = {"Host": f"attacker.example:{server.server_address[1]}"}
        assert _raw(server, "GET", token_path, rebound)[0] == 421
        assert _raw(server, "GET", token_path, {"Host": f"localhost:{server.server_address[1]}"})[0] == 421
        assert _raw(server, "GET", "/", rebound)[0] == 421

        status, headers, body = _raw(server, "GET", token_path, {**own, "Origin": "https://evil.example"})
        assert status == 403 and body == b""
        assert "Access-Control-Allow-Origin" not in headers
        preflight = {
            **own,
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        }
        status, headers, _ = _raw(server, "OPTIONS", token_path, preflight)
        assert status == 403
        assert "Access-Control-Allow-Private-Network" not in headers
    finally:
        server.shutdown()
        server.server_close()


def test_server_grants_cors_only_to_configured_viewer_origin(report_bytes):
    viewer = "https://viewer.example"
    server = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0, allowed_origin=viewer)
    _serve(server)
    token_path = f"/{server.token}/manifest.json"
    try:
        status, headers, _ = _raw(server, "GET", token_path, {"Host": server.host, "Origin": viewer})
        assert status == 200
        assert headers["Access-Control-Allow-Origin"] == viewer

        preflight = {
            "Host": server.host,
            "Origin": viewer,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        }
        status, headers, _ = _raw(server, "OPTIONS", token_path, preflight)
        assert status == 204
        assert headers["Access-Control-Allow-Origin"] == viewer
        assert "Access-Control-Allow-Private-Network" not in headers

        status, headers, _ = _raw(server, "GET", token_path, {"Host": server.host, "Origin": "https://viewer.example.evil"})
        assert status == 403
        assert "Access-Control-Allow-Origin" not in headers
        assert _raw(server, "GET", f"/{server.token}/nope", {"Host": server.host, "Origin": viewer})[0] == 404
    finally:
        server.shutdown()
        server.server_close()


def test_server_tokens_differ_per_launch(report_bytes):
    first = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0)
    second = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0)
    try:
        assert first.token != second.token
        assert first.server_address[0] == "127.0.0.1"
    finally:
        first.server_close()
        second.server_close()


def test_server_rejects_wildcard_allowed_origin(report_bytes):
    with pytest.raises(ValueError, match="origin"):
        make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0, allowed_origin="*")


def test_server_serves_static_dist_and_blocks_traversal(report_bytes, tmp_path):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("console.log(1)", encoding="utf-8")
    secret = tmp_path / "secret.txt"
    secret.write_text("nope", encoding="utf-8")

    server = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=dist, port=0)
    _serve(server)
    try:
        origin = server.origin
        status, headers, body = _get(f"{origin}/")
        assert status == 200
        assert b"viewer" in body
        assert headers["Content-Type"].startswith("text/html")

        status, headers, _ = _get(f"{origin}/assets/app.js")
        assert status == 200
        assert headers["Content-Type"].startswith("text/javascript")

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            _get(f"{origin}/../secret.txt")
        assert excinfo.value.code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_build_chain_steps_links_consecutive_pairs(tmp_path):
    files = [tmp_path / f"r{i}.ifc" for i in range(1, 4)]
    steps = build_chain_steps(files)
    assert [(s.old_idx, s.new_idx) for s in steps] == [(0, 1), (1, 2)]
    assert steps[0].label == "r1 → r2"
    assert steps[1].label == "r2 → r3"


def test_chain_server_manifest_files_and_lazy_reports(tmp_path):
    cache = tmp_path / "cache"
    server = make_chain_server(
        [OLD_IFC, NEW_IFC, OLD_IFC], cache_dir=cache, port=0
    )
    _serve(server)
    try:
        data = server.data_url
        status, _, body = _get(f"{data}/manifest.json")
        assert status == 200
        manifest = json.loads(body)
        assert manifest["athar_viewer_manifest"] == 1
        assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert manifest["active_step"] == 0
        assert len(manifest["steps"]) == 2
        # Shared file: step 0 new url == step 1 old url (mesh-cache dedup).
        assert manifest["steps"][0]["new"]["url"] == manifest["steps"][1]["old"]["url"]
        assert manifest["steps"][0]["report"]["url"] == "files/report/0.json"

        status, _, body = _get(f"{data}/files/model/0.ifc")
        assert status == 200 and body == OLD_IFC.read_bytes()

        status, _, body = _get(f"{data}/files/report/1.json")
        assert status == 200 and json.loads(body)["engine"] == "athar"

        for bad in ("/files/model/9.ifc", "/files/report/9.json", "/files/old.ifc"):
            with pytest.raises(urllib.error.HTTPError) as excinfo:
                _get(f"{data}{bad}")
            assert excinfo.value.code == 404
    finally:
        server.shutdown()
        server.server_close()


def test_prepare_view_chain_mode_builds_chain_server(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    monkeypatch.setenv("ATHAR_VIEWER_DIST", str(dist))
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)

    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), str(OLD_IFC), "--no-open", "--cache-dir", str(tmp_path / "c")]
    )
    session = prepare_view(args)
    try:
        assert session.mode == "offline"
        assert session.server.is_chain
        assert len(session.server.chain_steps) == 2
    finally:
        session.server.server_close()


def test_prepare_view_offline_mode(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    monkeypatch.setenv("ATHAR_VIEWER_DIST", str(dist))
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)

    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), "--no-open", "--cache-dir", str(tmp_path / "cache")]
    )
    session = prepare_view(args)
    _serve(session.server)
    try:
        assert session.mode == "offline"
        server = session.server
        assert session.url == f"{server.origin}/?src={urllib.parse.quote(server.data_url, safe='')}"
        status, _, body = _get(session.url)
        assert status == 200 and b"viewer" in body
        status, headers, _ = _get(f"{server.data_url}/manifest.json")
        assert status == 200
        assert "Access-Control-Allow-Origin" not in headers
    finally:
        session.server.shutdown()
        session.server.server_close()


@pytest.mark.parametrize("mode_args", [
    ["--offline"],
    ["--viewer-url", "http://192.0.2.1:9/app"],
])
def test_offline_mode_grants_the_dev_origin(tmp_path, monkeypatch, mode_args):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    monkeypatch.setenv("ATHAR_VIEWER_DIST", str(dist))
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)
    dev_origin = "http://192.0.2.1:9" if "--viewer-url" in mode_args else "http://127.0.0.1:4173"

    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), *mode_args, "--no-open", "--dev-origin", dev_origin,
         "--cache-dir", str(tmp_path / "cache")]
    )
    session = prepare_view(args, reachable=lambda url: False)
    _serve(session.server)
    try:
        assert session.mode == "offline"
        server = session.server
        manifest = f"/{server.token}/manifest.json"
        status, headers, _ = _raw(server, "GET", manifest, {"Host": server.host, "Origin": dev_origin})
        assert status == 200
        assert headers["Access-Control-Allow-Origin"] == dev_origin
        status, _, _ = _raw(server, "GET", manifest, {"Host": server.host, "Origin": "http://127.0.0.1:1"})
        assert status == 403
    finally:
        session.server.shutdown()
        session.server.server_close()


class _HostedStub(BaseHTTPRequestHandler):
    def do_HEAD(self):  # noqa: N802
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        pass


def test_prepare_view_hosted_mode(tmp_path):
    hosted = ThreadingHTTPServer(("127.0.0.1", 0), _HostedStub)
    _serve(hosted)
    hosted_url = f"http://127.0.0.1:{hosted.server_address[1]}/app"
    try:
        args = build_view_parser().parse_args(
            [
                str(OLD_IFC),
                str(NEW_IFC),
                "--no-open",
                "--viewer-url",
                hosted_url,
                "--cache-dir",
                str(tmp_path / "cache"),
            ]
        )
        with pytest.raises(ValueError, match="--dev-origin"):
            prepare_view(args)
        args.dev_origin = f"http://127.0.0.1:{hosted.server_address[1]}"
        session = prepare_view(args)
        try:
            assert session.mode == "hosted"
            src = urllib.parse.quote(session.server.data_url, safe="")
            assert session.url == f"{hosted_url}?src={src}"
            assert session.server.dist_dir is None
            assert session.server.allowed_origin == f"http://127.0.0.1:{hosted.server_address[1]}"
        finally:
            session.server.server_close()
    finally:
        hosted.shutdown()
        hosted.server_close()


@pytest.mark.parametrize("viewer_url, dev_origin", [
    ("https://Viewer.example:443/app", "https://Viewer.example:443"),
    ("https://viewer.example/app", "HTTPS://VIEWER.EXAMPLE:443"),
    ("https://Viewer.example:443/app", "https://viewer.example"),
])
def test_hosted_viewer_is_granted_the_origin_a_browser_sends(tmp_path, monkeypatch, viewer_url, dev_origin):
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)
    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), "--no-open", "--viewer-url", viewer_url,
         "--dev-origin", dev_origin, "--cache-dir", str(tmp_path / "cache")]
    )
    session = prepare_view(args, reachable=lambda url: True)
    _serve(session.server)
    try:
        assert session.mode == "hosted"
        server = session.server
        manifest = f"/{server.token}/manifest.json"
        status, headers, _ = _raw(server, "GET", manifest, {"Host": server.host, "Origin": "https://viewer.example"})
        assert status == 200
        assert headers["Access-Control-Allow-Origin"] == "https://viewer.example"
    finally:
        session.server.shutdown()
        session.server.server_close()


def test_prepare_view_refuses_a_dev_origin_other_than_the_viewer_origin(tmp_path, monkeypatch):
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)
    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), "--no-open", "--viewer-url", "https://viewer.example/app",
         "--dev-origin", "http://127.0.0.1:5173", "--cache-dir", str(tmp_path / "cache")]
    )
    with pytest.raises(ValueError, match="does not match the origin https://viewer.example"):
        prepare_view(args)


def test_prepare_view_names_both_ways_out_when_the_env_viewer_lacks_a_dev_origin(tmp_path, monkeypatch):
    monkeypatch.setenv("ATHAR_VIEWER_URL", "https://viewer.example/app")
    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), "--no-open", "--cache-dir", str(tmp_path / "cache")]
    )
    with pytest.raises(ValueError) as excinfo:
        prepare_view(args)
    message = str(excinfo.value)
    assert "ATHAR_VIEWER_URL" in message
    assert "--dev-origin https://viewer.example" in message
    assert "--offline" in message


def test_prepare_view_falls_back_when_hosted_unreachable(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("x", encoding="utf-8")
    monkeypatch.setenv("ATHAR_VIEWER_DIST", str(dist))

    args = build_view_parser().parse_args(
        [
            str(OLD_IFC),
            str(NEW_IFC),
            "--no-open",
            "--viewer-url",
            "http://192.0.2.1:9/app",
            "--dev-origin",
            "http://192.0.2.1:9",
            "--cache-dir",
            str(tmp_path / "cache"),
        ]
    )
    session = prepare_view(args, reachable=lambda url: False)
    try:
        assert session.mode == "offline"
        assert any("unreachable" in message for message in session.messages)
    finally:
        session.server.server_close()


def test_prepare_view_errors_without_dist(tmp_path, monkeypatch):
    monkeypatch.delenv("ATHAR_VIEWER_URL", raising=False)
    monkeypatch.setattr(view_cli_mod, "find_dist", lambda: None)
    args = build_view_parser().parse_args(
        [str(OLD_IFC), str(NEW_IFC), "--offline", "--no-open", "--cache-dir", str(tmp_path / "c")]
    )
    with pytest.raises(FileNotFoundError, match="make viewer-build"):
        prepare_view(args)


def test_prepare_view_missing_input(tmp_path):
    args = build_view_parser().parse_args(
        [str(tmp_path / "missing.ifc"), str(NEW_IFC), "--no-open"]
    )
    with pytest.raises(FileNotFoundError, match="missing.ifc"):
        prepare_view(args)


def test_size_warnings(tmp_path):
    big = tmp_path / "big.ifc"
    big.write_bytes(b"x" * 2048)
    small = tmp_path / "small.ifc"
    small.write_bytes(b"x")
    messages = view_cli_mod._size_warnings([big, small], threshold=1024)
    assert len(messages) == 1
    assert "big.ifc" in messages[0]


def test_find_dist_prefers_explicit_and_requires_index(tmp_path, monkeypatch):
    monkeypatch.delenv("ATHAR_VIEWER_DIST", raising=False)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert find_dist(empty) != empty
    built = tmp_path / "built"
    built.mkdir()
    (built / "index.html").write_text("x", encoding="utf-8")
    assert find_dist(built) == built


def test_install_difftool_writes_git_config(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(global_config=False, no_attributes=True, difftool=True)
    assert git_cli_mod._cmd_install(args) == 0
    configured = subprocess.run(
        ["git", "config", "--get", "difftool.athar.cmd"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert shlex.split(configured) == [
        str(Path(sysconfig.get_path("scripts")) / "athar"), "view", "$LOCAL", "$REMOTE"
    ]


def test_main_dispatches_view_subcommand():
    result = subprocess.run(
        [sys.executable, "-m", "athar", "view", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "athar view" in result.stdout
    assert "--offline" in result.stdout


def test_view_no_open_prints_url_immediately_through_a_pipe(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>viewer</html>", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k not in ("ATHAR_VIEWER_URL", "PYTHONUNBUFFERED")}
    env["ATHAR_VIEWER_DIST"] = str(dist)
    process = subprocess.Popen(
        [sys.executable, "-m", "athar", "view", str(OLD_IFC), str(NEW_IFC), "--no-open",
         "--cache-dir", str(tmp_path / "cache")],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    lines: list[str] = []

    def read_until_viewer_line() -> None:
        for line in process.stdout:
            lines.append(line)
            if line.startswith("Viewer: "):
                return

    reader = threading.Thread(target=read_until_viewer_line, daemon=True)
    reader.start()
    try:
        reader.join(timeout=60)
        viewer_lines = [line for line in lines if line.startswith("Viewer: ")]
        assert viewer_lines, f"no Viewer line while the server runs; got {lines!r}"
        url = viewer_lines[0].removeprefix("Viewer: ").strip()
        status, _, body = _get(url)
        assert status == 200 and b"viewer" in body
    finally:
        process.terminate()
        process.wait(timeout=10)
