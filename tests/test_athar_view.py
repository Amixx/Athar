"""Tests for the athar_view launcher: server endpoints, modes, difftool."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import urllib.error
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

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / "viewer" / "e2e" / "fixtures"
OLD_IFC = FIXTURE_DIR / "old.ifc"
NEW_IFC = FIXTURE_DIR / "new.ifc"


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


def test_build_report_bytes_is_an_engine_report_and_populates_cache(tmp_path):
    cache_dir = tmp_path / "cache"
    payload = json.loads(build_report_bytes(OLD_IFC, NEW_IFC, cache_dir=cache_dir))
    assert payload["engine"] == "athar"
    assert payload["stats"]["added"] == 1
    assert payload["stats"]["deleted"] == 1
    assert list(cache_dir.rglob("*.pickle"))


def test_server_serves_manifest_files_and_cors(report_bytes):
    server = make_server(OLD_IFC, NEW_IFC, report_bytes, dist_dir=None, port=0)
    _serve(server)
    try:
        origin = server.origin

        status, headers, body = _get(f"{origin}/manifest.json")
        assert status == 200
        manifest = json.loads(body)
        assert manifest["athar_viewer_manifest"] == 1
        assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert manifest["old"] == {"name": "old.ifc", "url": "/files/old.ifc"}
        assert manifest["new"] == {"name": "new.ifc", "url": "/files/new.ifc"}
        assert manifest["report"] == {"url": "/files/report.json"}
        assert headers["Access-Control-Allow-Origin"] == "*"
        assert headers["Cross-Origin-Resource-Policy"] == "cross-origin"
        assert headers["Cache-Control"] == "no-store"

        status, _, body = _get(f"{origin}/files/old.ifc")
        assert status == 200
        assert body == OLD_IFC.read_bytes()

        status, _, body = _get(f"{origin}/files/report.json")
        assert status == 200
        assert json.loads(body)["engine"] == "athar"

        request = urllib.request.Request(f"{origin}/manifest.json", method="OPTIONS")
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 204
            assert response.headers["Access-Control-Allow-Private-Network"] == "true"

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            _get(f"{origin}/nope")
        assert excinfo.value.code == 404
    finally:
        server.shutdown()
        server.server_close()


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
        origin = server.origin
        status, _, body = _get(f"{origin}/manifest.json")
        assert status == 200
        manifest = json.loads(body)
        assert manifest["athar_viewer_manifest"] == 1
        assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert manifest["active_step"] == 0
        assert len(manifest["steps"]) == 2
        # Shared file: step 0 new url == step 1 old url (mesh-cache dedup).
        assert manifest["steps"][0]["new"]["url"] == manifest["steps"][1]["old"]["url"]
        assert manifest["steps"][0]["report"]["url"] == "/files/report/0.json"

        status, _, body = _get(f"{origin}/files/model/0.ifc")
        assert status == 200 and body == OLD_IFC.read_bytes()

        status, _, body = _get(f"{origin}/files/report/1.json")
        assert status == 200 and json.loads(body)["engine"] == "athar"

        for bad in ("/files/model/9.ifc", "/files/report/9.json", "/files/old.ifc"):
            with pytest.raises(urllib.error.HTTPError) as excinfo:
                _get(f"{origin}{bad}")
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
        assert session.url.startswith(session.server.origin)
        assert "src=" in session.url
        status, _, body = _get(f"{session.server.origin}/")
        assert status == 200 and b"viewer" in body
        status, _, _ = _get(f"{session.server.origin}/manifest.json")
        assert status == 200
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
        session = prepare_view(args)
        try:
            assert session.mode == "hosted"
            assert session.url.startswith(f"{hosted_url}?src=http%3A%2F%2F127.0.0.1%3A")
            assert session.server.dist_dir is None
            assert session.server.allowed_origin == f"http://127.0.0.1:{hosted.server_address[1]}"
        finally:
            session.server.server_close()
    finally:
        hosted.shutdown()
        hosted.server_close()


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
            # RFC 5737 TEST-NET address: connect fails fast.
            "--viewer-url",
            "http://192.0.2.1:9/app",
            "--cache-dir",
            str(tmp_path / "cache"),
        ]
    )
    session = prepare_view(args)
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
    assert configured == 'athar view "$LOCAL" "$REMOTE"'


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
