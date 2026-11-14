from __future__ import annotations

import json
import re
import shutil
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

import athar_git.pr_bot as pr_bot

from athar.engine import diff_files
from tests.corpus import corpus_path
from tests.test_git_end_to_end import _run, git_env  # noqa: F401  (git_env is a fixture)

STAT_COLUMNS = ("added", "deleted", "modified", "unchanged", "old_signatures", "new_signatures")


class FakeGitHub:
    """In-process GitHub REST fake for the issue-comment endpoints the bot uses."""

    def __init__(self, *, login: str | None = "athar-user", page_size: int = 2) -> None:
        self.login = login
        self.page_size = page_size
        self.comments: list[dict] = []
        self.calls: list[tuple[str, str]] = []
        self.on_post = None
        self.forbid_writes = False
        self.forbidden_message = "Resource not accessible by integration"
        self.patch_missing: set[int] = set()
        self._next_id = 1000
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, payload=None, headers=None):
                body = b"" if payload is None else json.dumps(payload).encode()
                self.send_response(status)
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _payload(self):
                length = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(length)) if length else None

            def do_GET(self):
                url = urlparse(self.path)
                fake.calls.append(("GET", url.path))
                if url.path == "/user":
                    if fake.login is None:
                        return self._send(403, {"message": "Resource not accessible by integration"})
                    return self._send(200, {"login": fake.login})
                page = int(parse_qs(url.query).get("page", ["1"])[0])
                start = (page - 1) * fake.page_size
                chunk = fake.comments[start : start + fake.page_size]
                headers = {}
                if start + fake.page_size < len(fake.comments):
                    headers["Link"] = f'<{fake.url}{url.path}?per_page=100&page={page + 1}>; rel="next"'
                return self._send(200, chunk, headers)

            def do_POST(self):
                fake.calls.append(("POST", self.path))
                if fake.forbid_writes:
                    return self._send(403, {"message": fake.forbidden_message})
                body = self._payload()["body"]
                if fake.on_post:
                    fake.on_post()
                created = fake.add(fake.login or pr_bot.GITHUB_ACTIONS_LOGIN, body)
                return self._send(201, created)

            def do_PATCH(self):
                fake.calls.append(("PATCH", self.path))
                if fake.forbid_writes:
                    return self._send(403, {"message": fake.forbidden_message})
                comment_id = int(self.path.rsplit("/", 1)[1])
                comment = next((c for c in fake.comments if c["id"] == comment_id), None)
                if comment is None or comment_id in fake.patch_missing:
                    fake.comments = [c for c in fake.comments if c["id"] != comment_id]
                    return self._send(404, {"message": "Not Found"})
                comment["body"] = self._payload()["body"]
                return self._send(200, comment)

            def do_DELETE(self):
                fake.calls.append(("DELETE", self.path))
                comment_id = int(self.path.rsplit("/", 1)[1])
                fake.comments = [c for c in fake.comments if c["id"] != comment_id]
                return self._send(204)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def add(self, login: str, body: str, comment_id: int | None = None) -> dict:
        if comment_id is None:
            self._next_id += 1
            comment_id = self._next_id
        comment = {"id": comment_id, "user": {"login": login}, "body": body}
        self.comments.append(comment)
        self.comments.sort(key=lambda c: c["id"])
        return comment

    def bodies(self) -> dict[int, str]:
        return {c["id"]: c["body"] for c in self.comments}


@pytest.fixture
def github(monkeypatch):
    fake = FakeGitHub()
    monkeypatch.setenv("GITHUB_API_URL", fake.url)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    yield fake
    fake.server.shutdown()


MARKED = "old\n" + pr_bot.COMMENT_MARKER


def test_upsert_updates_own_marker_comment_on_a_later_page(github):
    github.add("someone-else", "quoting " + pr_bot.COMMENT_MARKER, 1)
    github.add("athar-user", "unrelated", 2)
    github.add("athar-user", "unrelated", 3)
    github.add("athar-user", MARKED, 4)

    pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "new " + pr_bot.COMMENT_MARKER)

    assert github.bodies()[4] == "new " + pr_bot.COMMENT_MARKER
    assert github.bodies()[1] == "quoting " + pr_bot.COMMENT_MARKER
    assert not any(method == "POST" for method, _ in github.calls)


def test_upsert_creates_when_only_other_authors_carry_the_marker(github):
    github.add("someone-else", MARKED, 1)

    pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "new " + pr_bot.COMMENT_MARKER)

    assert github.bodies()[1] == MARKED
    assert [c["user"]["login"] for c in github.comments] == ["someone-else", "athar-user"]


def test_upsert_identifies_actions_token_as_github_actions_bot(github, monkeypatch):
    github.login = None
    github.add(pr_bot.GITHUB_ACTIONS_LOGIN, MARKED, 5)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_TOKEN", "actions-token")

    pr_bot.GitHubClient(repo="o/r", token="actions-token").upsert_issue_comment(7, "new " + pr_bot.COMMENT_MARKER)

    assert github.bodies() == {5: "new " + pr_bot.COMMENT_MARKER}


def test_upsert_rejects_unidentifiable_token(github):
    github.login = None

    with pytest.raises(RuntimeError, match="cannot resolve the GitHub login"):
        pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "body")


def test_upsert_converges_on_one_comment_when_a_concurrent_run_created_first(github):
    github.on_post = lambda: github.add("athar-user", "concurrent " + pr_bot.COMMENT_MARKER, 10)

    pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "mine " + pr_bot.COMMENT_MARKER)

    assert github.bodies() == {10: "mine " + pr_bot.COMMENT_MARKER}


def test_upsert_relists_when_the_marker_comment_vanished(github):
    github.add("athar-user", MARKED, 4)
    github.patch_missing.add(4)

    pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "new " + pr_bot.COMMENT_MARKER)

    assert list(github.bodies().values()) == ["new " + pr_bot.COMMENT_MARKER]


def test_upsert_keeps_the_oldest_own_marker_comment_and_deletes_the_others(github):
    github.add("athar-user", MARKED, 4)
    github.add("someone-else", MARKED, 5)
    github.add("athar-user", MARKED, 6)
    github.add("athar-user", "unrelated", 7)
    github.add("athar-user", MARKED, 8)

    pr_bot.GitHubClient(repo="o/r", token="t").upsert_issue_comment(7, "new " + pr_bot.COMMENT_MARKER)

    assert github.bodies() == {4: "new " + pr_bot.COMMENT_MARKER, 5: MARKED, 7: "unrelated"}


