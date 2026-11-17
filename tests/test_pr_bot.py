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


def _pr_comment(repo, env, *args, check=True):
    return _run(
        [sys.executable, "-m", "athar", "git", "pr-comment", "--dry-run", *args],
        cwd=repo,
        env=env,
        check=check,
    )


def _table_counts(body: str) -> list[dict[str, int]]:
    rows = re.findall(r"^\| ([\d,]+(?: \| [\d,]+){5}) \|$", body, flags=re.MULTILINE)
    return [dict(zip(STAT_COLUMNS, (int(cell.replace(",", "")) for cell in row.split(" | ")))) for row in rows]


def _stats(old, new) -> dict[str, int]:
    stats = diff_files(old, new)["stats"]
    return {name: stats[name] for name in STAT_COLUMNS}


def _init_repo(tmp_path, env):
    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "init", "-q", "-b", "main"], cwd=repo, env=env)
    return repo


def _commit(repo, env, source, message, *, path="model.ifc"):
    shutil.copyfile(source, repo / path)
    _run(["git", "add", "-A"], cwd=repo, env=env)
    _run(["git", "commit", "-q", "-m", message], cwd=repo, env=env)


def test_pr_comment_diffs_against_merge_base_when_base_moved(tmp_path, git_env):
    v0, v1, v2 = corpus_path("bl_v0"), corpus_path("bl_v1"), corpus_path("bl_v2")
    repo = _init_repo(tmp_path, git_env)
    _commit(repo, git_env, v0, "fork point")
    _run(["git", "checkout", "-q", "-b", "feature"], cwd=repo, env=git_env)
    _commit(repo, git_env, v1, "feature edit")
    _run(["git", "checkout", "-q", "main"], cwd=repo, env=git_env)
    _commit(repo, git_env, v2, "base moved on")

    expected = _stats(v0, v1)
    assert expected != _stats(v2, v1)

    result = _pr_comment(repo, git_env, "--base", "main", "--head", "feature", "--cache-dir", str(tmp_path / "c"))

    assert _table_counts(result.stdout) == [expected]


def _lfs_repo(tmp_path, env):
    if shutil.which("git-lfs") is None:
        pytest.skip("git-lfs is not installed; the LFS PR-comment path needs `git lfs smudge`")
    repo = _init_repo(tmp_path, env)
    _run(["git", "lfs", "install", "--local"], cwd=repo, env=env)
    (repo / ".gitattributes").write_text("*.ifc filter=lfs diff=athar -merge\n", encoding="utf-8")
    return repo


def test_pr_comment_diffs_lfs_tracked_ifc(tmp_path, git_env):
    v0, v1 = corpus_path("bl_v0"), corpus_path("bl_v1")
    repo = _lfs_repo(tmp_path, git_env)
    _commit(repo, git_env, v0, "v0")
    _commit(repo, git_env, v1, "v1")
    assert _run(["git", "cat-file", "-p", "HEAD:model.ifc"], cwd=repo, env=git_env).stdout.startswith(
        "version https://git-lfs"
    )

    result = _pr_comment(repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--cache-dir", str(tmp_path / "c"))

    assert _table_counts(result.stdout) == [_stats(v0, v1)]


def test_pr_comment_fails_loudly_naming_file_when_lfs_object_is_missing(tmp_path, git_env):
    v0, v1 = corpus_path("bl_v0"), corpus_path("bl_v1")
    repo = _lfs_repo(tmp_path, git_env)
    _commit(repo, git_env, v0, "v0")
    _commit(repo, git_env, v1, "v1")
    pointer = _run(["git", "cat-file", "-p", "HEAD~1:model.ifc"], cwd=repo, env=git_env).stdout
    oid = re.search(r"oid sha256:([0-9a-f]{64})", pointer).group(1)
    (repo / ".git" / "lfs" / "objects" / oid[:2] / oid[2:4] / oid).unlink()
    base = _run(["git", "rev-parse", "HEAD~1"], cwd=repo, env=git_env).stdout.strip()

    result = _pr_comment(
        repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--cache-dir", str(tmp_path / "c"), check=False
    )

    assert result.returncode == 1
    assert _table_counts(result.stdout) == []
    assert f"model.ifc at {base[:12]}" in result.stdout
    assert "Git LFS" in result.stdout
    assert f"model.ifc at {base[:12]}" in result.stderr


def test_pr_comment_requires_explicit_base_and_head(tmp_path, git_env):
    repo = _init_repo(tmp_path, git_env)
    _commit(repo, git_env, corpus_path("bl_v0"), "v0")
    _commit(repo, git_env, corpus_path("bl_v1"), "v1")
    env = {**git_env, "GITHUB_BASE_SHA": "HEAD~1", "GITHUB_BASE_REF_SHA": "HEAD~1", "GITHUB_SHA": "HEAD"}

    result = _pr_comment(repo, env, "--cache-dir", str(tmp_path / "c"), check=False)

    assert result.returncode != 0
    assert "--base" in result.stderr
    assert "Athar IFC Diff" not in result.stdout


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


def _multi_file_pr(tmp_path, env, pairs):
    repo = _init_repo(tmp_path, env)
    for name, (old, _) in pairs.items():
        shutil.copyfile(old, repo / name)
    _run(["git", "add", "-A"], cwd=repo, env=env)
    _run(["git", "commit", "-q", "-m", "base"], cwd=repo, env=env)
    for name, (_, new) in pairs.items():
        shutil.copyfile(new, repo / name)
    _run(["git", "commit", "-q", "-am", "head"], cwd=repo, env=env)
    return repo


def _renamed_products(tmp_path, source) -> str:
    import ifcopenshell

    renamed = tmp_path / "renamed.ifc"
    model = ifcopenshell.open(str(source))
    for product in model.by_type("IfcProduct"):
        product.Name = (product.Name or "") + " renamed"
    model.write(str(renamed))
    return str(renamed)


def _file_section(body: str, name: str) -> str:
    start = body.index(f"### `{name}`")
    end = body.find("\n### ", start + 1)
    return body[start : end if end != -1 else len(body)]


def test_pr_comment_attaches_policy_verdicts_to_their_files(tmp_path, git_env):
    pairs = {
        "a.ifc": (corpus_path("gni_50"), _renamed_products(tmp_path, corpus_path("gni_50"))),
        "b.ifc": (corpus_path("gni_9"), corpus_path("gni_190")),
    }
    repo = _multi_file_pr(tmp_path, git_env, pairs)
    args = []
    for name, (old, new) in pairs.items():
        check = _run(
            [sys.executable, "-m", "athar", "check", str(old), str(new), "--policy", "no-deletions"],
            cwd=repo, env=git_env, check=False,
        )
        result_path = tmp_path / f"{name}.check.json"
        result_path.write_text(check.stdout, encoding="utf-8")
        args += ["--policy-result", f"{name}={result_path}"]
    verdicts = {name: json.loads((tmp_path / f"{name}.check.json").read_text()) for name in pairs}
    assert verdicts["a.ifc"]["ok"] and verdicts["b.ifc"]["violations"]

    body = _pr_comment(repo, git_env, "--base", "HEAD~1", "--head", "HEAD", *args).stdout

    assert "Policy gate: `fail` (1 of 2 checked file(s) failed)" in body
    for name, verdict in verdicts.items():
        section = _file_section(body, name)
        assert f"Policy: `{'pass' if verdict['ok'] else 'fail'}` ({len(verdict['violations'])} violation(s))" in section
        for violation in verdict["violations"]:
            assert f"- `{violation['message']}`" in section
    assert verdicts["b.ifc"]["violations"][0]["message"] not in _file_section(body, "a.ifc")


def test_pr_comment_rejects_policy_result_for_an_unchanged_file(tmp_path, git_env):
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": (corpus_path("bl_v0"), corpus_path("bl_v1"))})
    result_path = tmp_path / "check.json"
    result_path.write_text(json.dumps({"ok": True, "violations": []}), encoding="utf-8")

    result = _pr_comment(
        repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--policy-result", f"other.ifc={result_path}", check=False
    )

    assert result.returncode == 1
    assert "other.ifc" in result.stderr


def test_pr_comment_fits_the_budget_cutting_details_then_modified_then_added(tmp_path, git_env):
    pairs = {
        "a.ifc": (corpus_path("gni_50"), _renamed_products(tmp_path, corpus_path("gni_50"))),
        "b.ifc": (corpus_path("gni_9"), corpus_path("gni_190")),
        "c.ifc": (corpus_path("gni_50"), corpus_path("gni_62")),
    }
    repo = _multi_file_pr(tmp_path, git_env, pairs)

    body = _pr_comment(repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--max-items", "10000").stdout

    assert len(body) <= pr_bot.COMMENT_BUDGET
    assert _table_counts(body) == [_stats(old, new) for old, new in pairs.values()]
    modified = _stats(*pairs["a.ifc"])["modified"]
    assert f"... and {modified} more modified not shown" in body
    assert "  data:" not in body
    assert re.search(r"^\.\.\. and \d+ more (added|deleted) not shown$", body, flags=re.MULTILINE)
    assert re.search(r"^[+-] Ifc", body, flags=re.MULTILINE)


def _github_env(env, github, **extra):
    env = {key: value for key, value in env.items() if not key.startswith("GITHUB_")}
    return {**env, "GITHUB_API_URL": github.url, "NO_PROXY": "127.0.0.1", **extra}


def test_pr_comment_writes_job_summary_when_comment_writes_are_forbidden(tmp_path, git_env, github):
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": (corpus_path("bl_v0"), corpus_path("bl_v1"))})
    github.forbid_writes = True
    summary = tmp_path / "summary.md"
    env = _github_env(git_env, github, GITHUB_STEP_SUMMARY=str(summary))

    result = _run(
        [sys.executable, "-m", "athar", "git", "pr-comment", "--base", "HEAD~1", "--head", "HEAD",
         "--repo", "o/r", "--pr", "7", "--token", "t"],
        cwd=repo, env=env, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "job summary" in result.stdout
    written = summary.read_text(encoding="utf-8")
    assert pr_bot.COMMENT_MARKER in written
    assert _table_counts(written) == [_stats(corpus_path("bl_v0"), corpus_path("bl_v1"))]
    assert github.comments == []


def test_pr_comment_forbidden_write_without_job_summary_fails(tmp_path, git_env, github):
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": (corpus_path("bl_v0"), corpus_path("bl_v1"))})
    github.forbid_writes = True

    result = _run(
        [sys.executable, "-m", "athar", "git", "pr-comment", "--base", "HEAD~1", "--head", "HEAD",
         "--repo", "o/r", "--pr", "7", "--token", "t"],
        cwd=repo, env=_github_env(git_env, github), check=False,
    )

    assert result.returncode == 1
    assert "HTTP 403" in result.stderr


def _comment_for_reports(monkeypatch, reports: dict[str, dict]):
    monkeypatch.setattr(pr_bot, "_diff_git_paths", lambda base, head, *, old_path, new_path, cache_dir: reports[new_path])
    return pr_bot.render_pr_comment(
        [pr_bot.IfcChange(status="M", old_path=path, new_path=path) for path in reports],
        base="a" * 40, head="b" * 40, cache_dir=None, max_files=20, max_items=80,
    ).body


def test_pr_comment_budget_bounds_cohort_lines_and_long_names(monkeypatch):
    cohorts = [{"count": 2, "members": [2 * n, 2 * n + 1], "delta_mm": [n + 1, 0, 0]} for n in range(20_000)]
    long_name = "P" * 100_000
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"modified": 40_000, "placement_cohorts": cohorts},
        "orphan_property_sets": {"added": [{"name": long_name}], "removed": []},
    }

    body = _comment_for_reports(monkeypatch, {"a.ifc": report})

    assert len(body) <= pr_bot.COMMENT_BUDGET
    assert re.search(r"^\.\.\. and [\d,]+ more placement cohorts \([\d,]+ elements\) not shown$", body, flags=re.MULTILINE)
    assert "P" * 201 not in body
    assert "...[99,800 more characters]" in body


def test_pr_comment_over_budget_names_the_file_that_cannot_fit(monkeypatch):
    added = [{"class": f"IfcClass{n:05d}", "step_id": n} for n in range(6_000)]
    small = {"schemas": {"old": "IFC4", "new": "IFC4"}, "stats": {}}
    big = {"schemas": {"old": "IFC4", "new": "IFC4"}, "stats": {"added": len(added)}, "added": added}

    with pytest.raises(RuntimeError, match=r"the section for big\.ifc alone is [\d,]+ characters") as error:
        _comment_for_reports(monkeypatch, {"small.ifc": small, "big.ifc": big})
    assert "--max-files" not in str(error.value)


def test_pr_comment_header_over_budget_is_an_error_naming_the_policy_verdicts():
    paths = [f"m{n:04d}.ifc" for n in range(1_500)]

    with pytest.raises(RuntimeError, match=r"comment header alone is [\d,]+ characters.*1500 files.*fewer --policy-result"):
        pr_bot.render_pr_comment(
            [pr_bot.IfcChange(status="M", old_path=path, new_path=path) for path in paths],
            base="a" * 40, head="b" * 40, cache_dir=None, max_files=0, max_items=80,
            policy_results={path: {"ok": True, "violations": []} for path in paths},
        )


def test_pr_comment_fails_on_a_rate_limit_403_even_with_a_job_summary(tmp_path, git_env, github):
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": (corpus_path("bl_v0"), corpus_path("bl_v1"))})
    github.forbid_writes = True
    github.forbidden_message = "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."
    summary = tmp_path / "summary.md"

    result = _run(
        [sys.executable, "-m", "athar", "git", "pr-comment", "--base", "HEAD~1", "--head", "HEAD",
         "--repo", "o/r", "--pr", "7", "--token", "t"],
        cwd=repo, env=_github_env(git_env, github, GITHUB_STEP_SUMMARY=str(summary)), check=False,
    )

    assert result.returncode == 1
    assert "secondary rate limit" in result.stderr
    assert not summary.exists()


@pytest.mark.parametrize(
    ("result", "field"),
    [({"violations": []}, "'ok'"), ({"ok": False}, "'violations'"), ({"ok": False, "violations": [{}]}, "'message'")],
)
def test_pr_comment_names_the_policy_file_and_missing_field(tmp_path, git_env, result, field):
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": (corpus_path("bl_v0"), corpus_path("bl_v1"))})
    result_path = tmp_path / "check.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")

    run = _pr_comment(
        repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--policy-result", f"a.ifc={result_path}", check=False
    )

    assert run.returncode == 1
    assert str(result_path) in run.stderr and field in run.stderr


def test_pr_comment_shows_policy_verdicts_of_files_skipped_by_max_files(tmp_path, git_env):
    pair = (corpus_path("bl_v0"), corpus_path("bl_v1"))
    repo = _multi_file_pr(tmp_path, git_env, {"a.ifc": pair, "b.ifc": pair})
    args = []
    for name, result in {"a.ifc": {"ok": True, "violations": []},
                         "b.ifc": {"ok": False, "violations": [{"message": "x"}, {"message": "y"}]}}.items():
        result_path = tmp_path / f"{name}.json"
        result_path.write_text(json.dumps(result), encoding="utf-8")
        args += ["--policy-result", f"{name}={result_path}"]

    body = _pr_comment(repo, git_env, "--base", "HEAD~1", "--head", "HEAD", "--max-files", "1", *args).stdout

    assert "Policy gate: `fail` (1 of 2 checked file(s) failed)" in body
    assert "### `b.ifc`" not in body
    assert "- `b.ifc`: Policy: `fail` (2 violation(s))" in body
    assert "- `a.ifc`:" not in body
