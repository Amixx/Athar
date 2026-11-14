"""GitHub PR comment delivery for Athar IFC diffs."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from athar.engine import diff_bundles

from .cache import load_or_build_bundle
from .render import DEFAULT_MAX_ITEMS, render_markdown_report

COMMENT_MARKER = "<!-- athar-pr-comment -->"
DEFAULT_MAX_FILES = 20


@dataclass(frozen=True)
class IfcChange:
    status: str
    old_path: str | None
    new_path: str | None


def add_pr_comment_parser(git_sub: argparse._SubParsersAction) -> None:
    parser = git_sub.add_parser(
        "pr-comment",
        help="Post or update a GitHub PR comment with semantic IFC diff summaries",
    )
    parser.add_argument("--base", help="Base git ref/sha for the PR diff")
    parser.add_argument("--head", help="Head git ref/sha for the PR diff")
    parser.add_argument("--repo", help="GitHub repository in owner/name form")
    parser.add_argument("--pr", type=int, help="Pull request number")
    parser.add_argument("--token", help="GitHub token; defaults to GITHUB_TOKEN")
    parser.add_argument("--cache-dir", help="Override persistent signature cache directory")
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES, help="Maximum IFC files to process")
    parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS, help="Maximum changed entities per file")
    parser.add_argument(
        "--policy-result",
        help="Optional athar check JSON result to summarize in the comment",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the comment body without calling GitHub")
    parser.set_defaults(func=cmd_pr_comment)


def cmd_pr_comment(args: argparse.Namespace) -> int:
    try:
        return _cmd_pr_comment(args)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _cmd_pr_comment(args: argparse.Namespace) -> int:
    base = args.base or _env("GITHUB_BASE_REF_SHA", "GITHUB_BASE_SHA")
    head = args.head or _env("GITHUB_HEAD_REF_SHA", "GITHUB_SHA")
    repo = args.repo or os.environ.get("GITHUB_REPOSITORY")
    pr_number = args.pr or _event_pr_number()
    token = args.token or os.environ.get("GITHUB_TOKEN")

    if not base or not head:
        raise ValueError("pr-comment requires --base/--head or GitHub Actions base/head environment values")
    changes = discover_ifc_changes(base, head)
    body = render_pr_comment(
        changes,
        base=base,
        head=head,
        cache_dir=args.cache_dir,
        max_files=args.max_files,
        max_items=args.max_items,
        policy_result_path=args.policy_result,
    )

    if args.dry_run:
        print(body)
        return 0

    if not repo:
        raise ValueError("pr-comment requires --repo or GITHUB_REPOSITORY")
    if pr_number is None:
        raise ValueError("pr-comment requires --pr or GITHUB_EVENT_PATH for a pull_request event")
    if not token:
        raise ValueError("pr-comment requires --token or GITHUB_TOKEN")

    client = GitHubClient(repo=repo, token=token)
    client.upsert_issue_comment(pr_number, body)
    print(f"Updated Athar PR comment for {repo}#{pr_number}")
    return 0


def discover_ifc_changes(base: str, head: str) -> list[IfcChange]:
    output = _git(["diff", "--name-status", "-z", f"{base}...{head}"])
    tokens = output.decode("utf-8", errors="replace").split("\0")
    changes: list[IfcChange] = []
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        if not status:
            continue
        code = status[0]
        if code in {"R", "C"}:
            if index + 1 >= len(tokens):
                break
            old_path = tokens[index]
            new_path = tokens[index + 1]
            index += 2
        else:
            if index >= len(tokens):
                break
            path = tokens[index]
            index += 1
            old_path = path if code != "A" else None
            new_path = path if code != "D" else None
        if _is_ifc_path(old_path) or _is_ifc_path(new_path):
            changes.append(IfcChange(status=status, old_path=old_path, new_path=new_path))
    return changes


def render_pr_comment(
    changes: list[IfcChange],
    *,
    base: str,
    head: str,
    cache_dir: str | os.PathLike[str] | None,
    max_files: int,
    max_items: int,
    policy_result_path: str | None = None,
) -> str:
    max_files = max(0, int(max_files))
    shown = changes[:max_files]
    hidden = max(0, len(changes) - len(shown))
    lines = [
        COMMENT_MARKER,
        "## Athar IFC Diff",
        "",
        f"Compared `{_short_ref(base)}` to `{_short_ref(head)}`.",
    ]

    policy_line = _policy_summary(policy_result_path)
    if policy_line:
        lines.extend(["", policy_line])

    if not changes:
        lines.extend(["", "No `.ifc` files changed in this PR."])
        return "\n".join(lines) + "\n"

    lines.extend(["", f"Found {len(changes)} changed IFC file(s)."])
    if hidden:
        lines.append(f"Showing the first {len(shown)}; {hidden} more skipped.")

    for change in shown:
        path = change.new_path or change.old_path or "unknown.ifc"
        lines.extend(["", f"### `{path}`"])
        if change.old_path is None:
            lines.append(f"File added (`{change.status}`). Semantic pair diff requires both old and new IFC files.")
            continue
        if change.new_path is None:
            lines.append(f"File deleted (`{change.status}`). Semantic pair diff requires both old and new IFC files.")
            continue
        try:
            report = _diff_git_paths(
                base,
                head,
                old_path=change.old_path,
                new_path=change.new_path,
                cache_dir=cache_dir,
            )
        except Exception as exc:
            lines.extend(
                [
                    "<details open>",
                    "<summary>Diff failed</summary>",
                    "",
                    "```text",
                    str(exc),
                    "```",
                    "</details>",
                ]
            )
            continue
        lines.append(render_markdown_report(report, max_items=max_items).rstrip())

    return "\n".join(lines) + "\n"


def _diff_git_paths(
    base: str,
    head: str,
    *,
    old_path: str,
    new_path: str,
    cache_dir: str | os.PathLike[str] | None,
) -> dict:
    old_oid = _git_text(["rev-parse", f"{base}:{old_path}"]).strip()
    new_oid = _git_text(["rev-parse", f"{head}:{new_path}"]).strip()
    with tempfile.TemporaryDirectory(prefix="athar-pr-") as tmp:
        old_file = Path(tmp) / "old.ifc"
        new_file = Path(tmp) / "new.ifc"
        old_file.write_bytes(_git(["show", f"{base}:{old_path}"]))
        new_file.write_bytes(_git(["show", f"{head}:{new_path}"]))
        old_result = load_or_build_bundle(old_file, key=old_oid, cache_dir=cache_dir)
        new_result = load_or_build_bundle(new_file, key=new_oid, cache_dir=cache_dir)
        return diff_bundles(old_result.bundle, new_result.bundle)


def _policy_summary(path: str | None) -> str | None:
    if not path:
        return None
    try:
        result = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        return f"Policy gate: `unknown` (could not read `{path}`: {exc})"
    ok = bool(result.get("ok"))
    violations = result.get("violations") or []
    if ok:
        return "Policy gate: `pass`"
    return f"Policy gate: `fail` ({len(violations)} violation(s))"


def _event_pr_number() -> int | None:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    try:
        event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except Exception:
        return None
    number = (event.get("pull_request") or {}).get("number") or event.get("number")
    return int(number) if number is not None else None


def _env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _is_ifc_path(path: str | None) -> bool:
    return bool(path and path.lower().endswith(".ifc"))


def _short_ref(value: str) -> str:
    return value[:12] if len(value) >= 20 and all(char in "0123456789abcdef" for char in value.lower()) else value


def _git(args: list[str]) -> bytes:
    result = subprocess.run(["git", *args], check=True, capture_output=True)
    return result.stdout


def _git_text(args: list[str]) -> str:
    return _git(args).decode("utf-8", errors="replace")


GITHUB_ACTIONS_LOGIN = "github-actions[bot]"


class GitHubClient:
    def __init__(self, *, repo: str, token: str) -> None:
        self.repo = repo
        self.token = token
        self.api_root = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
        self._login: str | None = None

    def token_login(self) -> str:
        if self._login is None:
            self._login = self._resolve_login()
        return self._login

    def _resolve_login(self) -> str:
        try:
            user, _ = self._request("GET", self.api_root + "/user")
            return str(user["login"])
        except GitHubAPIError as exc:
            if exc.status != 403:
                raise
            # Installation tokens cannot read /user; the Actions GITHUB_TOKEN is one.
            is_actions_token = os.environ.get("GITHUB_ACTIONS") == "true" and self.token == os.environ.get(
                "GITHUB_TOKEN"
            )
            if not is_actions_token:
                raise RuntimeError(
                    "cannot resolve the GitHub login of the supplied token (GET /user answered 403); "
                    "use a user token or the GitHub Actions GITHUB_TOKEN"
                ) from exc
            return GITHUB_ACTIONS_LOGIN

    def upsert_issue_comment(self, pr_number: int, body: str) -> None:
        comments_url = f"{self.api_root}/repos/{self.repo}/issues/{pr_number}/comments"
        for attempt in range(2):
            own = self._own_marker_comments(comments_url)
            if not own:
                break
            try:
                self._request("PATCH", self._comment_url(own[0]["id"]), {"body": body})
            except GitHubAPIError as exc:
                if exc.status != 404 or attempt:
                    raise
                continue
            self._delete_comments(own[1:])
            return

        created, _ = self._request("POST", comments_url, {"body": body})
        own = self._own_marker_comments(comments_url)
        if own and own[0]["id"] != created["id"]:
            self._request("PATCH", self._comment_url(own[0]["id"]), {"body": body})
        self._delete_comments(own[1:])

    def _delete_comments(self, comments: list[dict]) -> None:
        for comment in comments:
            try:
                self._request("DELETE", self._comment_url(comment["id"]))
            except GitHubAPIError as exc:
                if exc.status != 404:
                    raise

    def _own_marker_comments(self, comments_url: str) -> list[dict]:
        login = self.token_login()
        own = [
            comment
            for comment in self._paginate(comments_url + "?per_page=100")
            if (comment.get("user") or {}).get("login") == login and COMMENT_MARKER in str(comment.get("body", ""))
        ]
        return sorted(own, key=lambda comment: comment["id"])

    def _paginate(self, url: str) -> list[dict]:
        items: list[dict] = []
        next_url: str | None = url
        while next_url:
            page, headers = self._request("GET", next_url)
            items.extend(page)
            next_url = _next_link(headers.get("Link"))
        return items

    def _comment_url(self, comment_id: int) -> str:
        return f"{self.api_root}/repos/{self.repo}/issues/comments/{comment_id}"

    def _request(self, method: str, url: str, payload: dict[str, Any] | None = None) -> tuple[Any, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "athar-pr-bot",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                headers = response.headers
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise GitHubAPIError(method, url, exc.code, detail) from exc
        return (json.loads(body.decode("utf-8")) if body else None), headers


class GitHubAPIError(RuntimeError):
    def __init__(self, method: str, url: str, status: int, detail: str) -> None:
        super().__init__(f"GitHub API {method} {url} failed: HTTP {status}: {detail}")
        self.method = method
        self.status = status
        try:
            message = json.loads(detail).get("message")
        except (ValueError, AttributeError):
            message = None
        self.integration_forbidden = status == 403 and message == "Resource not accessible by integration"


def _next_link(header: str | None) -> str | None:
    for part in (header or "").split(","):
        target, _, params = part.partition(";")
        if 'rel="next"' in params:
            return target.strip().strip("<>")
    return None

