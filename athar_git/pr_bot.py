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

from athar.bottom.types import SignatureBundle
from athar.engine import diff_bundles

from .cache import load_or_build_bundle
from .render import DEFAULT_MAX_ITEMS, clip, markdown_block, markdown_code, render_markdown_report

COMMENT_MARKER = "<!-- athar-pr-comment -->"
DEFAULT_MAX_FILES = 20
COMMENT_BUDGET = 60_000


@dataclass(frozen=True)
class IfcChange:
    status: str
    old_path: str | None
    new_path: str | None


@dataclass(frozen=True)
class RenderedComment:
    body: str
    failures: list[str]


def add_pr_comment_parser(git_sub: argparse._SubParsersAction) -> None:
    parser = git_sub.add_parser(
        "pr-comment",
        help="Post or update a GitHub PR comment with semantic IFC diff summaries",
    )
    parser.add_argument("--base", required=True, help="Base git ref/sha for the PR diff")
    parser.add_argument("--head", required=True, help="Head git ref/sha for the PR diff")
    parser.add_argument("--repo", help="GitHub repository in owner/name form")
    parser.add_argument("--pr", type=int, help="Pull request number")
    parser.add_argument("--token", help="GitHub token; defaults to GITHUB_TOKEN")
    parser.add_argument("--cache-dir", help="Override persistent signature cache directory")
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES, help="Maximum IFC files to process")
    parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS, help="Maximum changed entities per file")
    parser.add_argument(
        "--policy-result",
        action="append",
        metavar="IFC_PATH=RESULT_JSON",
        help="athar check JSON result for one changed IFC path, shown in that file's section; repeatable",
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
    policy_results = _load_policy_results(args.policy_result)
    merge_base = _git_text(["merge-base", args.base, args.head]).strip()
    changes = discover_ifc_changes(merge_base, args.head)
    comment = render_pr_comment(
        changes,
        base=merge_base,
        head=args.head,
        cache_dir=args.cache_dir,
        max_files=args.max_files,
        max_items=args.max_items,
        policy_results=policy_results,
    )

    if args.dry_run:
        print(comment.body)
        return _report_failures(comment.failures)

    repo = args.repo or os.environ.get("GITHUB_REPOSITORY")
    pr_number = args.pr or _event_pr_number()
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not repo:
        raise ValueError("pr-comment requires --repo or GITHUB_REPOSITORY")
    if pr_number is None:
        raise ValueError("pr-comment requires --pr or GITHUB_EVENT_PATH for a pull_request event")
    if not token:
        raise ValueError("pr-comment requires --token or GITHUB_TOKEN")

    client = GitHubClient(repo=repo, token=token)
    try:
        client.upsert_issue_comment(pr_number, comment.body)
    except GitHubAPIError as exc:
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if not exc.integration_forbidden or exc.method == "GET" or not summary_path:
            raise
        notice = "The token cannot write PR comments (fork pull request?); the Athar diff is in the job summary."
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(f"{notice}\n\n{comment.body}")
        print(f"::notice::{notice}")
        return _report_failures(comment.failures)
    print(f"Updated Athar PR comment for {repo}#{pr_number}")
    return _report_failures(comment.failures)


def _report_failures(failures: list[str]) -> int:
    for failure in failures:
        print(f"Error: {failure}", file=sys.stderr)
    return 1 if failures else 0


def discover_ifc_changes(base: str, head: str) -> list[IfcChange]:
    output = _git(["diff", "--name-status", "-z", base, head])
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
    policy_results: dict[str, dict] | None = None,
) -> RenderedComment:
    policy_results = policy_results or {}
    max_files = max(0, int(max_files))
    shown = changes[:max_files]
    hidden = max(0, len(changes) - len(shown))
    paths = {change.new_path or change.old_path for change in changes}
    unmatched = sorted(set(policy_results) - paths)
    if unmatched:
        raise ValueError(f"--policy-result names files that did not change: {', '.join(unmatched)}")

    header = [
        COMMENT_MARKER,
        "## Athar IFC Diff",
        "",
        f"Compared {markdown_code(_short_ref(base))} to {markdown_code(_short_ref(head))}.",
    ]
    if policy_results:
        failed = sum(1 for result in policy_results.values() if not result["ok"])
        verdict = "fail" if failed else "pass"
        header.extend(
            ["", f"Policy gate: {markdown_code(verdict)} ({failed} of {len(policy_results)} checked file(s) failed)"]
        )

    if not changes:
        header.extend(["", "No `.ifc` files changed in this PR."])
        return RenderedComment(body="\n".join(header) + "\n", failures=[])

    header.extend(["", f"Found {len(changes)} changed IFC file(s)."])
    hidden_policy: list[tuple[str, dict]] = []
    if hidden:
        header.append(f"Showing the first {len(shown)}; {hidden} more skipped.")
        hidden_policy = [
            (path, policy_results[path])
            for change in changes[len(shown):]
            if (path := change.new_path or change.old_path) in policy_results
        ]
        if hidden_policy:
            header.extend(["", "Policy results for skipped files:"])
            header.extend(f"- {markdown_code(path)}: {_policy_verdict(result)}" for path, result in hidden_policy)

    header_length = len("\n".join(header)) + 1
    if header_length > COMMENT_BUDGET:
        raise RuntimeError(
            f"the comment header alone is {header_length:,} characters, over the {COMMENT_BUDGET:,}-character "
            f"PR comment budget: it lists policy verdicts for {len(hidden_policy)} files skipped by --max-files; "
            "pass fewer --policy-result files"
        )
    failures: list[str] = []
    sections: list[tuple[str, list[str], dict | None]] = []
    for change in shown:
        path = change.new_path or change.old_path
        lines = ["", f"### {markdown_code(path)}"]
        if path in policy_results:
            lines.extend(_policy_lines(policy_results[path]))
        report = None
        if change.old_path is None:
            lines.append(f"File added (`{change.status}`). Semantic pair diff requires both old and new IFC files.")
        elif change.new_path is None:
            lines.append(f"File deleted (`{change.status}`). Semantic pair diff requires both old and new IFC files.")
        else:
            try:
                report = _diff_git_paths(
                    base,
                    head,
                    old_path=change.old_path,
                    new_path=change.new_path,
                    cache_dir=cache_dir,
                )
            except Exception as exc:
                failures.append(str(exc))
                lines.extend(
                    [
                        "<details open>",
                        "<summary>Diff failed</summary>",
                        "",
                        markdown_block(str(exc)),
                        "</details>",
                    ]
                )
        sections.append((path, lines, report))

    head = "\n".join(header) + "\n"
    for max_listed, max_modified, max_cohorts, details in _budget_levels(max(0, int(max_items))):
        rendered_sections = []
        for path, section_lines, report in sections:
            lines = list(section_lines)
            if report is not None:
                rendered = render_markdown_report(
                    report, max_items=max_listed, max_modified=max_modified, max_cohorts=max_cohorts, details=details
                )
                lines.append(rendered.rstrip())
            rendered_sections.append((path, "\n".join(lines) + "\n"))
        body = head + "".join(text for _, text in rendered_sections)
        if len(body) <= COMMENT_BUDGET:
            return RenderedComment(body=body, failures=failures)
    path, largest = max(rendered_sections, key=lambda pair: len(pair[1]))
    if len(head) + len(largest) > COMMENT_BUDGET:
        raise RuntimeError(
            f"the section for {path} alone is {len(largest):,} characters without entity lists, "
            f"over the {COMMENT_BUDGET:,}-character PR comment budget"
        )
    raise RuntimeError(
        f"the PR comment for {len(sections)} files exceeds {COMMENT_BUDGET:,} characters without entity lists "
        f"(largest section: {path}, {len(largest):,} characters); lower --max-files"
    )


def _budget_levels(max_items: int) -> list[tuple[int, int, int, bool]]:
    """(deleted/added cap, modified cap, cohort cap, detail rows) from fullest to most compact."""

    def halvings(start: int) -> list[int]:
        steps = []
        while start > 0:
            start //= 2
            steps.append(start)
        return steps

    levels = [(max_items, max_items, max_items, True), (max_items, max_items, max_items, False)]
    levels.extend((max_items, cap, max_items, False) for cap in halvings(max_items))
    levels.extend((max_items, 0, cap, False) for cap in halvings(max_items))
    levels.extend((cap, 0, 0, False) for cap in halvings(max_items))
    return levels


def _diff_git_paths(
    base: str,
    head: str,
    *,
    old_path: str,
    new_path: str,
    cache_dir: str | os.PathLike[str] | None,
) -> dict:
    with tempfile.TemporaryDirectory(prefix="athar-pr-") as tmp:
        old_bundle = _load_git_bundle(base, old_path, Path(tmp) / "old.ifc", cache_dir)
        new_bundle = _load_git_bundle(head, new_path, Path(tmp) / "new.ifc", cache_dir)
        return diff_bundles(old_bundle, new_bundle)


def _load_git_bundle(
    ref: str, path: str, scratch: Path, cache_dir: str | os.PathLike[str] | None
) -> SignatureBundle:
    try:
        oid = _git_text(["rev-parse", f"{ref}:{path}"]).strip()
        scratch.write_bytes(_git(["show", f"{ref}:{path}"]))
        return load_or_build_bundle(scratch, key=oid, cache_dir=cache_dir).bundle
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{path} at {_short_ref(ref)}: {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"{path} at {_short_ref(ref)}: {exc}") from exc


def _policy_lines(result: dict) -> list[str]:
    lines = [_policy_verdict(result)]
    lines.extend(f"- {markdown_code(clip(violation['message']))}" for violation in result["violations"])
    return lines


def _policy_verdict(result: dict) -> str:
    verdict = "pass" if result["ok"] else "fail"
    return f"Policy: {markdown_code(verdict)} ({len(result['violations'])} violation(s))"


def _load_policy_results(specs: list[str] | None) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for spec in specs or []:
        path, separator, result_path = spec.partition("=")
        if not separator or not path or not result_path:
            raise ValueError(f"--policy-result expects IFC_PATH=RESULT_JSON, got {spec!r}")
        if path in results:
            raise ValueError(f"--policy-result given twice for {path}")
        results[path] = _policy_result(Path(result_path))
    return results


def _policy_result(path: Path) -> dict:
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise ValueError(f"--policy-result {path} is not JSON: {exc}") from exc
    if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
        raise ValueError(f"--policy-result {path} has no boolean 'ok' field; expected an athar check JSON result")
    violations = result.get("violations")
    if not isinstance(violations, list):
        raise ValueError(f"--policy-result {path} has no 'violations' list; expected an athar check JSON result")
    for index, violation in enumerate(violations):
        if not isinstance(violation, dict) or not isinstance(violation.get("message"), str):
            raise ValueError(f"--policy-result {path} violation {index} has no string 'message' field")
    return result


def _event_pr_number() -> int | None:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    number = (event.get("pull_request") or {}).get("number") or event.get("number")
    return int(number) if number is not None else None


def _is_ifc_path(path: str | None) -> bool:
    return bool(path and path.lower().endswith(".ifc"))


def _short_ref(value: str) -> str:
    return value[:12] if len(value) >= 20 and all(char in "0123456789abcdef" for char in value.lower()) else value


def _git(args: list[str]) -> bytes:
    result = subprocess.run(["git", *args], check=True, capture_output=True)
    return result.stdout


def _git_text(args: list[str]) -> str:
    return _git(args).decode("utf-8", errors="replace")


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


def _next_link(header: str | None) -> str | None:
    for part in (header or "").split(","):
        target, _, params = part.partition(";")
        if 'rel="next"' in params:
            return target.strip().strip("<>")
    return None
