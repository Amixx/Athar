"""Contracts of the repository workflows under .github/workflows/."""

from __future__ import annotations

import json
import itertools
import os
import re
import shlex
import subprocess
import sys

import pytest
import yaml

from tests.corpus import CORPUS, LARGE, REPO_ROOT, default_suite_lfs

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "tests.yml"
PR_DIFF_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "athar-pr-diff.yml"
RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "python-release.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-acceptance.yml"
DASHBOARD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-dashboard.yml"
TINY_IFC = REPO_ROOT / "tests" / "fixtures" / "tiny_no_products.ifc"


def _rel(path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def test_default_suite_lfs_excludes_the_large_tier():
    listed = set(default_suite_lfs())
    large_repo = {_rel(f.path) for f in CORPUS.values() if f.tier == LARGE and f.required}
    assert not listed & large_repo


def test_default_suite_lfs_files_are_lfs_tracked():
    out = subprocess.run(
        ["git", "check-attr", "filter", "--", *default_suite_lfs()],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    untracked = [line for line in out.splitlines() if not line.endswith(": filter: lfs")]
    assert not untracked


def test_workflow_takes_its_lfs_list_from_the_corpus_registry():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "python -m tests.corpus --default-lfs" in text
    assert not re.search(r"\.ifc\b", text)


def test_workflow_cancels_superseded_runs_only_on_pull_requests():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "cancel-in-progress: ${{ github.event_name == 'pull_request' }}" in text


def test_workflow_fails_when_the_lfs_list_is_empty():
    # An empty include makes `git lfs pull` fetch the whole corpus.
    text = WORKFLOW.read_text(encoding="utf-8")
    assert 'list=$(python -m tests.corpus --default-lfs)\n          test -n "$list"' in text


def _readme_workflow_text() -> str:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```yaml\n(.*?)```", readme, re.DOTALL)
    assert len(blocks) == 1
    return blocks[0]


def _readme_workflow() -> dict:
    return yaml.safe_load(_readme_workflow_text())


def _load(path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _steps(workflow: dict) -> list[dict]:
    return [step for job in workflow["jobs"].values() for step in job["steps"]]


def _third_party_uses(workflow: dict) -> list[str]:
    return [step["uses"] for step in _steps(workflow) if "uses" in step and not step["uses"].startswith("./")]


def _github_if(expression: str, context: dict[str, str]) -> bool:
    """Evaluate a GitHub `if:` built from context names, string literals, ==, !=, && and ||."""
    python = re.sub(r"github(\.[\w-]+)+", lambda m: repr(context[m.group(0)]), expression)
    python = python.replace("&&", " and ").replace("||", " or ")
    assert re.fullmatch(r"(\s|'[^']*'|==|!=|and|or)+", python), expression
    return eval(python)


PR_COMMENT_WORKFLOWS = {"own": lambda: _load(PR_DIFF_WORKFLOW), "readme": _readme_workflow}
PINNED_WORKFLOW_TEXTS = {
    "own": lambda: PR_DIFF_WORKFLOW.read_text(encoding="utf-8"),
    "readme": _readme_workflow_text,
    "release": lambda: RELEASE_WORKFLOW.read_text(encoding="utf-8"),
    "tests": lambda: WORKFLOW.read_text(encoding="utf-8"),
    "nightly": lambda: NIGHTLY_WORKFLOW.read_text(encoding="utf-8"),
    "benchmark-dashboard": lambda: DASHBOARD_WORKFLOW.read_text(encoding="utf-8"),
}
PINNED_WORKFLOWS = {
    name: (lambda read=read: yaml.safe_load(read())) for name, read in PINNED_WORKFLOW_TEXTS.items()
}


@pytest.mark.parametrize("name", PINNED_WORKFLOWS)
def test_workflow_pins_every_third_party_action_to_a_commit_sha(name):
    uses = _third_party_uses(PINNED_WORKFLOWS[name]())
    assert uses
    for action in uses:
        assert re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", action), action


@pytest.mark.parametrize("name", PINNED_WORKFLOW_TEXTS)
def test_every_pinned_action_carries_its_version_comment(name):
    pinned = [
        line for line in PINNED_WORKFLOW_TEXTS[name]().splitlines()
        if "uses:" in line and re.search(r"@[0-9a-f]{40}\b", line)
    ]
    assert pinned
    for line in pinned:
        assert re.search(r"@[0-9a-f]{40} # v\d+(\.\d+)*$", line), line


@pytest.mark.parametrize("name", PR_COMMENT_WORKFLOWS)
def test_pr_comment_workflow_runs_python_isolated_from_the_checked_out_pull_request(name):
    commands = [
        shlex.split(line)
        for step in _steps(PR_COMMENT_WORKFLOWS[name]())
        for line in step.get("run", "").splitlines()
        if line.strip()
    ]
    python = [command for command in commands if command[0] == "python"]
    assert python
    for command in python:
        assert command[1] == "-I", command


def test_pr_diff_workflow_caches_bounds_and_cancels_per_pull_request():
    workflow = _load(PR_DIFF_WORKFLOW)
    assert workflow["concurrency"] == {
        "group": "athar-pr-diff-${{ github.event.pull_request.number }}",
        "cancel-in-progress": True,
    }
    (job,) = workflow["jobs"].values()
    assert isinstance(job["timeout-minutes"], int)
    by_action = {step["uses"].split("@")[0]: step.get("with", {}) for step in job["steps"] if "uses" in step}
    assert by_action["actions/checkout"]["lfs"] is True
    assert by_action["actions/setup-python"]["cache"] == "pip"
    assert by_action["Swatinem/rust-cache"]["workspaces"] == "athar/_native"


@pytest.mark.parametrize("name", PR_COMMENT_WORKFLOWS)
def test_pr_comment_posts_only_with_a_writable_token_and_summarizes_the_rest(name):
    steps = _steps(PR_COMMENT_WORKFLOWS[name]())
    bot_steps = [step for step in steps if shlex.split(step.get("run", ""))[:3] == ["athar", "git", "pr-comment"]]
    posting = [step for step in bot_steps if "--dry-run" not in shlex.split(step["run"])]
    summarizing = [step for step in bot_steps if "--dry-run" in shlex.split(step["run"])]
    assert len(posting) == 1 and len(summarizing) == 1
    assert posting[0]["env"] == {"GITHUB_TOKEN": "${{ secrets.GITHUB_TOKEN }}"}
    assert "env" not in summarizing[0]
    for head_repo, actor in itertools.product(("owner/repo", "fork/repo"), ("someone", "dependabot[bot]")):
        context = {
            "github.event.pull_request.head.repo.full_name": head_repo,
            "github.repository": "owner/repo",
            "github.actor": actor,
        }
        can_write = head_repo == "owner/repo" and actor != "dependabot[bot]"
        assert _github_if(posting[0]["if"], context) is can_write, (head_repo, actor)
        assert _github_if(summarizing[0]["if"], context) is not can_write, (head_repo, actor)
    assert shlex.split(summarizing[0]["run"])[-2:] == [">>", "$GITHUB_STEP_SUMMARY"]


def test_pr_comment_dry_run_needs_no_token_or_github_access(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "GITHUB_"))}
    env["GIT_CEILING_DIRECTORIES"] = str(tmp_path)
    env["ATHAR_CACHE_DIR"] = str(tmp_path / "cache")
    repo = tmp_path / "repo"

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=repo, env=env, capture_output=True, text=True, check=True
        ).stdout.strip()

    repo.mkdir()
    git("init", "-q")
    git("config", "user.email", "athar-test@example.invalid")
    git("config", "user.name", "Athar Test")
    model = repo / "model.ifc"
    model.write_text(TINY_IFC.read_text(encoding="utf-8"), encoding="utf-8")
    git("add", "model.ifc")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    model.write_text(
        model.read_text(encoding="utf-8").replace("2026-05-17T00:00:00", "2026-05-18T00:00:00"),
        encoding="utf-8",
    )
    git("commit", "-qam", "head")
    head = git("rev-parse", "HEAD")

    result = subprocess.run(
        [sys.executable, "-m", "athar", "git", "pr-comment", "--dry-run",
         "--base", base, "--head", head, "--repo", "example/repo", "--pr", "1"],
        cwd=repo, env=env, capture_output=True, text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "model.ifc" in result.stdout


_ANNOTATED_TAG = {"tags": {"0.1.1": {"type": "tag", "sha": "c" * 40}},
                  "tag_objects": {"c" * 40: {"type": "commit", "sha": "a" * 40}}}


@pytest.mark.parametrize("overrides,error", [
    ({}, None),
    (_ANNOTATED_TAG, None),
    ({"path": ".github/workflows/tests.yml"}, "not the Python release workflow"),
    ({"conclusion": "failure"}, "did not succeed"),
    ({"head_branch": None}, "release run has no ref name"),
    ({"head_branch": "release-branch"}, "is not a tag in this repository"),
    ({"tags": {"0.1.1": {"type": "commit", "sha": "b" * 40}}}, "does not point at the release run's commit"),
    ({**_ANNOTATED_TAG, "tag_objects": {"c" * 40: {"type": "commit", "sha": "b" * 40}}},
     "does not point at the release run's commit"),
    ({"wheel_version": "0.9.0"}, "does not equal tag"),
    ({"sdist_version": "0.9.0"}, "does not equal tag"),
    ({"package": "other"}, "does not equal tag"),
    ({"empty": True}, "must provide wheels and an sdist"),
])
def test_publish_validates_run_tag_and_artifact_metadata_before_upload(tmp_path, overrides, error):
    config = {
        "path": ".github/workflows/python-release.yml", "conclusion": "success",
        "head_branch": "0.1.1", "head_sha": "a" * 40,
        "tags": {"0.1.1": {"type": "commit", "sha": "a" * 40}}, "tag_objects": {},
        "wheel_version": "0.1.1", "sdist_version": "0.1.1", "package": "athar-ifc",
        **overrides,
    }
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text(f"#!{sys.executable}\n" + '''import io
import json
import os
import sys
import tarfile
import zipfile
from pathlib import Path
c = json.loads(os.environ["RELEASE_TEST_CONFIG"])
a = sys.argv[1:]
repo = "repos/{owner}/{repo}/"
if a[0] == "api" and a[1] == repo + "actions/runs/123":
    print(json.dumps({k: c[k] for k in ("path", "conclusion", "head_branch", "head_sha")}))
elif a[0] == "api" and a[1].startswith(repo + "git/ref/tags/"):
    target = c["tags"].get(a[1].removeprefix(repo + "git/ref/tags/"))
    if target is None:
        sys.exit("gh: Not Found (HTTP 404)")
    print(json.dumps({"object": target}))
elif a[0] == "api" and a[1].startswith(repo + "git/tags/"):
    print(json.dumps({"object": c["tag_objects"][a[1].removeprefix(repo + "git/tags/")]}))
elif a[:2] == ["run", "download"] and "--pattern" in a:
    root = Path(a[a.index("--dir") + 1])
    root.mkdir(parents=True, exist_ok=True)
    if not c.get("empty"):
        out = root / "dist-test"
        out.mkdir()
        with zipfile.ZipFile(out / "athar_ifc-0.1.1-py3-none-any.whl", "w") as z:
            z.writestr("athar_ifc-0.1.1.dist-info/METADATA", f'Name: {c["package"]}\\nVersion: {c["wheel_version"]}\\n')
        content = f'Name: {c["package"]}\\nVersion: {c["sdist_version"]}\\n'.encode()
        with tarfile.open(out / "athar_ifc-0.1.1.tar.gz", "w:gz") as t:
            info = tarfile.TarInfo("athar_ifc-0.1.1/PKG-INFO")
            info.size = len(content)
            t.addfile(info, io.BytesIO(content))
else:
    sys.exit(f"unexpected gh command: {a}")
''', encoding="utf-8")
    gh.chmod(0o755)
    uvx = bin_dir / "uvx"
    uvx.write_text(f"#!{sys.executable}\n" + '''import os
import sys
from pathlib import Path
with Path(os.environ["RELEASE_TEST_LOG"]).open("a") as f:
    f.write(" ".join(sys.argv[1:3]) + "\\n")
''', encoding="utf-8")
    uvx.chmod(0o755)
    log = tmp_path / "upload.log"
    result = subprocess.run(
        ["make", "-f", str(REPO_ROOT / "Makefile"), "publish", "RUN=123", f"PYTHON={sys.executable}"],
        cwd=tmp_path, capture_output=True, text=True,
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
             "RELEASE_TEST_CONFIG": json.dumps(config), "RELEASE_TEST_LOG": str(log)},
    )
    if error:
        assert result.returncode != 0
        assert error in result.stderr, result.stdout + result.stderr
        assert not log.exists()
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert log.read_text().splitlines() == ["twine check", "twine upload"]


def test_readme_workflow_for_other_repositories_installs_the_published_wheel():
    workflow = _readme_workflow()
    installs = [shlex.split(step["run"]) for step in _steps(workflow) if "pip" in shlex.split(step.get("run", ""))]
    assert installs == [["python", "-I", "-m", "pip", "install", "athar-ifc"]]
    assert set(_third_party_uses(workflow)) <= set(_third_party_uses(_load(PR_DIFF_WORKFLOW)))
