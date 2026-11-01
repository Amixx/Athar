"""Contracts of the default-suite test workflow (.github/workflows/tests.yml)."""

from __future__ import annotations

import re
import subprocess

from tests.corpus import CORPUS, LARGE, REPO_ROOT, default_suite_lfs

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "tests.yml"


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
