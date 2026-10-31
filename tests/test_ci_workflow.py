"""Contracts of the default-suite test workflow (.github/workflows/tests.yml)."""

from __future__ import annotations

import re
import subprocess

from tests.corpus import CORPUS, LARGE, REPO_ROOT, SMALL, VIEWER_NEW_IFC, VIEWER_OLD_IFC, default_suite_lfs

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "tests.yml"


def _rel(path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def test_default_suite_lfs_covers_small_repo_tier_and_viewer_fixtures():
    listed = set(default_suite_lfs())
    small_repo = {_rel(f.path) for f in CORPUS.values() if f.tier == SMALL and f.required}
    assert small_repo <= listed
    assert {_rel(VIEWER_OLD_IFC), _rel(VIEWER_NEW_IFC)} <= listed
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
