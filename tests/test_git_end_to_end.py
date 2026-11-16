"""End-to-end Git workflow over real revision files.

Builds a throwaway Git repository in tmp_path, commits real-world
Building-Landscaping revisions as successive versions of one ``model.ifc``,
installs the Athar diff driver via ``athar git install``, and asserts that
plain Git commands (``git diff``, ``git show --ext-diff``) surface semantic
entity-level output instead of binary noise, with signature bundles cached by
Git blob oid.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.corpus import REPO_ROOT, corpus_path

DRIVER_COMMAND = f'"{sys.executable}" -m athar git diff --external'
REVIT_EXPORTS = REPO_ROOT / "corpus" / "roundtrip-revit-2026-07-06" / "exports"


def _revit_export(name: str) -> Path:
    path = REVIT_EXPORTS / f"{name}.ifc"
    if not path.is_file() or path.stat().st_size < 10_000:
        pytest.skip(f"revit corpus export not available: {path} (decompress exports/*.zst)")
    return path


@pytest.fixture
def git_env(tmp_path):
    env = dict(os.environ)
    env.update(
        {
            "HOME": str(tmp_path),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "tester",
            "GIT_AUTHOR_EMAIL": "tester@example.com",
            "GIT_COMMITTER_NAME": "tester",
            "GIT_COMMITTER_EMAIL": "tester@example.com",
            "ATHAR_CACHE_DIR": str(tmp_path / "athar-cache"),
            "PYTHONPATH": str(REPO_ROOT),
        }
    )
    return env


def _run(args, *, cwd, env, check=True):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise AssertionError(f"{args} failed ({result.returncode}):\n{result.stdout}\n{result.stderr}")
    return result


def _commit_version(repo, env, source_path, message):
    shutil.copyfile(source_path, repo / "model.ifc")
    _run(["git", "add", "-A"], cwd=repo, env=env)
    _run(["git", "commit", "-q", "-m", message], cwd=repo, env=env)


def test_real_ifc_history_diffs_semantically_through_git(tmp_path, git_env):
    v0 = corpus_path("bl_v0")
    v1 = corpus_path("bl_v1")
    v2 = corpus_path("bl_v2")

    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "init", "-q"], cwd=repo, env=git_env)

    install = _run(
        [sys.executable, "-m", "athar", "git", "install", "--local"], cwd=repo, env=git_env
    )
    assert "Configured Git diff driver" in install.stdout
    assert "*.ifc diff=athar -merge" in (repo / ".gitattributes").read_text()
    _run(["git", "config", "diff.athar.command", DRIVER_COMMAND], cwd=repo, env=git_env)

    _commit_version(repo, git_env, v0, "v0: original landscaping model")
    _commit_version(repo, git_env, v1, "v1: first revision")

    committed = _run(["git", "diff", "HEAD~1", "HEAD"], cwd=repo, env=git_env)
    assert "Athar IFC diff" in committed.stdout
    assert "Summary: +" in committed.stdout
    assert "~ Ifc" in committed.stdout
    assert "Binary files" not in committed.stdout
    assert "Error:" not in committed.stdout

    cache_entries = list((tmp_path / "athar-cache").rglob("git-*.pickle"))
    assert len(cache_entries) == 2

    shutil.copyfile(v2, repo / "model.ifc")
    worktree = _run(["git", "diff"], cwd=repo, env=git_env)
    assert "Athar IFC diff" in worktree.stdout
    assert "Summary: +" in worktree.stdout
    _run(["git", "checkout", "-q", "--", "model.ifc"], cwd=repo, env=git_env)

    root_commit = _run(
        ["git", "rev-list", "--max-parents=0", "HEAD"], cwd=repo, env=git_env
    ).stdout.strip()
    shown = _run(["git", "show", "--ext-diff", root_commit], cwd=repo, env=git_env)
    assert "File-level add/delete" in shown.stdout

    log = _run(["git", "log", "-p", "--ext-diff"], cwd=repo, env=git_env)
    assert "v1: first revision" in log.stdout
    assert "Athar IFC diff" in log.stdout

    again = _run(["git", "diff", "HEAD~1", "HEAD"], cwd=repo, env=git_env)
    assert again.stdout == committed.stdout


@pytest.mark.skipif(
    os.environ.get("ATHAR_RUN_LARGE_ACCEPTANCE") != "1",
    reason="large acceptance tier is opt-in via ATHAR_RUN_LARGE_ACCEPTANCE=1",
)
def test_revit_roundtrip_chain_diffs_semantically_through_git(tmp_path, git_env):
    """Real Revit exports through real Git: an atomic edit surfaces as its
    entities, and a same-settings re-export (bytes differ only in the STEP
    header) is semantically empty."""
    r7 = _revit_export("r7")
    r8 = _revit_export("r8")
    r9 = _revit_export("r9")

    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "init", "-q"], cwd=repo, env=git_env)
    _run([sys.executable, "-m", "athar", "git", "install", "--local"], cwd=repo, env=git_env)
    _run(["git", "config", "diff.athar.command", DRIVER_COMMAND], cwd=repo, env=git_env)

    _commit_version(repo, git_env, r7, "r7: door frame-type property edit")
    _commit_version(repo, git_env, r8, "r8: duplicated that door twice")
    _commit_version(repo, git_env, r9, "r9: re-export, no changes")

    doors_added = _run(["git", "diff", "HEAD~2", "HEAD~1"], cwd=repo, env=git_env)
    assert "Athar IFC diff" in doors_added.stdout
    assert "IfcDoor" in doors_added.stdout
    assert "Error:" not in doors_added.stdout

    reexport = _run(["git", "diff", "HEAD~1", "HEAD"], cwd=repo, env=git_env)
    assert "Summary: +0 -0 ~0" in reexport.stdout


@pytest.mark.skipif(shutil.which("git-lfs") is None, reason="git-lfs not installed")
def test_lfs_tracked_ifc_diffs_semantically_through_git(tmp_path, git_env):
    """LFS-tracked IFC stays semantically diffable. Git may smudge blobs
    before handing temp files to the external driver, but skip-smudge and
    partial-fetch setups hand it raw pointer stubs — the driver must resolve
    those itself via `git lfs smudge`."""
    v0 = corpus_path("bl_v0")
    v1 = corpus_path("bl_v1")

    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "init", "-q"], cwd=repo, env=git_env)
    _run(["git", "lfs", "install", "--local"], cwd=repo, env=git_env)
    (repo / ".gitattributes").write_text("*.ifc filter=lfs diff=athar -merge\n", encoding="utf-8")
    _run(["git", "config", "diff.athar.command", DRIVER_COMMAND], cwd=repo, env=git_env)
    _run(["git", "config", "diff.athar.binary", "true"], cwd=repo, env=git_env)

    _commit_version(repo, git_env, v0, "v0: original landscaping model")
    _commit_version(repo, git_env, v1, "v1: first revision")

    blob = _run(["git", "cat-file", "-p", "HEAD:model.ifc"], cwd=repo, env=git_env)
    assert blob.stdout.startswith("version https://git-lfs")

    diffed = _run(["git", "diff", "HEAD~1", "HEAD"], cwd=repo, env=git_env)
    assert "Athar IFC diff" in diffed.stdout
    assert "Summary: +" in diffed.stdout
    assert "Error:" not in diffed.stdout
    assert len(list((tmp_path / "athar-cache").rglob("*.pickle"))) == 2

    old_pointer = repo / "old-pointer.ifc"
    new_pointer = repo / "new-pointer.ifc"
    old_pointer.write_text(
        _run(["git", "cat-file", "-p", "HEAD~1:model.ifc"], cwd=repo, env=git_env).stdout,
        encoding="utf-8",
    )
    new_pointer.write_text(blob.stdout, encoding="utf-8")

    direct = _run(
        [sys.executable, "-m", "athar", "git", "diff", str(old_pointer), str(new_pointer)],
        cwd=repo,
        env=git_env,
    )
    assert "Athar IFC diff" in direct.stdout
    assert "Summary: +" in direct.stdout
    assert len(list((tmp_path / "athar-cache").rglob("sha256-*.pickle"))) == 2
