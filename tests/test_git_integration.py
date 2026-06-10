from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import athar.__main__ as main_mod
import athar_git.cache as cache_mod
import athar_git.cli as cli_mod
from athar.bottom.constants import CANON_VERSION
from athar.bottom.types import ParseDiagnostics, SignatureBundle
from athar_git.render import render_text_report

REPO_ROOT = Path(__file__).resolve().parents[1]
TINY_IFC = REPO_ROOT / "tests" / "fixtures" / "tiny_no_products.ifc"


def _bundle(schema: str = "IFC4", count: int = 1) -> SignatureBundle:
    return SignatureBundle(
        filepath="model.ifc",
        schema=schema,
        canon_version=CANON_VERSION,
        signatures={},
        diagnostics=ParseDiagnostics(),
        edge_stats={},
    )


def test_render_text_report_summarizes_counts_classes_and_modified_details():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {
            "added": 1,
            "deleted": 1,
            "modified": 1,
            "unchanged": 5,
            "old_signatures": 7,
            "new_signatures": 7,
            "modified_change_scope": {"intrinsic": 1, "mixed": 0, "transitive": 0},
            "matcher_diagnostics": {"tiers": {"guid": 1}},
        },
        "modified": [
            {
                "old": {"class": "IfcWall", "step_id": 10, "guid": "old-guid"},
                "new": {"class": "IfcWall", "step_id": 11, "guid": "old-guid", "name": "W-101"},
                "match": {"reason": "guid", "score": 0.9},
                "aspects": {
                    "geometry": "unchanged",
                    "data": "changed",
                    "topology": "unchanged",
                    "placement": "changed",
                    "placement_delta_mm": (3.0, 4.0, 0.0),
                },
            }
        ],
        "added": [{"class": "IfcDoor", "step_id": 20, "guid": None, "name": "D-1"}],
        "deleted": [{"class": "IfcWindow", "step_id": 30, "guid": "gone"}],
    }

    rendered = render_text_report(report)

    assert "Summary: +1 -1 ~1 =5 (old 7, new 7)" in rendered
    assert "Modified by class: IfcWall 1" in rendered
    assert '~ IfcWall "W-101" #11 [data, placement, placement delta 5.000 mm]' in rendered
    assert '+ IfcDoor "D-1" #20' in rendered
    assert "- IfcWindow #30" in rendered


def test_render_text_report_caps_changed_items():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"added": 2, "deleted": 0, "modified": 0, "unchanged": 0, "old_signatures": 0, "new_signatures": 2},
        "added": [
            {"class": "IfcWall", "step_id": 1, "guid": None},
            {"class": "IfcWall", "step_id": 2, "guid": None},
        ],
        "deleted": [],
        "modified": [],
    }

    rendered = render_text_report(report, max_items=1)

    assert "+ IfcWall #1" in rendered
    assert "+ IfcWall #2" not in rendered
    assert "... and 1 more added" in rendered


def test_persistent_cache_reuses_blob_key(tmp_path, monkeypatch):
    source = tmp_path / "model.ifc"
    source.write_text("IFC", encoding="utf-8")
    calls = []

    def fake_build(path):
        calls.append(path)
        return _bundle()

    monkeypatch.setattr(cache_mod, "build_signature_bundle", fake_build)
    key = "a" * 40

    first = cache_mod.load_or_build_bundle(source, key=key, cache_dir=tmp_path / "cache")
    second = cache_mod.load_or_build_bundle(source, key=key, cache_dir=tmp_path / "cache")

    assert first.hit is False
    assert second.hit is True
    assert calls == [str(source)]
    assert first.path == second.path


def test_git_diff_external_mode_uses_blob_cache_keys(tmp_path, monkeypatch, capsys):
    seen = []
    old_path = tmp_path / "old.ifc"
    new_path = tmp_path / "new.ifc"
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")

    def fake_load(path, *, key, cache_dir):
        seen.append((path, key, cache_dir))
        return cache_mod.CacheResult(bundle=_bundle(), key=key, hit=False, path=None)

    monkeypatch.setattr(cli_mod, "load_or_build_bundle", fake_load)
    monkeypatch.setattr(
        cli_mod,
        "diff_bundles",
        lambda old, new: {
            "schemas": {"old": "IFC4", "new": "IFC4"},
            "stats": {"added": 0, "deleted": 0, "modified": 0, "unchanged": 1, "old_signatures": 1, "new_signatures": 1},
            "added": [],
            "deleted": [],
            "modified": [],
        },
    )
    parser = cli_mod.build_git_parser()
    args = parser.parse_args(
        [
            "diff",
            "--external",
            "--cache-dir",
            "/tmp/cache",
            "model.ifc",
            str(old_path),
            "a" * 40,
            "100644",
            str(new_path),
            "b" * 40,
            "100644",
        ]
    )

    assert cli_mod.run_git_command(args) == 0
    out = capsys.readouterr().out
    assert "No semantic entity changes." in out
    assert seen == [(str(old_path), "a" * 40, "/tmp/cache"), (str(new_path), "b" * 40, "/tmp/cache")]


def test_git_diff_external_mode_accepts_rename_arguments(tmp_path, monkeypatch):
    old_path = tmp_path / "old.ifc"
    new_path = tmp_path / "new.ifc"
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")
    seen = []

    monkeypatch.setattr(
        cli_mod,
        "load_or_build_bundle",
        lambda path, *, key, cache_dir: seen.append((path, key)) or cache_mod.CacheResult(
            bundle=_bundle(), key=key, hit=False, path=None
        ),
    )
    monkeypatch.setattr(
        cli_mod,
        "diff_bundles",
        lambda old, new: {
            "schemas": {"old": "IFC4", "new": "IFC4"},
            "stats": {"added": 0, "deleted": 0, "modified": 0, "unchanged": 0, "old_signatures": 0, "new_signatures": 0},
            "added": [],
            "deleted": [],
            "modified": [],
        },
    )
    args = cli_mod.build_git_parser().parse_args(
        [
            "diff",
            "--external",
            "new-name.ifc",
            str(old_path),
            "a" * 40,
            "100644",
            str(new_path),
            "b" * 40,
            "100644",
            "old-name.ifc",
            "new-name.ifc",
        ]
    )

    assert cli_mod.run_git_command(args) == 0
    assert seen == [(str(old_path), "a" * 40), (str(new_path), "b" * 40)]


def test_git_diff_external_mode_skips_unmerged_single_arg(capsys):
    args = cli_mod.build_git_parser().parse_args(["diff", "--external", "model.ifc"])

    assert cli_mod.run_git_command(args) == 0
    assert "Unmerged IFC path; semantic diff skipped: model.ifc" in capsys.readouterr().out


def test_git_diff_external_mode_prints_engine_errors_as_diff_body(monkeypatch, tmp_path, capsys):
    old_path = tmp_path / "old.ifc"
    new_path = tmp_path / "new.ifc"
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod,
        "load_or_build_bundle",
        lambda path, *, key, cache_dir: cache_mod.CacheResult(bundle=_bundle(), key=key, hit=False, path=None),
    )
    monkeypatch.setattr(cli_mod, "diff_bundles", lambda old, new: (_ for _ in ()).throw(ValueError("Schema mismatch")))
    args = cli_mod.build_git_parser().parse_args(
        [
            "diff",
            "--external",
            "model.ifc",
            str(old_path),
            "a" * 40,
            "100644",
            str(new_path),
            "b" * 40,
            "100644",
        ]
    )

    assert cli_mod.run_git_command(args) == 0
    captured = capsys.readouterr()
    assert "Error: Schema mismatch" in captured.out
    assert captured.err == ""


def test_git_diff_manual_mode_returns_nonzero_on_engine_errors(monkeypatch, tmp_path, capsys):
    old_path = tmp_path / "old.ifc"
    new_path = tmp_path / "new.ifc"
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod,
        "load_or_build_bundle",
        lambda path, *, key, cache_dir: cache_mod.CacheResult(bundle=_bundle(), key=key, hit=False, path=None),
    )
    monkeypatch.setattr(cli_mod, "diff_bundles", lambda old, new: (_ for _ in ()).throw(ValueError("Schema mismatch")))
    args = cli_mod.build_git_parser().parse_args(["diff", str(old_path), str(new_path)])

    assert cli_mod.run_git_command(args) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Error: Schema mismatch" in captured.err


def test_git_install_sets_config_and_appends_attributes(tmp_path, monkeypatch, capsys):
    calls = []

    def fake_run(command, check):
        calls.append((command, check))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod.subprocess, "run", fake_run)
    parser = cli_mod.build_git_parser()
    args = parser.parse_args(["install"])

    assert cli_mod.run_git_command(args) == 0

    assert calls == [
        (["git", "config", "--local", "diff.athar.command", "athar git diff --external"], True),
        (["git", "config", "--local", "diff.athar.binary", "true"], True),
    ]
    assert (tmp_path / ".gitattributes").read_text(encoding="utf-8") == "*.ifc diff=athar -merge\n"
    assert "Configured Git diff driver" in capsys.readouterr().out


def test_git_diff_driver_e2e_invoked_by_real_git(tmp_path):
    repo = tmp_path / "repo"
    cache_dir = tmp_path / "cache"
    repo.mkdir()
    wrapper = tmp_path / "athar-git-diff"
    wrapper.write_text(
        "#!/bin/sh\n"
        f"PYTHONPATH={str(REPO_ROOT)!r} "
        f"ATHAR_CACHE_DIR={str(cache_dir)!r} "
        f"exec {sys.executable!r} -m athar git diff --external \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)

    def git(*args: str) -> subprocess.CompletedProcess:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )

    git("init")
    git("config", "user.email", "athar-test@example.invalid")
    git("config", "user.name", "Athar Test")
    git("config", "diff.athar.command", str(wrapper))
    git("config", "diff.athar.binary", "true")
    (repo / ".gitattributes").write_text("*.ifc diff=athar -merge\n", encoding="utf-8")
    model = repo / "model.ifc"
    model.write_text(TINY_IFC.read_text(encoding="utf-8"), encoding="utf-8")
    git("add", ".gitattributes", "model.ifc")
    git("commit", "-m", "baseline")

    model.write_text(
        model.read_text(encoding="utf-8").replace(
            "2026-05-17T00:00:00",
            "2026-05-18T00:00:00",
        ),
        encoding="utf-8",
    )

    result = git("diff", "--", "model.ifc")

    assert "Athar IFC diff" in result.stdout
    assert "No semantic entity changes." in result.stdout
    assert "ISO-10303-21" not in result.stdout


def test_main_dispatches_git_subcommand(monkeypatch):
    called = {}

    def fake_run(args):
        called["command"] = args.git_command
        return 0

    monkeypatch.setattr(cli_mod, "run_git_command", fake_run)
    monkeypatch.setattr(sys, "argv", ["athar", "git", "install", "--no-attributes"])

    with pytest.raises(SystemExit) as exc:
        main_mod.main()

    assert exc.value.code == 0
    assert called == {"command": "install"}
