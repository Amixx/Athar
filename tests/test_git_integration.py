from __future__ import annotations

import os
import pickle
import shlex
import shutil
import stat
import subprocess
import sys
import sysconfig
from importlib import metadata
from pathlib import Path

import pytest

import athar_git.cache as cache_mod
import athar_git.cli as cli_mod
import athar_git.pr_bot as pr_bot
from athar.bottom.constants import CANON_VERSION
from athar.bottom.types import ParseDiagnostics, SignatureBundle
from athar_git.render import render_markdown_report, render_text_report

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
                "data_delta": [
                    {
                        "path": "IfcPropertySingleValue[FireRating].NominalValue",
                        "old": "IfcLabel: 90",
                        "new": "IfcLabel: 60",
                    }
                ],
            }
        ],
        "added": [{"class": "IfcDoor", "step_id": 20, "guid": None, "name": "D-1"}],
        "deleted": [{"class": "IfcWindow", "step_id": 30, "guid": "gone"}],
    }

    rendered = render_text_report(report)

    assert "Summary: +1 -1 ~1 =5 (old 7, new 7)" in rendered
    assert "Modified by class: IfcWall 1" in rendered
    assert '~ IfcWall "W-101" #11 [data, placement, placement delta 5.000 mm]' in rendered
    assert "data: IfcPropertySingleValue[FireRating].NominalValue: IfcLabel: 90 -> IfcLabel: 60" in rendered
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


def test_render_markdown_report_wraps_changed_entities():
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"added": 1, "deleted": 0, "modified": 0, "unchanged": 2, "old_signatures": 2, "new_signatures": 3},
        "added": [{"class": "IfcWall", "step_id": 1, "guid": None}],
        "deleted": [],
        "modified": [],
    }

    rendered = render_markdown_report(report)

    assert "| Added | Deleted | Modified | Unchanged | Old signatures | New signatures |" in rendered
    assert "| 1 | 0 | 0 | 2 | 2 | 3 |" in rendered
    assert "<details>" in rendered
    assert "+ IfcWall #1" in rendered


def test_pr_bot_discovers_modified_renamed_added_and_deleted_ifc(monkeypatch):
    payload = "\0".join(
        [
            "M",
            "a.ifc",
            "R100",
            "old.ifc",
            "new.ifc",
            "A",
            "added.IFC",
            "D",
            "gone.ifc",
            "M",
            "notes.txt",
            "",
        ]
    ).encode("utf-8")
    monkeypatch.setattr(pr_bot, "_git", lambda args: payload)

    changes = pr_bot.discover_ifc_changes("base", "head")

    assert changes == [
        pr_bot.IfcChange(status="M", old_path="a.ifc", new_path="a.ifc"),
        pr_bot.IfcChange(status="R100", old_path="old.ifc", new_path="new.ifc"),
        pr_bot.IfcChange(status="A", old_path=None, new_path="added.IFC"),
        pr_bot.IfcChange(status="D", old_path="gone.ifc", new_path=None),
    ]


def test_pr_bot_render_comment_uses_semantic_report_and_file_level_notes(monkeypatch, tmp_path):
    report = {
        "schemas": {"old": "IFC4", "new": "IFC4"},
        "stats": {"added": 0, "deleted": 0, "modified": 0, "unchanged": 1, "old_signatures": 1, "new_signatures": 1},
        "added": [],
        "deleted": [],
        "modified": [],
    }
    seen = []
    monkeypatch.setattr(
        pr_bot,
        "_diff_git_paths",
        lambda base, head, *, old_path, new_path, cache_dir: seen.append((base, head, old_path, new_path, cache_dir))
        or report,
    )
    policy = tmp_path / "policy.json"
    policy.write_text('{"ok": false, "violations": [{}, {}]}', encoding="utf-8")

    body = pr_bot.render_pr_comment(
        [
            pr_bot.IfcChange(status="M", old_path="model.ifc", new_path="model.ifc"),
            pr_bot.IfcChange(status="A", old_path=None, new_path="added.ifc"),
        ],
        base="a" * 40,
        head="b" * 40,
        cache_dir="/tmp/cache",
        max_files=10,
        max_items=5,
        policy_result_path=str(policy),
    )

    assert pr_bot.COMMENT_MARKER in body
    assert "Policy gate: `fail` (2 violation(s))" in body
    assert "### `model.ifc`" in body
    assert "No semantic entity changes." in body
    assert "File added (`A`)." in body
    assert seen == [("a" * 40, "b" * 40, "model.ifc", "model.ifc", "/tmp/cache")]




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


def test_cache_entries_are_keyed_to_the_installed_distribution_version(tmp_path):
    cache_dir = tmp_path / "cache"
    first = cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir)
    with first.path.open("rb") as handle:
        payload = pickle.load(handle)
    assert payload["producer_version"] == metadata.version("athar-ifc")
    assert cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir).hit is True

    payload["producer_version"] = metadata.version("athar-ifc") + ".other"
    with first.path.open("wb") as handle:
        pickle.dump(payload, handle)
    assert cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir).hit is False


def test_cache_directories_are_created_private(tmp_path):
    cache_dir = tmp_path / "fresh" / "cache"
    result = cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir)
    assert stat.S_IMODE(cache_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(result.path.parent.stat().st_mode) == 0o700


class _RunsOnLoad:
    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def __reduce__(self):
        return (os.mkdir, (str(self.marker),))


@pytest.mark.parametrize("loose", ["root", "canon"])
def test_cache_refuses_a_directory_others_can_write_before_unpickling(tmp_path, loose):
    cache_dir = tmp_path / "cache"
    entry = cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir).path
    marker = tmp_path / "unpickled"
    entry.write_bytes(pickle.dumps(_RunsOnLoad(marker)))
    (cache_dir if loose == "root" else entry.parent).chmod(0o777)

    with pytest.raises(PermissionError, match="writable by group or others"):
        cache_mod.load_or_build_bundle(TINY_IFC, cache_dir=cache_dir)
    assert not marker.exists()


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


def _git_env(tmp_path: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_CEILING_DIRECTORIES"] = str(tmp_path)
    env["GIT_CONFIG_GLOBAL"] = str(tmp_path / "global.gitconfig")
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    return env


def _athar_install(
    cwd: Path, env: dict[str, str], *args: str, python: str = sys.executable
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [python, "-m", "athar", "git", "install", *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


def _python_without_athar_script(tmp_path: Path, *, foreign_script: bool = False) -> str:
    venv = tmp_path / "bare-venv"
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(venv)], check=True)
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    purelib, scripts = (
        Path(subprocess.run(
            [str(python), "-c", f"import sysconfig; print(sysconfig.get_path({name!r}))"],
            capture_output=True, text=True, check=True,
        ).stdout.strip())
        for name in ("purelib", "scripts")
    )
    host = sysconfig.get_path("purelib")
    (purelib / "host.pth").write_text(f"import site; site.addsitedir({host!r})\n", encoding="utf-8")
    version = metadata.version("athar-ifc")
    record = purelib / f"athar_ifc-{version}.dist-info"
    record.mkdir()
    (record / "METADATA").write_text(f"Metadata-Version: 2.1\nName: athar-ifc\nVersion: {version}\n", encoding="utf-8")
    (record / "RECORD").write_text(f"{record.name}/METADATA,,\n{record.name}/RECORD,,\n", encoding="utf-8")
    if foreign_script:
        (scripts / ("athar.exe" if os.name == "nt" else "athar")).write_text("foreign", encoding="utf-8")
    return str(python)


def test_a_console_script_the_distribution_does_not_record_is_not_used(tmp_path):
    env = _git_env(tmp_path)
    python = _python_without_athar_script(tmp_path, foreign_script=True)
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True, env=env)

    installed = _athar_install(repo, env, python=python)
    assert installed.returncode == 0, installed.stderr
    command = shlex.split(subprocess.run(
        ["git", "config", "--get", "diff.athar.command"],
        cwd=repo, env=env, capture_output=True, text=True, check=True,
    ).stdout)
    assert command[:2] == [python, "-c"]


@pytest.mark.parametrize("interpreter", ["console-script", "no-console-script"])
def test_installed_git_commands_never_import_from_the_diffed_repository(tmp_path, interpreter):
    env = _git_env(tmp_path)
    python = sys.executable if interpreter == "console-script" else _python_without_athar_script(tmp_path)
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True, env=env)
    for key, value in (("user.email", "athar-test@example.invalid"), ("user.name", "Athar Test")):
        subprocess.run(["git", "config", key, value], cwd=repo, check=True, env=env)

    installed = _athar_install(repo, env, "--difftool", python=python)
    assert installed.returncode == 0, installed.stderr
    command = shlex.split(subprocess.run(
        ["git", "config", "--get", "diff.athar.command"],
        cwd=repo, env=env, capture_output=True, text=True, check=True,
    ).stdout)
    assert Path(command[0]).is_absolute()
    if interpreter == "console-script":
        assert command[0] == str(Path(sysconfig.get_path("scripts")) / "athar")
    else:
        assert command[:2] == [python, "-c"]

    model = repo / "model.ifc"
    model.write_text(TINY_IFC.read_text(encoding="utf-8"), encoding="utf-8")
    subprocess.run(["git", "add", ".gitattributes", "model.ifc"], cwd=repo, check=True, env=env)
    subprocess.run(["git", "commit", "-qm", "baseline"], cwd=repo, check=True, env=env)
    markers = tmp_path / "markers"
    markers.mkdir()
    for module in ("athar", "json", "ifcopenshell"):
        marker = markers / module
        (repo / f"{module}.py").write_text(
            f"from pathlib import Path\nPath({str(marker)!r}).write_text('imported')\n", encoding="utf-8"
        )

    model.write_text(
        model.read_text(encoding="utf-8").replace("2026-05-17T00:00:00", "2026-05-18T00:00:00"),
        encoding="utf-8",
    )
    bare_path = os.pathsep.join([str(Path(shutil.which("git")).parent), "/usr/bin", "/bin"])
    assert shutil.which("athar", path=bare_path) is None
    run_env = {**env, "PATH": bare_path, "ATHAR_CACHE_DIR": str(tmp_path / "cache")}
    diffed = subprocess.run(
        ["git", "diff", "--", "model.ifc"], cwd=repo, env=run_env, capture_output=True, text=True,
    )
    assert diffed.returncode == 0, diffed.stderr
    assert "Athar IFC diff" in diffed.stdout
    assert list(markers.iterdir()) == []

    model.write_text("not an IFC file\n", encoding="utf-8")
    viewed = subprocess.run(
        ["git", "difftool", "-y", "-t", "athar", "--", "model.ifc"],
        cwd=repo, env=run_env, capture_output=True, text=True, timeout=120,
    )
    assert "Error: Cannot parse IFC file" in viewed.stdout + viewed.stderr
    assert list(markers.iterdir()) == []


def test_git_install_writes_attributes_at_repository_top_level(tmp_path):
    env = _git_env(tmp_path)
    repo = tmp_path / "repo"
    subdir = repo / "models" / "arch"
    subdir.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True, env=env)

    first = _athar_install(subdir, env)
    assert first.returncode == 0, first.stderr
    assert (repo / ".gitattributes").read_text(encoding="utf-8") == "*.ifc diff=athar -merge\n"
    assert not (subdir / ".gitattributes").exists()

    second = _athar_install(subdir, env)
    assert second.returncode == 0
    assert "already contains" in second.stdout
    assert (repo / ".gitattributes").read_text(encoding="utf-8") == "*.ifc diff=athar -merge\n"


def test_git_install_outside_a_repository_fails_without_writing(tmp_path):
    env = _git_env(tmp_path)
    outside = tmp_path / "plain"
    outside.mkdir()

    result = _athar_install(outside, env)

    assert result.returncode == 1
    assert "must run inside a Git repository" in result.stderr
    assert not (outside / ".gitattributes").exists()
    assert not (tmp_path / "global.gitconfig").exists()


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


def test_main_dispatches_git_subcommand():
    result = subprocess.run(
        [sys.executable, "-m", "athar", "git", "diff", str(TINY_IFC), str(TINY_IFC)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Athar IFC diff" in result.stdout


_LFS_POINTER_TEXT = (
    "version https://git-lfs.github.com/spec/v1\n"
    "oid sha256:{oid}\n"
    "size 12345\n"
)


def test_lfs_pointer_oid_parses_pointer_and_rejects_other_files(tmp_path):
    oid = "ab" * 32
    pointer = tmp_path / "pointer.ifc"
    pointer.write_text(_LFS_POINTER_TEXT.format(oid=oid), encoding="utf-8")
    assert cache_mod._lfs_pointer_oid(pointer) == oid

    ifc = tmp_path / "real.ifc"
    ifc.write_text("ISO-10303-21;\nHEADER;\n", encoding="utf-8")
    assert cache_mod._lfs_pointer_oid(ifc) is None

    big = tmp_path / "big.ifc"
    big.write_bytes(b"version https://git-lfs" + b"x" * 2000)
    assert cache_mod._lfs_pointer_oid(big) is None

    assert cache_mod._lfs_pointer_oid(tmp_path / "missing.ifc") is None


def test_cached_lfs_pointer_diffs_without_smudging(tmp_path, monkeypatch):
    oid = "cd" * 32
    pointer = tmp_path / "pointer.ifc"
    pointer.write_text(_LFS_POINTER_TEXT.format(oid=oid), encoding="utf-8")
    cache_dir = tmp_path / "cache"
    entry = cache_mod._entry_path(cache_dir, f"sha256-{oid}")
    entry.parent.mkdir(mode=0o700, parents=True)
    cache_mod._write_entry(entry, _bundle(), f"sha256-{oid}")

    def refuse_smudge(*args, **kwargs):
        raise AssertionError("smudge must not run on a cache hit")

    monkeypatch.setattr(cache_mod, "_smudge_lfs_pointer", refuse_smudge)
    result = cache_mod.load_or_build_bundle(pointer, key="e" * 40, cache_dir=cache_dir)
    assert result.hit is True
    assert result.key == f"sha256-{oid}"


def test_lfs_pointer_miss_smudges_and_verifies(tmp_path, monkeypatch):
    content = b"ISO-10303-21;\nENDSEC;\n"
    oid = __import__("hashlib").sha256(content).hexdigest()
    pointer = tmp_path / "pointer.ifc"
    pointer.write_text(_LFS_POINTER_TEXT.format(oid=oid), encoding="utf-8")

    def fake_smudge(src, dest, *, expected_sha256):
        assert expected_sha256 == oid
        Path(dest).write_bytes(content)

    built = []

    def fake_build(path):
        built.append(Path(path).read_bytes())
        return _bundle()

    monkeypatch.setattr(cache_mod, "_smudge_lfs_pointer", fake_smudge)
    monkeypatch.setattr(cache_mod, "build_signature_bundle", fake_build)
    result = cache_mod.load_or_build_bundle(pointer, cache_dir=tmp_path / "cache")
    assert result.hit is False
    assert result.key == f"sha256-{oid}"
    assert built == [content]
