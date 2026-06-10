"""CLI entrypoints for Athar's Git integration."""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from athar.engine import diff_bundles

from .cache import load_or_build_bundle
from .render import DEFAULT_MAX_ITEMS, render_text_report

ATTRIBUTES_LINE = "*.ifc diff=athar -merge"
DIFF_COMMAND = "athar git diff --external"


@dataclass(frozen=True)
class DiffInputs:
    old_path: str | None = None
    new_path: str | None = None
    old_blob: str | None = None
    new_blob: str | None = None
    skipped_message: str | None = None


def build_git_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="athar git")
    subparsers = parser.add_subparsers(dest="git_command", required=True)
    _add_git_subcommands(subparsers)
    return parser


def add_git_parser(subparsers: argparse._SubParsersAction) -> None:
    git_parser = subparsers.add_parser("git", help="Git integration helpers")
    git_sub = git_parser.add_subparsers(dest="git_command", required=True)
    _add_git_subcommands(git_sub)


def _add_git_subcommands(git_sub: argparse._SubParsersAction) -> None:

    diff_parser = git_sub.add_parser("diff", help="Render a semantic IFC diff for Git")
    diff_parser.add_argument("args", nargs="*", help="OLD NEW, or Git external diff arguments with --external")
    diff_parser.add_argument("--external", action="store_true", help="Parse Git diff driver argument shape")
    diff_parser.add_argument("--old-blob", help="Old git blob oid for cache lookup")
    diff_parser.add_argument("--new-blob", help="New git blob oid for cache lookup")
    diff_parser.add_argument("--cache-dir", help="Override persistent signature cache directory")
    diff_parser.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS, help="Maximum changed entities to print")
    diff_parser.set_defaults(func=_cmd_diff)

    install_parser = git_sub.add_parser("install", help="Configure this repository to use Athar for .ifc diffs")
    scope = install_parser.add_mutually_exclusive_group()
    scope.add_argument("--local", dest="global_config", action="store_false", help="Write repo-local Git config (default)")
    scope.add_argument("--global", dest="global_config", action="store_true", help="Write global Git config")
    install_parser.set_defaults(global_config=False)
    install_parser.add_argument(
        "--no-attributes",
        action="store_true",
        help="Do not append .gitattributes guidance in the current repository",
    )
    install_parser.set_defaults(func=_cmd_install)


def run_git_command(args: argparse.Namespace) -> int:
    return args.func(args)


def _cmd_diff(args: argparse.Namespace) -> int:
    try:
        inputs = _diff_inputs(args)
        if inputs.skipped_message:
            print(inputs.skipped_message)
            return 0
        old_path = inputs.old_path
        new_path = inputs.new_path
        if old_path is None or new_path is None:
            raise ValueError("missing old/new IFC paths")
        if _is_missing_path(old_path) or _is_missing_path(new_path):
            print("Athar IFC diff requires both old and new IFC files.", file=sys.stderr)
            print(f"File-level add/delete: {old_path} -> {new_path}")
            return 0
        old_result = load_or_build_bundle(old_path, key=inputs.old_blob, cache_dir=args.cache_dir)
        new_result = load_or_build_bundle(new_path, key=inputs.new_blob, cache_dir=args.cache_dir)
        report = diff_bundles(old_result.bundle, new_result.bundle)
        print(render_text_report(report, max_items=args.max_items), end="")
        return 0
    except Exception as exc:
        stream = sys.stdout if args.external else sys.stderr
        print(f"Error: {exc}", file=stream)
        return 0 if args.external else 1


def _diff_inputs(args: argparse.Namespace) -> DiffInputs:
    if args.external:
        if len(args.args) == 1:
            return DiffInputs(skipped_message=f"Unmerged IFC path; semantic diff skipped: {args.args[0]}")
        if len(args.args) not in (7, 9):
            raise ValueError(
                "git external diff mode expects 1, 7, or 9 args: "
                "path old-file old-hex old-mode new-file new-hex new-mode [old-path new-path]"
            )
        _, old_file, old_hex, _, new_file, new_hex, _ = args.args[:7]
        return DiffInputs(old_file, new_file, old_hex, new_hex)
    if len(args.args) != 2:
        raise ValueError("diff mode expects OLD NEW")
    return DiffInputs(args.args[0], args.args[1], args.old_blob, args.new_blob)


def _is_missing_path(path: str) -> bool:
    return path == "/dev/null" or not Path(path).exists()


def _cmd_install(args: argparse.Namespace) -> int:
    config_scope = "--global" if args.global_config else "--local"
    _git_config(config_scope, "diff.athar.command", DIFF_COMMAND)
    _git_config(config_scope, "diff.athar.binary", "true")

    wrote_attributes = False
    if not args.no_attributes and not args.global_config:
        wrote_attributes = _ensure_attributes_line(Path(".gitattributes"))

    print(f"Configured Git diff driver: {DIFF_COMMAND}")
    if args.global_config:
        print(f"Add this to each IFC repository's .gitattributes: {ATTRIBUTES_LINE}")
    elif wrote_attributes:
        print(f"Updated .gitattributes: {ATTRIBUTES_LINE}")
    else:
        print(f".gitattributes already contains: {ATTRIBUTES_LINE}")
    return 0


def _git_config(scope: str, key: str, value: str) -> None:
    subprocess.run(["git", "config", scope, key, value], check=True)


def _ensure_attributes_line(path: Path) -> bool:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = existing.splitlines()
    if ATTRIBUTES_LINE in lines:
        return False
    prefix = existing
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    path.write_text(prefix + ATTRIBUTES_LINE + "\n", encoding="utf-8")
    return True
