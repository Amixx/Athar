"""Top-level `athar` command: dispatches to each subcommand's own parser."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from athar.engine import (
    SchemaMismatchError,
    diff_files,
    generated_at_now_utc,
    package_version,
    stream_diff_files,
)

_COMMAND_HELP = {
    "diff": "Semantic diff of two IFC files as JSON (the default: `athar OLD NEW`)",
    "check": "Evaluate a diff against a policy pack and exit non-zero on violations",
    "git": "Git diff driver, `git install`, and pull-request comments",
    "view": "Open a visual 3D diff of two or more IFC revisions in the browser",
    "store": "Persistent baselines, approvals, and review history",
    "rekey": "Restore GlobalIds of a re-exported revision from a base model",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="athar",
        description="Athar: semantic IFC diff at the entity and property level.",
        epilog="Run `athar COMMAND --help` for a command's options.",
    )
    parser.add_argument("--version", action="store_true", help="Print the installed athar-ifc version and exit")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND", title="commands")
    for name, summary in _COMMAND_HELP.items():
        commands.add_parser(name, help=summary, add_help=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and _starts_implicit_diff(argv[0]):
        argv.insert(0, "diff")
    parser = build_parser()
    args = parser.parse_args(argv[:1])
    if args.version:
        print(f"athar {package_version()}")
        return 0
    if args.command is None:
        parser.print_help(sys.stderr)
        return 2
    return _COMMANDS[args.command](argv[1:])


def _starts_implicit_diff(first: str) -> bool:
    if first in ("-h", "--help", "--version") or first in _COMMAND_HELP:
        return False
    if first.startswith("-"):
        return True
    return bool(first) and (first.lower().endswith(".ifc") or Path(first).exists())


def _run_diff(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="athar diff", description=_COMMAND_HELP["diff"])
    parser.add_argument("old", help="Path to old IFC file")
    parser.add_argument("new", help="Path to new IFC file")
    parser.add_argument(
        "--stream",
        choices=["none", "ndjson", "chunked_json"],
        default="none",
        help="Output streaming mode",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for --stream chunked_json",
    )
    parser.add_argument(
        "--generated-at",
        metavar="TIMESTAMP|now",
        help="Embed an audit timestamp; use 'now' for the current UTC time",
    )
    args = parser.parse_args(argv)

    try:
        generated_at = _generated_at_arg(args.generated_at)
        if args.stream != "none":
            for line in stream_diff_files(
                args.old,
                args.new,
                mode=args.stream,
                chunk_size=args.chunk_size,
                generated_at=generated_at,
            ):
                print(line)
            return 0

        result = diff_files(args.old, args.new, generated_at=generated_at)
        json.dump(result, sys.stdout, indent=2)
        print()
        return 0
    except SchemaMismatchError as exc:
        _emit_schema_incompatible(exc)
        return 3
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _run_rekey(argv: list[str]) -> int:
    from athar.rekey import RekeyRefused, rekey_files

    parser = argparse.ArgumentParser(prog="athar rekey", description=_COMMAND_HELP["rekey"])
    parser.add_argument("base")
    parser.add_argument("new")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(rekey_files(args.base, args.new, args.output), indent=2))
    except SchemaMismatchError as exc:
        _emit_schema_incompatible(exc)
        return 3
    except RekeyRefused as exc:
        print(json.dumps(exc.diagnostics, indent=2))
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


def _run_git(argv: list[str]) -> int:
    from athar_git.cli import build_git_parser, run_git_command

    return run_git_command(build_git_parser().parse_args(argv))


def _run_store(argv: list[str]) -> int:
    from athar_store.cli import build_store_parser, run_store

    return run_store(build_store_parser().parse_args(argv))


def _run_view(argv: list[str]) -> int:
    from athar_view.cli import build_view_parser, run_view

    return run_view(build_view_parser().parse_args(argv))


def _emit_schema_incompatible(exc: SchemaMismatchError) -> None:
    """Surface a cross-schema pair as a clear structured result, not churn.

    Exit code 3 distinguishes "schemas differ, cannot semantically diff" from
    a generic execution error (exit 1). The JSON document gives automation a
    parseable signal; the stderr line keeps the human message visible.
    """
    result = {
        "engine": "athar",
        "status": "schema_incompatible",
        "schemas": {"old": exc.old_schema, "new": exc.new_schema},
        "message": str(exc),
    }
    json.dump(result, sys.stdout, indent=2)
    print()
    print(f"Error: {exc}", file=sys.stderr)


def _run_check(argv: list[str]) -> int:
    from athar.check import (
        builtin_policy_packs,
        evaluate_report,
        load_report,
        resolve_policy,
        schema_mismatch_result,
    )

    parser = argparse.ArgumentParser(prog="athar check")
    parser.add_argument("old", nargs="?", help="Path to old IFC file")
    parser.add_argument("new", nargs="?", help="Path to new IFC file")
    parser.add_argument(
        "--report",
        help="Evaluate an existing Athar JSON report instead of diffing IFC files",
    )
    parser.add_argument(
        "--policy",
        help="A JSON policy file path, or the name of a shipped policy pack (see --list-packs)",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format for the verdict (default: json)",
    )
    parser.add_argument(
        "--list-packs",
        action="store_true",
        help="List the shipped policy packs and exit",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Print the normalized policy and exit",
    )
    args = parser.parse_args(argv)

    if args.list_packs:
        return _list_policy_packs(builtin_policy_packs)

    if args.policy is None:
        parser.error("the following arguments are required: --policy")
    if args.explain:
        try:
            policy = resolve_policy(args.policy)
        except (OSError, ValueError) as exc:
            if args.format == "text":
                print(f"Error: {exc}")
            else:
                print(f"Error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(policy, indent=2, sort_keys=True))
        return 0

    using_report = args.report is not None
    using_files = args.old is not None or args.new is not None
    if using_report and using_files:
        parser.error("use either --report or old/new IFC paths, not both")
    if not using_report and not (args.old and args.new):
        parser.error("the following arguments are required: old, new unless --report is used")

    try:
        policy = resolve_policy(args.policy)
        report = load_report(args.report) if using_report else diff_files(args.old, args.new)
        result = evaluate_report(report, policy)
    except SchemaMismatchError as exc:
        result = schema_mismatch_result(exc.old_schema, exc.new_schema)
    except (OSError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if args.format == "text":
        from athar_qa import render_check_report

        print(render_check_report(result), end="")
    else:
        json.dump(result, sys.stdout, indent=2)
        print()
    return 0 if result["ok"] else 2


def _list_policy_packs(builtin_policy_packs) -> int:
    packs = builtin_policy_packs()
    if not packs:
        print("No policy packs are installed.")
        return 0
    for name, path in packs.items():
        description = ""
        try:
            with open(path, encoding="utf-8") as fh:
                description = json.load(fh).get("description", "")
        except Exception:  # pragma: no cover - defensive
            pass
        print(f"{name}\t{description}" if description else name)
    return 0


def _generated_at_arg(value: str | None) -> str | None:
    if value is None:
        return None
    if value == "now":
        return generated_at_now_utc()
    if not value.strip():
        raise ValueError("--generated-at must not be empty")
    return value


_COMMANDS = {
    "diff": _run_diff,
    "check": _run_check,
    "git": _run_git,
    "view": _run_view,
    "store": _run_store,
    "rekey": _run_rekey,
}


if __name__ == "__main__":
    sys.exit(main())
