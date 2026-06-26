"""Minimal CLI for the Athar core engine."""

from __future__ import annotations

import argparse
import json
import sys

from athar.engine import diff_files, generated_at_now_utc, stream_diff_files


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "git":
        from athar_git.cli import build_git_parser, run_git_command

        git_parser = build_git_parser()
        args = git_parser.parse_args(sys.argv[2:])
        sys.exit(run_git_command(args))

    if len(sys.argv) > 1 and sys.argv[1] == "check":
        sys.exit(_run_check(sys.argv[2:]))

    if len(sys.argv) > 1 and sys.argv[1] == "view":
        from athar_view.cli import build_view_parser, run_view

        view_parser = build_view_parser()
        view_args = view_parser.parse_args(sys.argv[2:])
        sys.exit(run_view(view_args))

    parser = argparse.ArgumentParser(prog="athar-core")
    parser.add_argument("old", nargs="?", help="Path to old IFC file")
    parser.add_argument("new", nargs="?", help="Path to new IFC file")
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
    args = parser.parse_args()

    if not args.old or not args.new:
        parser.error("the following arguments are required: old, new")

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
            return

        result = diff_files(args.old, args.new, generated_at=generated_at)
        json.dump(result, sys.stdout, indent=2)
        print()
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


def _run_check(argv: list[str]) -> int:
    from athar.check import evaluate_report, load_policy, load_report

    parser = argparse.ArgumentParser(prog="athar check")
    parser.add_argument("old", nargs="?", help="Path to old IFC file")
    parser.add_argument("new", nargs="?", help="Path to new IFC file")
    parser.add_argument(
        "--report",
        help="Evaluate an existing Athar JSON report instead of diffing IFC files",
    )
    parser.add_argument(
        "--policy",
        required=True,
        help="Path to a JSON policy file",
    )
    args = parser.parse_args(argv)

    using_report = args.report is not None
    using_files = args.old is not None or args.new is not None
    if using_report and using_files:
        parser.error("use either --report or old/new IFC paths, not both")
    if not using_report and not (args.old and args.new):
        parser.error("the following arguments are required: old, new unless --report is used")

    try:
        policy = load_policy(args.policy)
        report = load_report(args.report) if using_report else diff_files(args.old, args.new)
        result = evaluate_report(report, policy)
        json.dump(result, sys.stdout, indent=2)
        print()
        return 0 if result["ok"] else 2
    except Exception as exc:  # pragma: no cover - CLI error path
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _generated_at_arg(value: str | None) -> str | None:
    if value is None:
        return None
    if value == "now":
        return generated_at_now_utc()
    if not value.strip():
        raise ValueError("--generated-at must not be empty")
    return value


if __name__ == "__main__":
    main()
