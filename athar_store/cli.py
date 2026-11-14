"""CLI entrypoint for the Athar baseline store (`athar store ...`).

Emits JSON on stdout. Exit codes:
- ``0``  success (and, for ``review``, verdict ``pass``)
- ``2``  ``review`` verdict ``fail`` (so it doubles as a CI gate)
- ``1``  execution or configuration error
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from athar.check import resolve_policy

from .store import BaselineStore, StoreError


def build_store_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="athar store")
    parser.add_argument("--root", help="Store root directory (default: $ATHAR_STORE_ROOT or ~/.athar/store)")
    sub = parser.add_subparsers(dest="store_command", required=True)

    p_init = sub.add_parser("init", help="Create a project namespace")
    p_init.add_argument("project_id")
    p_init.add_argument("--name", help="Human-readable project name")
    p_init.set_defaults(func=_cmd_init)

    p_set = sub.add_parser("set-baseline", help="Bootstrap the first accepted version for a model")
    p_set.add_argument("project_id")
    p_set.add_argument("model_key")
    p_set.add_argument("ifc", help="Path to the accepted IFC file")
    p_set.add_argument("--actor", required=True, help="Who set the baseline")
    p_set.add_argument("--note")
    p_set.add_argument("--replace", action="store_true", help="Replace an existing baseline")
    p_set.set_defaults(func=_cmd_set_baseline)

    p_review = sub.add_parser("review", help="Diff a candidate against the baseline and record the verdict")
    p_review.add_argument("project_id")
    p_review.add_argument("model_key")
    p_review.add_argument("ifc", help="Path to the candidate IFC file")
    p_review.add_argument("--actor", required=True, help="Who submitted the review")
    p_review.add_argument("--policy", help="A JSON policy file path or shipped policy pack name (CI gate)")
    p_review.set_defaults(func=_cmd_review)

    p_approve = sub.add_parser("approve", help="Promote a reviewed candidate to the new baseline")
    p_approve.add_argument("project_id")
    p_approve.add_argument("model_key")
    p_approve.add_argument("review_id")
    p_approve.add_argument("--actor", required=True, help="Who approved")
    p_approve.add_argument("--note")
    p_approve.set_defaults(func=_cmd_approve)

    p_reject = sub.add_parser("reject", help="Reject a reviewed candidate; baseline unchanged")
    p_reject.add_argument("project_id")
    p_reject.add_argument("model_key")
    p_reject.add_argument("review_id")
    p_reject.add_argument("--actor", required=True, help="Who rejected")
    p_reject.add_argument("--note")
    p_reject.set_defaults(func=_cmd_reject)

    p_baseline = sub.add_parser("baseline", help="Show the current accepted-version pointer")
    p_baseline.add_argument("project_id")
    p_baseline.add_argument("model_key")
    p_baseline.set_defaults(func=_cmd_baseline)

    p_history = sub.add_parser("history", help="Print the append-only event log")
    p_history.add_argument("project_id")
    p_history.add_argument("--model-key", help="Filter to one model lineage")
    p_history.add_argument(
        "--format", choices=("json", "text"), default="json", help="Output format (default: json)"
    )
    p_history.set_defaults(func=_cmd_history)

    p_projects = sub.add_parser("projects", help="List registered projects")
    p_projects.set_defaults(func=_cmd_projects)

    p_export = sub.add_parser("export", help="Write an artifact's exact IFC bytes to a file")
    p_export.add_argument("project_id")
    which = p_export.add_mutually_exclusive_group(required=True)
    which.add_argument("--sha", help="Artifact sha256")
    which.add_argument("--model-key", help="Export the current baseline of this model lineage")
    p_export.add_argument("--out", required=True, help="Destination file path")
    p_export.set_defaults(func=_cmd_export)

    p_stats = sub.add_parser("stats", help="Artifact storage stats (logical vs stored bytes)")
    p_stats.add_argument("project_id")
    p_stats.set_defaults(func=_cmd_stats)

    p_report = sub.add_parser("report", help="Print a review's full persisted delta report")
    p_report.add_argument("project_id")
    p_report.add_argument("review_id")
    p_report.set_defaults(func=_cmd_report)

    p_pending = sub.add_parser("pending", help="List unresolved reviews against the current baseline")
    p_pending.add_argument("project_id")
    p_pending.add_argument("model_key")
    p_pending.set_defaults(func=_cmd_pending)

    p_verify = sub.add_parser("verify", help="Verify the store record")
    p_verify.add_argument("project_id")
    p_verify.add_argument("--rebuild-pointers", action="store_true")
    p_verify.set_defaults(func=_cmd_verify)

    return parser


def run_store(args: argparse.Namespace) -> int:
    store = BaselineStore(args.root)
    try:
        return args.func(store, args)
    except StoreError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _cmd_init(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(store.init_project(args.project_id, name=args.name))


def _cmd_set_baseline(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(
        store.set_baseline(
            args.project_id,
            args.model_key,
            args.ifc,
            actor=args.actor,
            note=args.note,
            replace=args.replace,
        )
    )


def _cmd_review(store: BaselineStore, args: argparse.Namespace) -> int:
    policy = resolve_policy(args.policy) if args.policy else None
    event = store.review(args.project_id, args.model_key, args.ifc, actor=args.actor, policy=policy)
    _emit(event)
    return 0 if event["verdict"] == "pass" else 2


def _cmd_approve(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(
        store.approve(args.project_id, args.model_key, args.review_id, actor=args.actor, note=args.note)
    )


def _cmd_reject(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(
        store.reject(args.project_id, args.model_key, args.review_id, actor=args.actor, note=args.note)
    )


def _cmd_baseline(store: BaselineStore, args: argparse.Namespace) -> int:
    baseline = store.current_baseline(args.project_id, args.model_key)
    if baseline is None:
        print(f"Error: no baseline for {args.project_id}/{args.model_key}", file=sys.stderr)
        return 1
    return _emit(baseline)


def _cmd_history(store: BaselineStore, args: argparse.Namespace) -> int:
    events = store.history(args.project_id, args.model_key)
    if args.format == "text":
        from athar_qa import render_history

        print(render_history(events), end="")
        return 0
    return _emit(events)


def _cmd_projects(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(store.list_projects())


def _cmd_export(store: BaselineStore, args: argparse.Namespace) -> int:
    sha = args.sha
    if sha is None:
        baseline = store.current_baseline(args.project_id, args.model_key)
        if baseline is None:
            print(f"Error: no baseline for {args.project_id}/{args.model_key}", file=sys.stderr)
            return 1
        sha = baseline["artifact"]["sha256"]
    dest = store.export_artifact(args.project_id, sha, args.out)
    return _emit({"sha256": sha, "path": str(dest), "size_bytes": dest.stat().st_size})


def _cmd_stats(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(store.artifact_stats(args.project_id))


def _cmd_report(store: BaselineStore, args: argparse.Namespace) -> int:
    report = store.review_report(args.project_id, args.review_id)
    if report is None:
        print(f"Error: no stored report for {args.project_id}/{args.review_id}", file=sys.stderr)
        return 1
    return _emit(report)


def _cmd_pending(store: BaselineStore, args: argparse.Namespace) -> int:
    return _emit(store.pending_reviews(args.project_id, args.model_key))


def _cmd_verify(store: BaselineStore, args: argparse.Namespace) -> int:
    def show_changes(changes: list[str]) -> None:
        print(json.dumps({"pointer_changes": changes}, indent=2), file=sys.stderr, flush=True)

    result = store.verify(
        args.project_id,
        rebuild_pointers=args.rebuild_pointers,
        before_rebuild=show_changes,
    )
    _emit(result)
    return 2 if result["problems"] else 0


def _emit(payload: Any) -> int:
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    print()
    return 0
