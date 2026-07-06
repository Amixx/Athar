#!/usr/bin/env python3
"""Benchmark Athar against runnable IFC diff tools on curated pairs.

The default pair set is intentionally small enough for normal development but
covers both real corpus pairs and synthetic IfcOpenShell mutations with known
ground truth. Larger corpus stress pairs are opt-in via ``--large``; the Revit
round-trip corpus (real revision edits and cross-schema refusals, from
``corpus/roundtrip-revit-2026-07-06/``) is opt-in via ``--revit``.

Usage:
    python scripts/explore/benchmark_competitors.py --out /tmp/athar_benchmark.json
    python scripts/explore/benchmark_competitors.py --revit --only r8_r9 --repeats 1
"""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import math
import os
import platform
import re
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import athar.engine as athar_engine  # noqa: E402
from athar_dev import ifc_mutations as mutations  # noqa: E402
from athar_dev.changeset_scoring import score_changeset  # noqa: E402
from tests.corpus import CORPUS, acceptance_path, assert_report_invariants, corpus_path  # noqa: E402

_SCRAMBLE_NAMESPACE = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")
_TIME_MAX_RSS_RE = re.compile(r"(?P<rss>\d+)\s+maximum resident set size")

Counts = dict[str, int]
PairBuilder = Callable[[Path], tuple[str, str]]


@dataclass(frozen=True)
class Expected:
    """Machine-checkable expectation for a benchmark pair."""

    counts: Counts | None = None
    athar: dict[str, Any] | None = None
    changed_guids: dict[str, list[str]] | None = None
    note: str = ""


@dataclass(frozen=True)
class Pair:
    name: str
    kind: str
    builder: PairBuilder
    expectation: str
    expected: Expected | None = None
    large: bool = False
    revit: bool = False
    set_name: str = "synthetic"


def _static_pair(old_key: str, new_key: str, *, acceptance: bool = False) -> PairBuilder:
    resolver = acceptance_path if acceptance else corpus_path

    def build(_workdir: Path) -> tuple[str, str]:
        return resolver(old_key), resolver(new_key)

    return build


def _ifcopenshell_rewrite_pair(key: str) -> PairBuilder:
    """Same model, both sides written through IfcOpenShell."""

    def build(workdir: Path) -> tuple[str, str]:
        import ifcopenshell

        src = corpus_path(key)
        workdir.mkdir(parents=True, exist_ok=True)
        old_path = workdir / f"{key}-rewrite-old.ifc"
        new_path = workdir / f"{key}-rewrite-new.ifc"
        ifcopenshell.open(src).write(old_path)
        ifcopenshell.open(src).write(new_path)
        return str(old_path), str(new_path)

    return build


def _asymmetric_rewrite_pair(key: str) -> PairBuilder:
    """Original file vs its IfcOpenShell rewrite — the export-churn scenario."""

    def build(workdir: Path) -> tuple[str, str]:
        import ifcopenshell

        src = corpus_path(key)
        workdir.mkdir(parents=True, exist_ok=True)
        new_path = workdir / f"{key}-rewritten.ifc"
        ifcopenshell.open(src).write(new_path)
        return src, str(new_path)

    return build


def _owner_history_churn_pair(key: str) -> PairBuilder:
    """Only OwnerHistory metadata changes; core hashes intentionally ignore it."""

    def build(workdir: Path) -> tuple[str, str]:
        import ifcopenshell

        src = corpus_path(key)
        workdir.mkdir(parents=True, exist_ok=True)
        old_path = workdir / f"{key}-owner-history-old.ifc"
        new_path = workdir / f"{key}-owner-history-new.ifc"
        ifcopenshell.open(src).write(old_path)
        model = ifcopenshell.open(src)
        changed = 0
        for history in model.by_type("IfcOwnerHistory"):
            if hasattr(history, "ChangeAction"):
                history.ChangeAction = "MODIFIED"
                changed += 1
            if hasattr(history, "CreationDate") and history.CreationDate:
                history.CreationDate = int(history.CreationDate) + 86_400
                changed += 1
        if changed == 0:
            raise RuntimeError(f"{key} has no editable OwnerHistory metadata")
        model.write(new_path)
        return str(old_path), str(new_path)

    return build


def _guid_scramble_pair(key: str) -> PairBuilder:
    def build(workdir: Path) -> tuple[str, str]:
        import ifcopenshell
        import ifcopenshell.guid

        src = corpus_path(key)
        workdir.mkdir(parents=True, exist_ok=True)
        clean = workdir / f"{key}-clean.ifc"
        scrambled = workdir / f"{key}-scrambled.ifc"
        model = ifcopenshell.open(src)
        model.write(str(clean))
        for entity in model:
            if not hasattr(entity, "GlobalId"):
                continue
            guid = entity.GlobalId
            if isinstance(guid, str) and guid.strip():
                entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(_SCRAMBLE_NAMESPACE, guid).hex)
        model.write(str(scrambled))
        return str(clean), str(scrambled)

    return build


def _mutation_pair(key: str, mutator: Callable[[Any], mutations.Mutation]) -> PairBuilder:
    def build(workdir: Path) -> tuple[str, str]:
        import ifcopenshell

        src = corpus_path(key)
        workdir.mkdir(parents=True, exist_ok=True)
        baseline = workdir / f"{key}-{mutator.__name__}-baseline.ifc"
        mutated = workdir / f"{key}-{mutator.__name__}.ifc"
        ifcopenshell.open(src).write(baseline)
        model = ifcopenshell.open(src)
        mutator(model)
        model.write(mutated)
        return str(baseline), str(mutated)

    return build


ZERO_DIFF = Expected(
    counts={"added": 0, "deleted": 0, "modified": 0},
    athar={"matched_by_tier": {"guid": 0}},
    note="No semantic product changes expected.",
)


BENCHMARK_PAIRS: tuple[Pair, ...] = (
    Pair(
        name="same_file_gni_190",
        kind="same_file",
        builder=_static_pair("gni_190", "gni_190"),
        expectation="zero semantic changes",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
    ),
    Pair(
        name="reserialize_gni_190",
        kind="reserialize",
        builder=_ifcopenshell_rewrite_pair("gni_190"),
        expectation="zero semantic changes after IfcOpenShell rewrite",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
    ),
    Pair(
        name="reserialize_asymmetric_gni_190",
        kind="reserialize_asymmetric",
        builder=_asymmetric_rewrite_pair("gni_190"),
        expectation="zero semantic changes when only one side is rewritten (line order/formatting churn)",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
    ),
    Pair(
        name="owner_history_churn_gni_190",
        kind="metadata_churn",
        builder=_owner_history_churn_pair("gni_190"),
        expectation="zero semantic changes when only OwnerHistory changes",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
    ),
    Pair(
        name="revision_building_landscaping_v1_v2",
        kind="revision",
        builder=_static_pair("bl_v1", "bl_v2"),
        expectation="mostly GUID matches, limited added/deleted churn",
    ),
    Pair(
        name="guid_scramble_gni_190",
        kind="guid_scramble",
        builder=_guid_scramble_pair("gni_190"),
        expectation="zero semantic changes without GUID-tier matches",
        expected=ZERO_DIFF,
    ),
    Pair(
        name="mutation_guid_scramble_gni_190",
        kind="synthetic_guid_scramble",
        builder=_mutation_pair("gni_190", mutations.scramble_guids),
        expectation="synthetic GUID scramble: zero semantic changes without GUID-tier matches",
        expected=ZERO_DIFF,
    ),
    Pair(
        name="mutation_duplicate_guid_gni_190",
        kind="synthetic_duplicate_guid",
        builder=_mutation_pair("gni_190", mutations.duplicate_guid),
        expectation="duplicated GlobalId is surfaced but not used as identity evidence",
        expected=Expected(
            counts={"added": 0, "deleted": 0, "modified": 0},
            athar={"guid_collisions": {"old": 0, "new": 1}},
        ),
    ),
    Pair(
        name="mutation_rename_product_gni_190",
        kind="synthetic_data",
        builder=_mutation_pair("gni_190", mutations.rename_product),
        expectation="one product Name scalar changed",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 1}),
    ),
    Pair(
        name="mutation_pset_value_gni_190",
        kind="synthetic_data",
        builder=_mutation_pair("gni_190", mutations.edit_pset_value),
        expectation="one exclusive property-set value changed",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 1}),
    ),
    Pair(
        name="mutation_type_pset_value_gni_190",
        kind="synthetic_inherited_data",
        builder=_mutation_pair("gni_190", mutations.edit_type_pset_value),
        expectation="one type property value changes inherited occurrence data",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 28}),
    ),
    Pair(
        name="mutation_move_product_gni_190",
        kind="synthetic_placement",
        builder=_mutation_pair("gni_190", mutations.move_product),
        expectation="one product placement changed",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 1}),
    ),
    Pair(
        name="mutation_delete_product_gni_190",
        kind="synthetic_delete",
        builder=_mutation_pair("gni_190", mutations.delete_product),
        expectation="one product deleted, with possible transitive fallout",
        expected=Expected(counts={"added": 0, "deleted": 1, "modified_max": 20}),
    ),
    Pair(
        name="mutation_add_product_gni_190",
        kind="synthetic_add",
        builder=_mutation_pair("gni_190", mutations.add_product),
        expectation="one product added, with possible transitive fallout",
        expected=Expected(counts={"added": 1, "deleted": 0, "modified_max": 20}),
    ),
    Pair(
        name="large_same_file_gni_p5_arc",
        kind="large_same_file",
        builder=_static_pair("gni_p5_arc", "gni_p5_arc", acceptance=True),
        expectation="76MB same-file: zero semantic changes, isolates parse cost",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
        large=True,
    ),
    Pair(
        name="large_same_file_spanish_180mb",
        kind="large_same_file",
        builder=_static_pair("spanish", "spanish", acceptance=True),
        expectation="180MB same-file: zero semantic changes, parse-bound stress",
        expected=Expected(counts={"added": 0, "deleted": 0, "modified": 0}),
        large=True,
    ),
)


_REVIT_CORPUS_DIR = REPO_ROOT / "corpus" / "roundtrip-revit-2026-07-06"
_REVIT_EXPORTS_DIR = _REVIT_CORPUS_DIR / "exports"
_REVIT_GROUND_TRUTH_PATH = _REVIT_CORPUS_DIR / "ground_truth.json"


def _revit_export_path(name: str) -> Path:
    """Resolve one Revit round-trip export, decompressing the committed .zst if needed."""

    ifc_path = _REVIT_EXPORTS_DIR / f"{name}.ifc"
    if ifc_path.exists():
        return ifc_path
    zst_path = _REVIT_EXPORTS_DIR / f"{name}.ifc.zst"
    if not zst_path.exists():
        raise FileNotFoundError(f"missing Revit export {name!r}: neither {ifc_path} nor {zst_path} exist")
    if shutil.which("zstd") is None:
        raise RuntimeError(f"zstd not found on PATH; cannot decompress {zst_path} to {ifc_path}")
    subprocess.run(["zstd", "-d", "-k", str(zst_path)], check=True, capture_output=True)
    if not ifc_path.exists():
        raise RuntimeError(f"zstd decompression of {zst_path} did not produce {ifc_path}")
    return ifc_path


def _revit_pair(old_name: str, new_name: str) -> PairBuilder:
    def build(_workdir: Path) -> tuple[str, str]:
        return str(_revit_export_path(old_name)), str(_revit_export_path(new_name))

    return build


def _load_revit_ground_truth() -> dict[str, dict]:
    return json.loads(_REVIT_GROUND_TRUTH_PATH.read_text(encoding="utf-8"))


_REVIT_GROUND_TRUTH = _load_revit_ground_truth()


def _revit_expected(key: str) -> Expected:
    raw = _REVIT_GROUND_TRUTH[key]
    return Expected(
        counts=raw.get("counts"),
        athar=raw.get("athar"),
        changed_guids=raw.get("changed_guids"),
        note=raw.get("note", ""),
    )


REVIT_ROUNDTRIP_PAIRS: tuple[Pair, ...] = (
    Pair(
        name="roundtrip_orig_r1",
        kind="revit_roundtrip_import",
        builder=_revit_pair("original", "r1"),
        expectation=(
            "Archicad IFC2X3 source vs Revit's default IFC2X3 re-export straight after import, "
            "no edits: import collapses granularity (44,389->886 products), so heavy add/delete "
            "churn documents that loss, not engine noise."
        ),
        expected=_revit_expected("orig_r1"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r2_r3",
        kind="revit_roundtrip_noop",
        builder=_revit_pair("r2", "r3"),
        expectation=(
            "IFC4 no-op re-export (+ 'keep GUIDs' option, zero model edits): ground truth is "
            "empty; the residual ~2 modified are Revit's nondeterministic mesh re-serialization "
            "(tessellation reorder wobble), counted as false positives."
        ),
        expected=_revit_expected("r2_r3"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r3_r4",
        kind="revit_roundtrip_data",
        builder=_revit_pair("r3", "r4"),
        expectation=(
            "Added 'export Revit property sets' option, no model edits: every occurrence "
            "gains property-set data (data aspect changes only)."
        ),
        expected=_revit_expected("r3_r4"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r4_r5",
        kind="revit_roundtrip_placement",
        builder=_revit_pair("r4", "r5"),
        expectation=(
            "Moved the whole extruded facade away from the building: 0 added/deleted, "
            "placement changes on the moved elements."
        ),
        expected=_revit_expected("r4_r5"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r5_r6",
        kind="revit_roundtrip_delete",
        builder=_revit_pair("r5", "r6"),
        expectation=(
            "Deleted a row of windows, merged into one Generic Model proxy at import: "
            "exactly one deletion."
        ),
        expected=_revit_expected("r5_r6"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r6_r7",
        kind="revit_roundtrip_data",
        builder=_revit_pair("r6", "r7"),
        expectation=(
            "One door instance property ('Frame type' = 'Test frame type') edited: "
            "exactly one intrinsic modification."
        ),
        expected=_revit_expected("r6_r7"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r7_r8",
        kind="revit_roundtrip_add",
        builder=_revit_pair("r7", "r8"),
        expectation=(
            "Duplicated (pasted) the edited door twice onto other walls: 2 IfcDoor + "
            "2 IfcOpeningElement added with fresh GUIDs."
        ),
        expected=_revit_expected("r7_r8"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r8_r9",
        kind="revit_roundtrip_noop",
        builder=_revit_pair("r8", "r9"),
        expectation=(
            "Same-settings determinism pair, zero model edits: ground truth is empty; the "
            "residual ~2 modified are Revit's nondeterministic mesh re-serialization "
            "(tessellation reorder wobble), counted as false positives."
        ),
        expected=_revit_expected("r8_r9"),
        revit=True,
        set_name="revit",
    ),
    Pair(
        name="roundtrip_r9_r10",
        kind="revit_roundtrip_data",
        builder=_revit_pair("r9", "r10"),
        expectation=(
            "Door type property value edited: fans out to all 11 occurrences of that type, "
            "each an intrinsic modification."
        ),
        expected=_revit_expected("r9_r10"),
        revit=True,
        set_name="revit",
    ),
)


ALL_PAIRS: tuple[Pair, ...] = BENCHMARK_PAIRS + REVIT_ROUNDTRIP_PAIRS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="/tmp/athar_competitor_benchmark.json")
    parser.add_argument("--workdir", default="/tmp/athar_competitor_benchmark")
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3, help="runs per tool; medians are reported")
    parser.add_argument("--large", action="store_true", help="include opt-in large corpus pairs")
    parser.add_argument("--revit", action="store_true", help="include opt-in Revit round-trip corpus pairs")
    parser.add_argument("--only", default=None, help="run only pairs whose name contains this substring")
    parser.add_argument("--no-ifcfast", action="store_true", help="disable optional ifcfast runner even if installed")
    parser.add_argument("--no-speckle", action="store_true", help="disable optional Speckle runner even if installed")
    parser.add_argument("--no-ifcgit", action="store_true", help="disable the IfcGit-port runner")
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    workdir = Path(args.workdir)
    tools = _tool_inventory(
        include_ifcfast=not args.no_ifcfast,
        include_speckle=not args.no_speckle,
        include_ifcgit=not args.no_ifcgit,
    )
    results = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "repo": str(REPO_ROOT),
        "environment": _environment(),
        "settings": {"timeout_s": args.timeout_s, "repeats": args.repeats, "large": args.large, "revit": args.revit},
        "tools": tools,
        "pairs": [],
        "summary": [],
    }

    for pair in ALL_PAIRS:
        if pair.large and not args.large:
            continue
        if pair.revit and not args.revit:
            continue
        if args.only and args.only not in pair.name:
            continue
        print(f"[{time.strftime('%H:%M:%S')}] pair {pair.name}", flush=True)
        pair_workdir = workdir / pair.name
        try:
            old_path, new_path = pair.builder(pair_workdir)
        except BaseException as exc:  # noqa: BLE001 - benchmark records skips/failures
            entry = {
                "name": pair.name,
                "kind": pair.kind,
                "set": pair.set_name,
                "expectation": pair.expectation,
                "expected": _expected_json(pair.expected),
                "error": f"{type(exc).__name__}: {exc}",
                "runs": {},
            }
            results["pairs"].append(entry)
            results["summary"].append(_summary_row(entry))
            _write_json(Path(args.out), results)
            continue

        entry = {
            "name": pair.name,
            "kind": pair.kind,
            "set": pair.set_name,
            "expectation": pair.expectation,
            "expected": _expected_json(pair.expected),
            "old": _input_summary(old_path),
            "new": _input_summary(new_path),
            "runs": {
                "athar": _repeat(lambda: _run_athar(old_path, new_path, args.timeout_s), args.repeats),
                "ifcdiff_default": _repeat(
                    lambda: _run_ifcdiff(old_path, new_path, pair_workdir / "ifcdiff_default", args.timeout_s),
                    args.repeats,
                ),
                "ifcdiff_relationships": _repeat(
                    lambda: _run_ifcdiff(
                        old_path,
                        new_path,
                        pair_workdir / "ifcdiff_relationships",
                        args.timeout_s,
                        relationships="type property container aggregate classification",
                    ),
                    args.repeats,
                ),
            },
        }
        if tools.get("ifcfast", {}).get("available"):
            entry["runs"]["ifcfast"] = _repeat(lambda: _run_ifcfast(old_path, new_path, args.timeout_s), args.repeats)
        if tools.get("speckle_local", {}).get("available"):
            entry["runs"]["speckle_local"] = _repeat(
                lambda: _run_json_runner(_SPECKLE_RUNNER, old_path, new_path, args.timeout_s), args.repeats
            )
        if tools.get("ifcgit_port", {}).get("available"):
            entry["runs"]["ifcgit_port"] = _repeat(
                lambda: _run_json_runner(_IFCGIT_RUNNER, old_path, new_path, args.timeout_s), args.repeats
            )
        entry["assessment"] = _assess_pair(entry, pair.expected)
        entry["changeset_scores"] = _score_pair(entry, pair.expected)
        results["pairs"].append(entry)
        results["summary"].append(_summary_row(entry))
        _write_json(Path(args.out), results)

    _write_json(Path(args.out), results)
    print(f"wrote {args.out}", flush=True)
    return 0


def _repeat(fn: Callable[[], dict], repeats: int) -> dict:
    runs = [fn() for _ in range(repeats)]
    best_counts = _first_counts(runs)
    ok_seconds = [
        run["seconds"]
        for run in runs
        if run.get("status") in ("ok", "refused") and isinstance(run.get("seconds"), (int, float))
    ]
    peak_rss_values = [
        run["peak_rss_mb"] for run in runs if run.get("peak_rss_mb") is not None and isinstance(run.get("peak_rss_mb"), (int, float))
    ]
    gb_seconds_values = [
        run["gb_seconds"] for run in runs if run.get("gb_seconds") is not None and isinstance(run.get("gb_seconds"), (int, float))
    ]
    result = dict(runs[-1])
    result["runs"] = runs
    result["repeat_count"] = repeats
    if ok_seconds:
        result["seconds_median"] = round(statistics.median(ok_seconds), 3)
        result["seconds_min"] = round(min(ok_seconds), 3)
        result["seconds_max"] = round(max(ok_seconds), 3)
    if peak_rss_values:
        result["peak_rss_mb_max"] = round(max(peak_rss_values), 1)
    if gb_seconds_values:
        result["gb_seconds_median"] = round(statistics.median(gb_seconds_values), 3)
    if best_counts is not None:
        result["counts"] = best_counts
    return result


def _run_athar(old_path: str, new_path: str, timeout_s: int) -> dict:
    # Run via the CLI as a subprocess, sampled by the same mechanism as every
    # other tool (no in-process special-casing) — same startup cost, same RSS
    # sampling, same GB·s definition. `python -m athar old new` prints the report
    # JSON to stdout.
    argv = [sys.executable, "-m", "athar", old_path, new_path]
    start = time.perf_counter()
    completed = _run_subprocess(argv, timeout_s)
    seconds = round(time.perf_counter() - start, 3)
    mem = {"peak_rss_mb": completed.get("peak_rss_mb"), "gb_seconds": completed.get("gb_seconds")}
    if completed.get("status") != "completed":
        return completed | {"seconds": seconds}
    if completed["returncode"] == 3:
        try:
            payload = json.loads(completed["stdout"])
        except Exception:  # noqa: BLE001
            payload = {}
        return {
            "status": "refused",
            "reason": payload.get("status") or "unknown",
            "message": payload.get("message"),
            "schemas": payload.get("schemas"),
            "seconds": seconds,
            **mem,
        }
    if completed["returncode"] != 0:
        return {"status": "error", "seconds": seconds, "error": completed.get("stderr", "")[:500], **mem}
    try:
        report = json.loads(completed["stdout"])
        assert_report_invariants(report)
        stats = report["stats"]
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "seconds": seconds, "error": f"{type(exc).__name__}: {exc}", **mem}
    return {
        "status": "ok",
        "seconds": seconds,
        **mem,
        "sections": {name: stats[name] for name in ("added", "deleted", "modified", "unchanged")},
        "counts": {name: stats[name] for name in ("added", "deleted", "modified", "unchanged")},
        "reported_guids": _athar_reported_guids(report),
        "signatures": {"old": stats["old_signatures"], "new": stats["new_signatures"]},
        "matched_by_tier": stats["matcher_diagnostics"]["matched_by_tier"],
        "modified_change_scope": stats["modified_change_scope"],
        "guid_collisions": stats["guid_collisions"],
    }


def _athar_reported_guids(report: dict) -> dict[str, list[str]]:
    def flat_guids(section: str) -> list[str]:
        return [entry["guid"] for entry in report.get(section, []) if entry.get("guid")]

    def modified_guids() -> list[str]:
        out = []
        for entry in report.get("modified", []):
            guid = (entry.get("new") or entry.get("old") or {}).get("guid")
            if guid:
                out.append(guid)
        return out

    return {
        "added": flat_guids("added"),
        "deleted": flat_guids("deleted"),
        "modified": modified_guids(),
    }


def _run_ifcdiff(
    old_path: str,
    new_path: str,
    workdir: Path,
    timeout_s: int,
    *,
    relationships: str | None = None,
) -> dict:
    if importlib.util.find_spec("ifcdiff") is None:
        return {"status": "error", "reason": "install benchmark extra: python -m pip install '.[benchmark]'"}

    output_path = workdir / "ifcdiff.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    argv = [
        sys.executable,
        "-m",
        "ifcdiff",
        "-o",
        str(output_path),
        old_path,
        new_path,
    ]
    if relationships:
        argv[5:5] = ["-r", relationships]
    start = time.perf_counter()
    completed = _run_subprocess(argv, timeout_s)
    if completed.get("status") != "completed":
        return completed | {"seconds": round(time.perf_counter() - start, 3), "relationships": relationships or "default"}

    payload = None
    if output_path.exists():
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            payload = {"parse_error": str(exc)}
    return {
        "status": "ok" if completed["returncode"] == 0 and isinstance(payload, dict) else "error",
        "returncode": completed["returncode"],
        "seconds": round(time.perf_counter() - start, 3),
        "peak_rss_mb": completed.get("peak_rss_mb"),
        "gb_seconds": completed.get("gb_seconds"),
        "relationships": relationships or "default",
        "counts": _ifcdiff_counts(payload),
        "output": str(output_path),
        "stdout_preview": completed.get("stdout", "")[:2000],
        "stderr_preview": completed.get("stderr", "")[:2000],
    }


_SPECKLE_RUNNER = REPO_ROOT / "scripts" / "explore" / "speckle_diff_runner.py"
_IFCGIT_RUNNER = REPO_ROOT / "scripts" / "explore" / "ifcgit_diff_runner.py"


def _run_json_runner(runner: Path, old_path: str, new_path: str, timeout_s: int) -> dict:
    argv = [sys.executable, str(runner), old_path, new_path]
    start = time.perf_counter()
    completed = _run_subprocess(argv, timeout_s)
    seconds = round(time.perf_counter() - start, 3)
    mem = {"peak_rss_mb": completed.get("peak_rss_mb"), "gb_seconds": completed.get("gb_seconds")}
    if completed.get("status") != "completed":
        return completed | {"seconds": seconds}
    try:
        payload = json.loads(completed["stdout"])
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "seconds": seconds, "error": f"{type(exc).__name__}: {exc}", **mem}
    if completed["returncode"] != 0 or "error" in payload:
        error = str(payload.get("error") or completed.get("stderr", ""))[:500]
        return {"status": "error", "seconds": seconds, "error": error, **mem}
    extras = {key: value for key, value in payload.items() if key not in ("tool", "counts")}
    return {"status": "ok", "seconds": seconds, **mem, "counts": payload.get("counts"), "extras": extras}


_IFCFAST_RUNNER = (
    "import sys, json, ifcfast; "
    "m = ifcfast.open(sys.argv[1], use_cache=False, write_cache=False); "
    "print(json.dumps(m.diff(sys.argv[2], sample=5), default=str))"
)


def _run_ifcfast(old_path: str, new_path: str, timeout_s: int) -> dict:
    if importlib.util.find_spec("ifcfast") is None:
        return {"status": "error", "reason": "install benchmark extra: python -m pip install '.[benchmark]'"}
    # Subprocess, sampled identically to the other tools (was in-process before).
    argv = [sys.executable, "-c", _IFCFAST_RUNNER, old_path, new_path]
    start = time.perf_counter()
    completed = _run_subprocess(argv, timeout_s)
    seconds = round(time.perf_counter() - start, 3)
    mem = {"peak_rss_mb": completed.get("peak_rss_mb"), "gb_seconds": completed.get("gb_seconds")}
    if completed.get("status") != "completed":
        return completed | {"seconds": seconds}
    if completed["returncode"] != 0:
        return {"status": "error", "seconds": seconds, "error": completed.get("stderr", "")[:500], **mem}
    try:
        payload = json.loads(completed["stdout"])
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "seconds": seconds, "error": f"{type(exc).__name__}: {exc}", **mem}
    return {
        "status": "ok",
        "seconds": seconds,
        **mem,
        "counts": _ifcfast_counts(payload),
        "output_preview": payload,
    }


def _run_subprocess(argv: list[str], timeout_s: int) -> dict:
    # Run the tool directly under Popen and sample its RSS over time, so the
    # same mechanism yields both peak RSS and GB·s. (Replaces the previous
    # `/usr/bin/time -l` wrapper, whose exact peak we trade for a sampled one —
    # acceptable since peak RSS is itself noisy and GB·s is the better metric.)
    try:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    sampler = _RssTimeSampler(proc.pid)
    timed_out = False
    with sampler:
        try:
            stdout, stderr = proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            timed_out = True
    if timed_out:
        return {"status": "timeout", "seconds": timeout_s}
    return {
        "status": "completed",
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "peak_rss_mb": sampler.peak_mb(),
        "gb_seconds": sampler.gb_seconds(0.0),
    }


def _ifcdiff_counts(payload: object) -> dict | None:
    if not isinstance(payload, dict):
        return None
    changed = payload.get("changed", {})
    return {
        "added": len(payload.get("added") or []),
        "deleted": len(payload.get("deleted") or []),
        "modified": len(changed) if isinstance(changed, dict) else 0,
    }


def _ifcfast_counts(payload: object) -> dict | None:
    if not isinstance(payload, dict):
        return None
    products = payload.get("products")
    if not isinstance(products, dict):
        return None
    return {
        "added": int(products.get("added_count") or 0),
        "deleted": int(products.get("removed_count") or 0),
        "modified": int(products.get("changed_count") or 0),
        "unchanged": int(products.get("kept") or 0) - int(products.get("changed_count") or 0),
    }


def _assess_pair(entry: dict, expected: Expected | None) -> dict[str, str]:
    if expected is None:
        return {name: "observed" for name in entry.get("runs", {})}
    return {name: _assess_run(name, run, expected) for name, run in entry.get("runs", {}).items()}


def _assess_run(name: str, run: dict, expected: Expected) -> str:
    expected_refusal = (expected.athar or {}).get("refused") if name == "athar" and expected.athar else None
    if expected_refusal:
        if run.get("status") == "refused":
            return "expected" if run.get("reason") == expected_refusal else "unexpected"
        if run.get("status") == "ok":
            return "unexpected"
        return "not_run"
    if run.get("status") == "refused":
        # Athar refused a pair no expectation said it should; other tools never refuse today.
        return "unexpected" if name == "athar" else "not_run"
    if run.get("status") != "ok":
        return "not_run"
    counts = _run_counts(run)
    assessed = False
    if expected.counts and counts:
        assessed = True
        for key, value in expected.counts.items():
            if key.endswith("_min"):
                actual_key = key[: -len("_min")]
                if counts.get(actual_key, 0) < value:
                    return "unexpected"
                continue
            if key.endswith("_max"):
                actual_key = key[: -len("_max")]
                if counts.get(actual_key, 0) > value:
                    return "unexpected"
                continue
            if counts.get(key) != value:
                return "unexpected"
    if name == "athar" and expected.athar:
        if "matched_total" in expected.athar:
            assessed = True
            matched = sum((run.get("matched_by_tier") or {}).values())
            if matched != expected.athar["matched_total"]:
                return "unexpected"
        if "matched_by_tier" in expected.athar:
            assessed = True
            for tier, value in expected.athar["matched_by_tier"].items():
                if (run.get("matched_by_tier") or {}).get(tier) != value:
                    return "unexpected"
        if "guid_collisions" in expected.athar:
            assessed = True
            if run.get("guid_collisions") != expected.athar["guid_collisions"]:
                return "unexpected"
    return "expected" if assessed else "observed"


def _score_pair(entry: dict, expected: Expected | None) -> dict[str, dict]:
    if expected is None or not expected.changed_guids:
        return {}
    scores = {}
    for name, run in entry.get("runs", {}).items():
        reported = run.get("reported_guids")
        if run.get("status") == "ok" and reported is not None:
            scores[name] = score_changeset(reported, expected.changed_guids)
    return scores


def _summary_row(entry: dict) -> dict:
    row = {
        "name": entry.get("name"),
        "kind": entry.get("kind"),
        "set": entry.get("set"),
        "expectation": entry.get("expectation"),
        "assessment": entry.get("assessment", {}),
    }
    if entry.get("error"):
        row["error"] = entry["error"]
        return row
    scores = entry.get("changeset_scores", {})
    for name, run in entry.get("runs", {}).items():
        row[name] = {
            "status": run.get("status"),
            "counts": _run_counts(run),
            "seconds_median": run.get("seconds_median", run.get("seconds")),
            "peak_rss_mb_max": run.get("peak_rss_mb_max", run.get("peak_rss_mb")),
            "gb_seconds_median": run.get("gb_seconds_median", run.get("gb_seconds")),
        }
        score = scores.get(name)
        if score is not None:
            row[name]["changeset"] = {key: score[key] for key in ("precision", "recall", "f1", "tp", "fp", "fn")}
    return row


def _run_counts(run: dict) -> dict | None:
    return run.get("counts") or run.get("sections")


def _first_counts(runs: list[dict]) -> dict | None:
    for run in runs:
        counts = _run_counts(run)
        if counts is not None:
            return counts
    return None


def _tool_inventory(*, include_ifcfast: bool, include_speckle: bool = True, include_ifcgit: bool = True) -> dict:
    tools = {
        "athar": {"status": "subprocess", "version": athar_engine._package_version()},
        "ifcopenshell": {"version": _package_version("ifcopenshell")},
        "ifcdiff": {
            "module": "ifcdiff",
            "available": importlib.util.find_spec("ifcdiff") is not None,
            "version": _package_version("ifcdiff"),
        },
    }
    if include_ifcfast:
        tools["ifcfast"] = {
            "module": "ifcfast",
            "available": importlib.util.find_spec("ifcfast") is not None,
            "version": _package_version("ifcfast"),
            "notes": "Model.diff is GUID/product-table based; output normalized to added/deleted/modified counts.",
        }
    if include_speckle:
        tools["speckle_local"] = {
            "runner": str(_SPECKLE_RUNNER.relative_to(REPO_ROOT)),
            "available": importlib.util.find_spec("speckleifc") is not None,
            "version": _package_version("specklepy"),
            "notes": (
                "speckleifc (Speckle's production IFC importer, ships in specklepy) converts locally; "
                "specklepy serializer computes object hashes; Speckle version-diff semantics "
                "(applicationId=GlobalId correlation, hash equality) — no server involved."
            ),
        }
    if include_ifcgit:
        tools["ifcgit_port"] = {
            "runner": str(_IFCGIT_RUNNER.relative_to(REPO_ROOT)),
            "available": _IFCGIT_RUNNER.exists() and shutil.which("git") is not None,
            "notes": (
                "Faithful port of Bonsai tool.ifcgit (ifc_diff_ids + get_modified_step_ids): "
                "git text diff of STEP lines, propagated to products. Original is Blender-bound."
            ),
        }
    return tools


def _environment() -> dict:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "git_commit": _git_commit(),
    }


def _git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    return completed.stdout.strip() if completed.returncode == 0 else None


def _package_version(package: str) -> str | None:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None


def _input_summary(path: str) -> dict:
    spec = next((spec for spec in CORPUS.values() if spec.path == Path(path)), None)
    return {
        "path": path,
        "key": spec.key if spec else None,
        "size_bytes": Path(path).stat().st_size,
    }


def _expected_json(expected: Expected | None) -> dict | None:
    if expected is None:
        return None
    return {
        "counts": expected.counts,
        "athar": expected.athar,
        "changed_guids": expected.changed_guids,
        "note": expected.note,
    }


def _current_peak_rss_mb() -> float | None:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = usage.ru_maxrss
    if not rss:
        return None
    # macOS reports bytes; Linux reports KiB.
    return round(rss / (1024 * 1024), 1) if platform.system() == "Darwin" else round(rss / 1024, 1)


def _read_rss_mb(pid: int) -> float | None:
    """Current RSS of `pid` in MiB via `ps` (KiB on macOS and Linux).

    Every tool runs as a subprocess and is sampled this way, so the sampling
    harness stays small — `ps` is only ever forked from the lightweight parent,
    never from a process holding gigabytes (which is what inflated timings when
    Athar ran in-process). Same mechanism for every tool, by design.
    """
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None
    return int(out) / 1024.0 if out else None


class _RssTimeSampler:
    """Background thread that records (t, rss_mb) for a pid, to integrate GB·s.

    GB·s (the area under the RSS curve) fuses "how much memory" and "for how
    long" into the unit cloud platforms bill — a fairer cost metric than a
    one-point peak. For in-process tools pass the harness baseline to
    ``gb_seconds`` so only the diff's marginal memory is counted; subprocess
    tools start near zero so their raw integral is already the diff's cost.
    """

    def __init__(self, pid: int, interval: float = 0.03) -> None:
        self.pid = pid
        self.interval = interval
        self.samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            mb = _read_rss_mb(self.pid)
            if mb is not None:
                self.samples.append((time.perf_counter(), mb))
            self._stop.wait(self.interval)

    def __enter__(self) -> "_RssTimeSampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)

    def gb_seconds(self, baseline_mb: float = 0.0) -> float | None:
        if len(self.samples) < 2:
            return None
        total = 0.0
        for (t0, m0), (t1, m1) in zip(self.samples, self.samples[1:]):
            a = max(m0 - baseline_mb, 0.0)
            b = max(m1 - baseline_mb, 0.0)
            total += (a + b) / 2.0 / 1024.0 * (t1 - t0)  # MB->GB, * dt
        return round(total, 3)

    def peak_mb(self) -> float | None:
        return round(max(m for _, m in self.samples), 1) if self.samples else None


def _parse_time_peak_rss(stderr: str) -> float | None:
    match = _TIME_MAX_RSS_RE.search(stderr)
    if not match:
        return None
    return round(int(match.group("rss")) / (1024 * 1024), 1)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
