#!/usr/bin/env python3
"""Benchmark Athar against runnable IFC diff tools on curated pairs.

The default pair set is intentionally small enough for normal development but
covers both real corpus pairs and synthetic IfcOpenShell mutations with known
ground truth. Larger corpus stress pairs are opt-in via ``--large``.

Usage:
    python scripts/explore/benchmark_competitors.py --out /tmp/athar_benchmark.json
"""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import math
import platform
import re
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import athar.engine as athar_engine  # noqa: E402
from athar_dev import ifc_mutations as mutations  # noqa: E402
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
    note: str = ""


@dataclass(frozen=True)
class Pair:
    name: str
    kind: str
    builder: PairBuilder
    expectation: str
    expected: Expected | None = None
    large: bool = False


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
        name="unrelated_gni_190_gni_9",
        kind="unrelated",
        builder=_static_pair("gni_190", "gni_9"),
        expectation="no GUID matches and most entities unmatched",
        expected=Expected(athar={"matched_total": 0}),
    ),
    Pair(
        name="discipline_gni_pair_0_arc_structure",
        kind="discipline",
        builder=_static_pair("gni_p0_arc", "gni_p0_str"),
        expectation="mostly disjoint architecture/structure content",
        expected=Expected(athar={"matched_total": 0}),
    ),
    Pair(
        name="large_unrelated_gni_77_gni_68",
        kind="large_unrelated",
        builder=_static_pair("gni_77", "gni_68", acceptance=True),
        expectation="large opt-in unrelated-pair stress shape",
        large=True,
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="/tmp/athar_competitor_benchmark.json")
    parser.add_argument("--workdir", default="/tmp/athar_competitor_benchmark")
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3, help="runs per tool; medians are reported")
    parser.add_argument("--large", action="store_true", help="include opt-in large corpus pairs")
    parser.add_argument("--no-ifcfast", action="store_true", help="disable optional ifcfast runner even if installed")
    args = parser.parse_args()

    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    workdir = Path(args.workdir)
    tools = _tool_inventory(include_ifcfast=not args.no_ifcfast)
    results = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "repo": str(REPO_ROOT),
        "environment": _environment(),
        "settings": {"timeout_s": args.timeout_s, "repeats": args.repeats, "large": args.large},
        "tools": tools,
        "pairs": [],
        "summary": [],
    }

    for pair in BENCHMARK_PAIRS:
        if pair.large and not args.large:
            continue
        print(f"[{time.strftime('%H:%M:%S')}] pair {pair.name}", flush=True)
        pair_workdir = workdir / pair.name
        try:
            old_path, new_path = pair.builder(pair_workdir)
        except BaseException as exc:  # noqa: BLE001 - benchmark records skips/failures
            entry = {
                "name": pair.name,
                "kind": pair.kind,
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
            "expectation": pair.expectation,
            "expected": _expected_json(pair.expected),
            "old": _input_summary(old_path),
            "new": _input_summary(new_path),
            "runs": {
                "athar": _repeat(lambda: _run_athar(old_path, new_path), args.repeats),
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
            entry["runs"]["ifcfast"] = _repeat(lambda: _run_ifcfast(old_path, new_path), args.repeats)
        entry["assessment"] = _assess_pair(entry, pair.expected)
        results["pairs"].append(entry)
        results["summary"].append(_summary_row(entry))
        _write_json(Path(args.out), results)

    _write_json(Path(args.out), results)
    print(f"wrote {args.out}", flush=True)
    return 0


def _repeat(fn: Callable[[], dict], repeats: int) -> dict:
    runs = [fn() for _ in range(repeats)]
    best_counts = _first_counts(runs)
    ok_seconds = [run["seconds"] for run in runs if run.get("status") == "ok" and isinstance(run.get("seconds"), (int, float))]
    peak_rss_values = [
        run["peak_rss_mb"] for run in runs if run.get("peak_rss_mb") is not None and isinstance(run.get("peak_rss_mb"), (int, float))
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
    if best_counts is not None:
        result["counts"] = best_counts
    return result


def _run_athar(old_path: str, new_path: str) -> dict:
    start = time.perf_counter()
    try:
        athar_engine._BUNDLE_CACHE.clear()
        report = athar_engine.diff_files(old_path, new_path)
        assert_report_invariants(report)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "seconds": round(time.perf_counter() - start, 3), "error": f"{type(exc).__name__}: {exc}"}
    stats = report["stats"]
    return {
        "status": "ok",
        "seconds": round(time.perf_counter() - start, 3),
        "peak_rss_mb": _current_peak_rss_mb(),
        "sections": {name: stats[name] for name in ("added", "deleted", "modified", "unchanged")},
        "counts": {name: stats[name] for name in ("added", "deleted", "modified", "unchanged")},
        "signatures": {"old": stats["old_signatures"], "new": stats["new_signatures"]},
        "matched_by_tier": stats["matcher_diagnostics"]["matched_by_tier"],
        "modified_change_scope": stats["modified_change_scope"],
        "guid_collisions": stats["guid_collisions"],
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
        "relationships": relationships or "default",
        "counts": _ifcdiff_counts(payload),
        "output": str(output_path),
        "stdout_preview": completed.get("stdout", "")[:2000],
        "stderr_preview": completed.get("stderr", "")[:2000],
    }


def _run_ifcfast(old_path: str, new_path: str) -> dict:
    if importlib.util.find_spec("ifcfast") is None:
        return {"status": "error", "reason": "install benchmark extra: python -m pip install '.[benchmark]'"}
    import ifcfast

    start = time.perf_counter()
    try:
        model = ifcfast.open(old_path, use_cache=False, write_cache=False)
        payload = model.diff(new_path, sample=5)
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "seconds": round(time.perf_counter() - start, 3), "error": f"{type(exc).__name__}: {exc}"}
    return {
        "status": "ok",
        "seconds": round(time.perf_counter() - start, 3),
        "peak_rss_mb": _current_peak_rss_mb(),
        "counts": _ifcfast_counts(payload),
        "parse_seconds_left": getattr(model, "parse_seconds", None),
        "output_preview": payload,
    }


def _run_subprocess(argv: list[str], timeout_s: int) -> dict:
    time_bin = shutil.which("time")
    time_output_path = None
    timed_argv = argv
    if time_bin and platform.system() == "Darwin":
        fd, name = tempfile.mkstemp(prefix="athar-benchmark-time-", suffix=".txt")
        Path(name).unlink(missing_ok=True)
        time_output_path = Path(name)
        timed_argv = [time_bin, "-l", "-o", str(time_output_path), *argv]
        try:
            import os

            os.close(fd)
        except OSError:
            pass
    try:
        completed = subprocess.run(
            timed_argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "seconds": timeout_s}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    stderr = completed.stderr
    peak_rss_mb = None
    if time_output_path is not None and time_output_path.exists():
        peak_rss_mb = _parse_time_peak_rss(time_output_path.read_text(encoding="utf-8"))
        time_output_path.unlink(missing_ok=True)
    return {
        "status": "completed",
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": stderr,
        "peak_rss_mb": peak_rss_mb,
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
    if run.get("status") != "ok":
        return "not_run"
    counts = _run_counts(run)
    if expected.counts and counts:
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
            matched = sum((run.get("matched_by_tier") or {}).values())
            if matched != expected.athar["matched_total"]:
                return "unexpected"
        if "matched_by_tier" in expected.athar:
            for tier, value in expected.athar["matched_by_tier"].items():
                if (run.get("matched_by_tier") or {}).get(tier) != value:
                    return "unexpected"
        if "guid_collisions" in expected.athar and run.get("guid_collisions") != expected.athar["guid_collisions"]:
            return "unexpected"
    return "expected"


def _summary_row(entry: dict) -> dict:
    row = {
        "name": entry.get("name"),
        "kind": entry.get("kind"),
        "expectation": entry.get("expectation"),
        "assessment": entry.get("assessment", {}),
    }
    if entry.get("error"):
        row["error"] = entry["error"]
        return row
    for name, run in entry.get("runs", {}).items():
        row[name] = {
            "status": run.get("status"),
            "counts": _run_counts(run),
            "seconds_median": run.get("seconds_median", run.get("seconds")),
            "peak_rss_mb_max": run.get("peak_rss_mb_max", run.get("peak_rss_mb")),
        }
    return row


def _run_counts(run: dict) -> dict | None:
    return run.get("counts") or run.get("sections")


def _first_counts(runs: list[dict]) -> dict | None:
    for run in runs:
        counts = _run_counts(run)
        if counts is not None:
            return counts
    return None


def _tool_inventory(*, include_ifcfast: bool) -> dict:
    tools = {
        "athar": {"status": "in_process", "version": athar_engine._package_version()},
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
    return {"counts": expected.counts, "athar": expected.athar, "note": expected.note}


def _current_peak_rss_mb() -> float | None:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = usage.ru_maxrss
    if not rss:
        return None
    # macOS reports bytes; Linux reports KiB.
    return round(rss / (1024 * 1024), 1) if platform.system() == "Darwin" else round(rss / 1024, 1)


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
