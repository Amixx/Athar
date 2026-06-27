#!/usr/bin/env python3
"""Reproducible memory profile of the native signature pipeline.

Why this exists
---------------
The competitor benchmark flagged peak RSS as Athar's one consistent weakness
(~1.8-2.4x ifcdiff). The same-file large pairs in that harness alias one bundle
(``engine.diff_files`` sets ``new_bundle = old_bundle`` for identical paths), so
they undercount a real two-file diff and cannot tell us *which phase* owns the
peak. This script answers both:

* ``single`` -- builds one bundle and reports per-phase RSS (ifcopenshell open +
  property traversal, the native Rust arena, Python signature materialization).
  This separates the transient build peak ``T`` from the resident bundle ``B``.
* ``double`` -- builds two bundles of the SAME file and holds both alive. If the
  Rust arena is released between builds, peak stays ~``T + B`` rather than
  ``2*T``; that confirms a real two-file diff ceiling is ``T + B``, not ``2*T``.
* ``diff``   -- full ``engine.diff_files`` on two paths (real end-to-end,
  including matcher + report), for two distinct files.

It is intentionally dependency-free: RSS is sampled by shelling out to ``ps``
(KiB on macOS and Linux) from a background thread, so there is nothing to
``pip install``. Phase peaks are computed from timestamped samples after the run.

Usage
-----
    python scripts/explore/profile_memory.py single real-world-test/real-world-spanish-180mb.ifc
    python scripts/explore/profile_memory.py double real-world-test/real-world-spanish-180mb.ifc --out mem.json
    python scripts/explore/profile_memory.py diff a.ifc b.ifc

Run the same command later to compare; record the git commit (captured in the
output) so numbers are attributable to a build.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import resource
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TypeVar

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

T = TypeVar("T")


# --------------------------------------------------------------------------- #
# RSS sampling
# --------------------------------------------------------------------------- #
def _read_rss_kib(pid: int) -> int | None:
    """Current resident set size in KiB via ``ps`` (macOS and Linux report KiB)."""
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None
    return int(out) if out else None


class RssSampler:
    """Background thread that records (timestamp, rss_mb) samples until stopped."""

    def __init__(self, interval_s: float = 0.05) -> None:
        self.interval_s = interval_s
        self.pid = _own_pid()
        self.samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            kib = _read_rss_kib(self.pid)
            if kib is not None:
                self.samples.append((time.perf_counter(), kib / 1024.0))
            self._stop.wait(self.interval_s)

    def __enter__(self) -> "RssSampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)

    def current_mb(self) -> float | None:
        kib = _read_rss_kib(self.pid)
        return round(kib / 1024.0, 1) if kib is not None else None

    def peak_in(self, start: float, end: float) -> float | None:
        window = [mb for (t, mb) in self.samples if start <= t <= end]
        return round(max(window), 1) if window else None

    def peak_overall(self) -> float | None:
        return round(max(mb for _, mb in self.samples), 1) if self.samples else None

    def gb_seconds(self) -> float | None:
        """Integral of resident memory (GB) over time (s) — the area under the
        RSS curve. Fuses "how much memory" and "for how long" into one number;
        this is the GB-second unit cloud platforms bill. Lower is leaner."""
        if len(self.samples) < 2:
            return None
        total = 0.0
        for (t0, m0), (t1, m1) in zip(self.samples, self.samples[1:]):
            total += ((m0 + m1) / 2.0 / 1024.0) * (t1 - t0)  # MB->GB, * dt
        return round(total, 2)


def _own_pid() -> int:
    import os

    return os.getpid()


# --------------------------------------------------------------------------- #
# Phase timing/memory record
# --------------------------------------------------------------------------- #
@dataclass
class Phase:
    name: str
    start: float
    end: float
    rss_enter_mb: float | None
    rss_exit_mb: float | None
    peak_mb: float | None

    @property
    def seconds(self) -> float:
        return self.end - self.start

    def as_dict(self) -> dict[str, Any]:
        return {
            "phase": self.name,
            "seconds": round(self.seconds, 3),
            "rss_enter_mb": self.rss_enter_mb,
            "rss_exit_mb": self.rss_exit_mb,
            "peak_mb": self.peak_mb,
            # Net resident growth attributable to this phase.
            "rss_delta_mb": (
                round(self.rss_exit_mb - self.rss_enter_mb, 1)
                if self.rss_enter_mb is not None and self.rss_exit_mb is not None
                else None
            ),
        }


@dataclass
class Recorder:
    sampler: RssSampler
    phases: list[Phase] = field(default_factory=list)

    def run(self, name: str, func: Callable[[], T]) -> T:
        enter = self.sampler.current_mb()
        start = time.perf_counter()
        try:
            return func()
        finally:
            end = time.perf_counter()
            exit_mb = self.sampler.current_mb()
            # Fold the boundary readings into the window peak: the sampler runs
            # at a fixed interval and can miss an allocation spike that lands
            # right at a phase edge (observed: resident climbing through `exit`).
            window = self.sampler.peak_in(start, end)
            peak = max(v for v in (window, enter, exit_mb) if v is not None) if any(
                v is not None for v in (window, enter, exit_mb)
            ) else None
            self.phases.append(Phase(name, start, end, enter, exit_mb, peak))


# --------------------------------------------------------------------------- #
# Pipeline phases (mirrors athar.bottom.signatures._build_signature_bundle_native)
# --------------------------------------------------------------------------- #
def _build_bundle_phased(path: str, rec: Recorder):
    """Build one SignatureBundle, recording each native-pipeline phase.

    Reuses the real helpers from ``athar.bottom`` -- this only owns the phase
    boundaries so per-phase RSS is visible, the work itself is the production
    code path.
    """
    import ifcopenshell

    from athar.bottom.constants import CANON_VERSION
    from athar.bottom.native_schema import schema_descriptors_json
    from athar.bottom.parser import _assert_supported_schema, _extract_unit_context
    from athar.bottom.properties import extract_properties
    from athar.bottom.signatures import native
    from athar.bottom.types import ParseDiagnostics, SignatureBundle, SignatureVector

    native_mod = native()
    if native_mod is None:
        raise SystemExit("athar_native is not built. Run `make native-build` first.")

    holder: dict[str, Any] = {}

    def _open() -> None:
        ifc = ifcopenshell.open(path)
        schema = str(ifc.schema or "")
        _assert_supported_schema(schema)
        holder["ifc"] = ifc
        holder["schema"] = schema
        holder["unit_factors"] = _extract_unit_context(ifc).get("unit_factors", {})

    rec.run("ifc_open", _open)
    rec.run("extract_properties", lambda: holder.update(props=extract_properties(holder["ifc"])))

    def _release() -> None:
        del holder["ifc"]
        gc.collect()

    rec.run("ifc_release", _release)

    schema_json = schema_descriptors_json(holder["schema"])

    def _native() -> Any:
        return native_mod.build_signature_bundle(path, schema_json, holder["unit_factors"])

    sigs, edge_stats_map, diag = rec.run("native_build", _native)
    dangling, cycle_breaks, warnings = diag

    def _materialize() -> dict[int, Any]:
        signatures: dict[int, SignatureVector] = {}
        for (
            step_id, guid, name, entity_type, canonical_class,
            vh_geometry, vh_data, vh_topology, placement, centroid, aabb, data_facts,
        ) in sigs:
            signatures[step_id] = SignatureVector(
                step_id=step_id, guid=guid, name=name, entity_type=entity_type,
                canonical_class=canonical_class, vh_geometry=vh_geometry, vh_data=vh_data,
                vh_topology=vh_topology,
                placement=tuple(placement) if placement is not None else None,
                centroid=tuple(centroid) if centroid is not None else None,
                aabb=tuple(aabb) if aabb is not None else None,
                canon_version=CANON_VERSION,
                data_facts=tuple((p, v) for p, v in data_facts),
            )
        return signatures

    signatures = rec.run("materialize_signatures", _materialize)

    # Free the raw native tuple list once Python objects exist, then measure the
    # resident bundle alone (this is "B" -- what a held bundle actually costs).
    del sigs
    gc.collect()

    return SignatureBundle(
        filepath=path,
        schema=holder["schema"],
        canon_version=CANON_VERSION,
        signatures=signatures,
        property_index=holder["props"],
        diagnostics=ParseDiagnostics(
            dangling_refs=dangling, cycle_breaks=cycle_breaks, warnings=list(warnings)
        ),
        edge_stats=edge_stats_map,
    )


# --------------------------------------------------------------------------- #
# Modes
# --------------------------------------------------------------------------- #
def mode_single(paths: list[str], rec: Recorder) -> dict[str, Any]:
    bundle = _build_bundle_phased(paths[0], rec)
    resident = rec.sampler.current_mb()
    return {
        "schema": bundle.schema,
        "signatures": len(bundle.signatures),
        "resident_after_build_mb": resident,
    }


def mode_double(paths: list[str], rec: Recorder) -> dict[str, Any]:
    """Hold two bundles of the same file -> is the transient arena freed?"""
    b1 = rec.run("build_first", lambda: _build_bundle_phased(paths[0], _Sub(rec)))
    resident_one = rec.sampler.current_mb()
    b2 = rec.run("build_second", lambda: _build_bundle_phased(paths[0], _Sub(rec)))
    resident_two = rec.sampler.current_mb()
    # Keep both alive past measurement.
    assert b1.signatures and b2.signatures
    return {
        "schema": b1.schema,
        "signatures": len(b1.signatures),
        "resident_one_bundle_mb": resident_one,
        "resident_two_bundles_mb": resident_two,
        "second_bundle_marginal_mb": (
            round(resident_two - resident_one, 1)
            if resident_one is not None and resident_two is not None
            else None
        ),
    }


def mode_diff(paths: list[str], rec: Recorder) -> dict[str, Any]:
    import athar.engine as engine

    old_path, new_path = paths[0], paths[1]
    engine._BUNDLE_CACHE.clear()
    old_bundle = rec.run("build_old", lambda: engine._load_bundle(old_path))
    new_bundle = rec.run("build_new", lambda: engine._load_bundle(new_path))
    report = rec.run(
        "match_and_report",
        lambda: engine.diff_bundles(old_bundle, new_bundle),
    )
    stats = report["stats"]
    return {
        "schema": old_bundle.schema,
        "signatures": {"old": stats["old_signatures"], "new": stats["new_signatures"]},
        "sections": {k: stats[k] for k in ("added", "deleted", "modified", "unchanged")},
        "resident_after_diff_mb": rec.sampler.current_mb(),
    }


class _Sub(Recorder):
    """Recorder that appends phase records into a parent, prefixing names."""

    def __init__(self, parent: Recorder, prefix: str = "") -> None:
        super().__init__(parent.sampler)
        self._parent = parent

    def run(self, name: str, func: Callable[[], T]) -> T:
        result = super().run(name, func)
        self._parent.phases.extend(self.phases)
        self.phases.clear()
        return result


MODES: dict[str, Callable[[list[str], Recorder], dict[str, Any]]] = {
    "single": mode_single,
    "double": mode_double,
    "diff": mode_diff,
}


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.mode == "diff" and len(args.files) < 2:
        print("diff mode needs two files", file=sys.stderr)
        return 2

    baseline_mb = round((resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / _maxrss_div()), 1)
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    t0 = time.perf_counter()

    with RssSampler(interval_s=args.interval) as sampler:
        rec = Recorder(sampler)
        summary = MODES[args.mode](args.files, rec)
        sampler_peak = sampler.peak_overall()
        gb_s = sampler.gb_seconds()

    true_peak_mb = round((resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / _maxrss_div()), 1)

    result = {
        "mode": args.mode,
        "started_at": started_at,
        "total_seconds": round(time.perf_counter() - t0, 3),
        "files": [_file_meta(Path(p)) for p in args.files],
        "summary": summary,
        "peak_rss_mb": {
            "sampler": sampler_peak,
            "getrusage_true_peak": true_peak_mb,
            "baseline_at_start": baseline_mb,
        },
        "gb_seconds": gb_s,
        "phases": [p.as_dict() for p in rec.phases],
        "environment": _environment(args.interval),
    }

    payload = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(payload + "\n", encoding="utf-8")
    _print_table(result)
    if args.json:
        print(payload)
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=sorted(MODES))
    parser.add_argument("files", nargs="+", help="IFC file(s); diff mode needs two")
    parser.add_argument("--out", help="write full JSON result here")
    parser.add_argument("--json", action="store_true", help="also print full JSON to stdout")
    parser.add_argument("--interval", type=float, default=0.05, help="RSS sample interval seconds (default 0.05)")
    return parser.parse_args(argv)


def _maxrss_div() -> float:
    # macOS ru_maxrss is bytes; Linux is KiB. Normalize to MiB.
    return (1024.0 * 1024.0) if platform.system() == "Darwin" else 1024.0


def _file_meta(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError:
        size = None
    return {"path": str(path), "size_mb": round(size / 1_000_000, 1) if size else None}


def _environment(interval: float) -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "sample_interval_s": interval,
        "git_commit": _git_commit(),
    }


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None


def _print_table(result: dict[str, Any]) -> None:
    peak = result["peak_rss_mb"]
    print(f"\n=== memory profile: {result['mode']} ===")
    for f in result["files"]:
        print(f"  file: {f['path']}  ({f['size_mb']} MB)")
    print(f"  total: {result['total_seconds']}s   "
          f"peak RSS: {peak['getrusage_true_peak']} MB (true) / {peak['sampler']} MB (sampler)")
    if result.get("gb_seconds") is not None:
        print(f"  memory-time: {result['gb_seconds']} GB·s (area under the RSS curve — lower is leaner)")
    print(f"\n  {'phase':<26}{'sec':>8}{'enter':>10}{'exit':>10}{'peak':>10}{'Δrss':>10}")
    print(f"  {'-'*26}{'-'*8}{'-'*10}{'-'*10}{'-'*10}{'-'*10}")
    for p in result["phases"]:
        print(f"  {p['phase']:<26}{p['seconds']:>8}{_fmt(p['rss_enter_mb']):>10}"
              f"{_fmt(p['rss_exit_mb']):>10}{_fmt(p['peak_mb']):>10}{_fmt(p['rss_delta_mb']):>10}")
    print("\n  summary:")
    for k, v in result["summary"].items():
        print(f"    {k}: {v}")
    print()


def _fmt(v: float | None) -> str:
    return "-" if v is None else f"{v}"


if __name__ == "__main__":
    raise SystemExit(main())
