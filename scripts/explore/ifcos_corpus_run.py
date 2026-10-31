"""Robustness run over an external IFC corpus (e.g. IfcOpenShell/files).

Per file, in an isolated subprocess:

1. load: build the signature bundle (time, peak RSS, timeout).
2. self_diff: diff the file against a byte-identical copy; must be zero.
3. reserialize: ifcopenshell open + write (renumbers/reorders), diff against
   the original; must be zero.
4. edits (sampled files only): move one product, edit one pset value, delete
   one product, each checked against its ``athar_dev.ifc_mutations`` manifest.

Prints one JSON line per file; ``--out`` collects them. Corpus files are only
read, never copied out of ``--workdir``.

    python scripts/explore/ifcos_corpus_run.py CORPUS_DIR --workdir W --out results.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_DELTA_TOLERANCE_MM = 0.01


def _summarize_nonzero(report: dict) -> dict:
    def classes(items, side):
        return Counter((item.get(side) or item).get("entity_type", "?") for item in items)

    modified = report["modified"]
    aspects = Counter()
    for item in modified:
        for aspect, state in item.get("aspects", {}).items():
            if state == "changed":
                aspects[aspect] += 1
    return {
        "stats": {k: report["stats"][k] for k in ("added", "deleted", "modified", "unchanged")},
        "added_classes": dict(classes(report["added"], "new").most_common(8)),
        "deleted_classes": dict(classes(report["deleted"], "old").most_common(8)),
        "modified_classes": dict(classes(modified, "old").most_common(8)),
        "modified_aspects": dict(aspects),
        "change_scope": report["stats"]["modified_change_scope"],
        "match_reasons": dict(Counter(item["match"]["reason"] for item in modified)),
        "samples": [_sample(item) for item in modified[:3]],
    }


def _sample(item: dict) -> dict:
    keep = ("aspects", "change_scope", "data_delta", "property_deltas")
    return {"guid": item["old"].get("guid"), **{k: item[k] for k in keep if k in item}}


def _is_zero(report: dict) -> bool:
    s = report["stats"]
    return s["added"] == 0 and s["deleted"] == 0 and s["modified"] == 0


def _check_edit(report: dict, mutation) -> list[str]:
    problems: list[str] = []
    stats = report["stats"]
    op = mutation.operation
    if op == "delete_product":
        if stats["added"]:
            problems.append(f"added={stats['added']}")
        deleted = [item.get("guid") for item in report["deleted"]]
        if deleted != [mutation.victim_guid]:
            problems.append(f"deleted={deleted[:5]} (n={len(deleted)})")
        intrinsic = [i["old"].get("guid") for i in report["modified"] if i["change_scope"] != "transitive"]
        if intrinsic:
            problems.append(f"non-transitive modified={intrinsic[:5]} (n={len(intrinsic)})")
        return problems
    if stats["added"] or stats["deleted"]:
        problems.append(f"added={stats['added']} deleted={stats['deleted']}")
    victims = [i for i in report["modified"] if i["old"].get("guid") == mutation.victim_guid]
    if len(victims) != 1:
        problems.append(f"victim not modified (modified n={stats['modified']})")
        return problems
    victim = victims[0]
    if victim["match"]["reason"] != "guid":
        problems.append(f"victim match reason {victim['match']['reason']}")
    for aspect, state in mutation.expected_victim_aspects.items():
        if victim["aspects"].get(aspect) != state:
            problems.append(f"victim {aspect}={victim['aspects'].get(aspect)} expected {state}")
    if op == "move_product":
        others = [i["old"].get("guid") for i in report["modified"] if i is not victim]
        if others:
            problems.append(f"other modified={others[:5]} (n={len(others)})")
        delta = victim["aspects"].get("placement_delta_mm")
        if delta is None:
            problems.append("no placement_delta_mm")
        elif abs(math.hypot(*delta) - mutation.expected_delta_mm) > _DELTA_TOLERANCE_MM:
            problems.append(f"delta norm {math.hypot(*delta):.3f} expected {mutation.expected_delta_mm:.3f}")
        if victim["change_scope"] != "intrinsic":
            problems.append(f"victim scope {victim['change_scope']}")
    else:
        if "changed" not in victim.get("property_deltas", {}):
            problems.append("no property_deltas.changed")
        others = [
            i["old"].get("guid") for i in report["modified"] if i is not victim and i["change_scope"] != "transitive"
        ]
        if others:
            problems.append(f"non-transitive other modified={others[:5]} (n={len(others)})")
    return problems


def _run_one(path: Path, workdir: Path, edits: bool) -> dict:
    import ifcopenshell

    from athar.engine import _load_bundle, diff_files
    from athar_dev import ifc_mutations as mutations

    result: dict = {}
    t0 = time.perf_counter()
    bundle = _load_bundle(str(path))
    result["load_s"] = round(time.perf_counter() - t0, 3)
    result["signatures"] = len(bundle.signatures)
    result["schema"] = bundle.schema

    copy = workdir / "copy.ifc"
    shutil.copyfile(path, copy)
    t0 = time.perf_counter()
    report = diff_files(str(path), str(copy))
    result["self_diff_s"] = round(time.perf_counter() - t0, 3)
    result["self_diff"] = "zero" if _is_zero(report) else _summarize_nonzero(report)

    reserialized = workdir / "reserialized.ifc"
    try:
        ifcopenshell.open(str(path)).write(str(reserialized))
    except Exception as exc:
        result["reserialize"] = {"error": f"ifcopenshell write: {type(exc).__name__}: {exc}"[:400]}
    else:
        try:
            report = diff_files(str(path), str(reserialized))
            result["reserialize"] = "zero" if _is_zero(report) else _summarize_nonzero(report)
        except Exception as exc:
            result["reserialize"] = {"error": f"{type(exc).__name__}: {exc}"[:400]}

    if edits:
        baseline = workdir / "baseline.ifc"
        ifcopenshell.open(str(path)).write(str(baseline))
        result["edits"] = {}
        for mutator in (mutations.move_product, mutations.edit_pset_value, mutations.delete_product):
            name = mutator.__name__
            model = ifcopenshell.open(str(path))
            try:
                mutation = mutator(model)
            except (AssertionError, LookupError, StopIteration, ValueError) as exc:
                result["edits"][name] = {"status": "no_candidate", "reason": str(exc)[:200]}
                continue
            mutated = workdir / f"{name}.ifc"
            model.write(str(mutated))
            try:
                report = diff_files(str(baseline), str(mutated))
                problems = _check_edit(report, mutation)
            except Exception as exc:
                problems = [f"{type(exc).__name__}: {exc}"[:400]]
            result["edits"][name] = {
                "status": "ok" if not problems else "fail",
                "victim": mutation.victim_guid,
                "victim_type": mutation.victim_type,
                **({"problems": problems} if problems else {}),
            }
    return result


def _worker(argv: list[str]) -> None:
    path, workdir, edits = Path(argv[0]), Path(argv[1]), argv[2] == "1"
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        out = {"status": "ok", **_run_one(path, workdir, edits)}
    except Exception as exc:
        out = {"status": "error", "error": f"{type(exc).__name__}: {exc}"[:600], "trace": traceback.format_exc()[-1500:]}
    print("RESULT " + json.dumps(out))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _drive(path: Path, rel: str, workdir: Path, edits: bool, timeout_s: float) -> dict:
    sub = workdir / hashlib.sha1(rel.encode()).hexdigest()[:12]
    cmd = [sys.executable, __file__, "--worker", str(path), str(sub), "1" if edits else "0"]
    entry = {"file": rel, "sha256": _sha256(path), "size_bytes": path.stat().st_size, "edits_sampled": edits}
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
        entry.update(status="timeout", wall_s=timeout_s)
        shutil.rmtree(sub, ignore_errors=True)
        return entry
    entry["wall_s"] = round(time.perf_counter() - t0, 2)
    line = next((ln for ln in stdout.splitlines() if ln.startswith("RESULT ")), None)
    if line is None:
        entry.update(status="crash", returncode=proc.returncode, stderr=stderr[-1500:])
    else:
        entry.update(json.loads(line[len("RESULT ") :]))
    shutil.rmtree(sub, ignore_errors=True)
    return entry


def _peak_rss_mb(path: Path, timeout_s: float) -> float | None:
    code = "import sys; from athar.engine import _load_bundle; _load_bundle(sys.argv[1])"
    proc = subprocess.Popen([sys.executable, "-c", code, str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + timeout_s
    while True:
        pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
        if pid:
            # macOS reports ru_maxrss in bytes.
            return round(usage.ru_maxrss / 2**20, 1) if os.waitstatus_to_exitcode(status) == 0 else None
        if time.monotonic() > deadline:
            proc.kill()
            proc.wait()
            return None
        time.sleep(0.05)


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        _worker(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--edit-sample", type=int, default=30, help="files that also get synthetic edits")
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--only", default=None, help="substring filter on relative path")
    args = parser.parse_args()

    files = sorted(p for p in args.corpus.rglob("*.ifc") if "real-world-big" not in p.name)
    if args.only:
        files = [p for p in files if args.only in str(p.relative_to(args.corpus))]
    stride = max(1, len(files) // args.edit_sample) if args.edit_sample else 0
    sampled = {p for i, p in enumerate(files) if stride and i % stride == 0}
    args.workdir.mkdir(parents=True, exist_ok=True)

    def job(path: Path) -> dict:
        rel = str(path.relative_to(args.corpus))
        entry = _drive(path, rel, args.workdir, path in sampled, args.timeout_s * (4 if path in sampled else 2))
        if entry.get("status") == "ok":
            entry["peak_rss_mb"] = _peak_rss_mb(path, args.timeout_s)
        return entry

    with args.out.open("w") as out, ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for entry in pool.map(job, files):
            out.write(json.dumps(entry) + "\n")
            out.flush()
            brief = {k: entry.get(k) for k in ("file", "status", "load_s", "peak_rss_mb")}
            print(json.dumps(brief), flush=True)


if __name__ == "__main__":
    main()
