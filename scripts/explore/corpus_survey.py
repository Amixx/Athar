#!/usr/bin/env python3
"""Survey the IFC corpus: per-file signature stats and per-pair matcher behavior.

Walks the known corpus roots (repo `real-world-test/`, `data/`,
`tests/fixtures/`, `corpus/gni-bim-sample/`, plus the external
`../vscode-ifc/test-files`), builds a signature bundle per unique file, runs a
curated set of diff pairs (same-file, revision, discipline, unrelated,
guid-scramble), and writes one JSON document with sizes, schemas, signature
counts, class histograms, tier distributions, change scopes, and stage
timings. Feeds the dated corpus findings notes under `docs/corpus/` and
tier-evaluation decisions.

Matching has exactly two tiers (`guid`, `geometry_hash`); the survey reports
their distribution as-is. There are no fallback/capped/low-confidence fields —
the former spatial/topology fallback tiers were deleted after the 2026-06-10
survey showed they only manufactured cross-model matches.

Usage:
    python scripts/explore/corpus_survey.py [--out PATH] [--skip-large] [--include-bad]

Large files (>100MB) take minutes each; progress streams to stdout and the
output JSON is rewritten after every group so partial results survive a crash.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import athar.engine as engine  # noqa: E402
from athar.engine import diff_files  # noqa: E402

EXTERNAL_ROOT = Path(
    os.getenv("ATHAR_EXTERNAL_CORPUS_DIR", str(REPO_ROOT.parent / "vscode-ifc" / "test-files"))
)

GNI_ROOT = REPO_ROOT / "corpus" / "gni-bim-sample"


def _gni_2025(key: str, name: str) -> tuple[str, dict]:
    return key, {
        "path": GNI_ROOT / "2025_BIMfundamentals" / name,
        "tags": ["gni", "2025-fundamentals"],
    }


def _gni_2026(key: str, name: str, discipline: str) -> tuple[str, dict]:
    return key, {
        "path": GNI_ROOT / "2026_BIMprojects" / name,
        "tags": ["gni", "2026-projects", discipline],
    }


# key -> (path, expected_schema, aliases). Aliases are byte-identical (or
# near-identical) copies that exist elsewhere; only the primary path is parsed.
CORPUS: dict[str, dict] = {
    "tiny_no_products": {
        "path": REPO_ROOT / "tests/fixtures/tiny_no_products.ifc",
        "aliases": [EXTERNAL_ROOT / "000.000598MB__tiny.ifc"],
    },
    "cube_brep": {"path": EXTERNAL_ROOT / "000.010611MB__Ifc4_CubeAdvancedBrep.ifc"},
    "sample_house_roof": {"path": EXTERNAL_ROOT / "000.063019MB__Ifc4_SampleHouse_1_Roof.ifc"},
    "building_arch": {"path": EXTERNAL_ROOT / "000.225635MB__Building-Architecture.ifc"},
    "bl_v0": {
        "path": REPO_ROOT / "real-world-test/Building-Landscaping-v0-original.ifc",
        "aliases": [EXTERNAL_ROOT / "001.467652MB__Building-Landscaping.ifc"],
    },
    "bl_v1": {"path": REPO_ROOT / "real-world-test/Building-Landscaping-v1.ifc"},
    "bl_v2": {"path": REPO_ROOT / "real-world-test/Building-Landscaping-v2.ifc"},
    "bl_v3": {"path": REPO_ROOT / "real-world-test/Building-Landscaping-v3.ifc"},
    "duplex_arch": {
        "path": REPO_ROOT / "real-world-test/Duplex-Architecture.ifc",
        "aliases": [EXTERNAL_ROOT / "002.380763MB__Ifc2x3_Duplex_Architecture.ifc"],
    },
    "duplex_mech": {"path": EXTERNAL_ROOT / "008.781887MB__Ifc2x3_Duplex_Mechanical.ifc"},
    "revit_arc": {"path": EXTERNAL_ROOT / "013.646137MB__Ifc4_Revit_ARC.ifc"},
    "duplex_mep": {"path": EXTERNAL_ROOT / "017.871432MB__Ifc2x3_Duplex_MEP.ifc"},
    "revit_mep": {"path": EXTERNAL_ROOT / "029.165277MB__Ifc4_Revit_MEP.ifc"},
    "advanced_project": {
        "path": REPO_ROOT / "real-world-test/AdvancedProject.ifc",
        "aliases": [
            REPO_ROOT / "data/AdvancedProject.ifc",
            EXTERNAL_ROOT / "044.337371MB__AdvancedProject.ifc",
        ],
    },
    "adv_changed": {"path": REPO_ROOT / "real-world-test/adv proj changed.ifc"},
    "basic_house": {"path": REPO_ROOT / "data/BasicHouse.ifc"},
    "uni_house": {
        "path": REPO_ROOT / "real-world-test/uni-project-house-50mb.ifc",
        "aliases": [EXTERNAL_ROOT / "055.867908MB__uni-project-house-50mb.ifc"],
    },
    "real_world_big": {"path": EXTERNAL_ROOT / "168.047573MB__real-world-big.ifc", "large": True},
    "spanish": {
        "path": REPO_ROOT / "real-world-test/real-world-spanish-180mb.ifc",
        "aliases": [EXTERNAL_ROOT / "181.848887MB__real-world-spanish-180mb.ifc"],
        "large": True,
    },
    "spanish_bad": {
        "path": EXTERNAL_ROOT / "181.848887MB__real-world-spanish-180mb-bad.ifc",
        "large": True,
        "bad": True,  # one entity ref mangled into a dangling #29999...; parse may fail
    },
    # GNI-BIM sample (tracked LFS; attribution in corpus/gni-bim-sample/NOTICE.md).
    # 2025 BIM Fundamentals: independent student models, ascending size.
    **dict(
        _gni_2025(key, name)
        for key, name in (
            ("gni_190", "model_190.ifc"),
            ("gni_9", "model_9.ifc"),
            ("gni_50", "model_50.ifc"),
            ("gni_62", "model_62.ifc"),
            ("gni_149", "model_149.ifc"),
            ("gni_18", "model_18.ifc"),
            ("gni_115", "model_115.ifc"),
            ("gni_89", "model_89.ifc"),
            ("gni_169", "model_169.ifc"),
            ("gni_77", "model_77.ifc"),  # 22MB
            ("gni_68", "model_68.ifc"),  # 45MB
        )
    ),
    # 2026 BIM Projects: same-building architecture + structure pairs.
    **dict(
        _gni_2026(f"gni_p{n}_{short}", f"model_{n}_{disc}.ifc", discipline)
        for n in (0, 3, 5, 7, 8)
        for short, disc, discipline in (("arc", "arc", "architecture"), ("str", "structure", "structure"))
    ),
}

# Groups are processed in order; the bundle cache is cleared after each group
# so peak memory stays bounded. Pairs only reference keys within their group.
GROUPS: list[dict] = [
    {
        "name": "ifc4_small",
        "files": ["tiny_no_products", "cube_brep", "sample_house_roof", "building_arch", "bl_v0", "bl_v1", "bl_v2", "bl_v3"],
        "pairs": [
            ("revision", "bl_v0", "bl_v1"),
            ("revision", "bl_v1", "bl_v2"),
            ("revision", "bl_v2", "bl_v3"),
            ("revision", "bl_v0", "bl_v3"),
            ("discipline", "building_arch", "bl_v0"),
            ("unrelated", "cube_brep", "sample_house_roof"),
            ("unrelated", "sample_house_roof", "building_arch"),
        ],
        "scrambles": ["bl_v2", "sample_house_roof"],
    },
    {
        "name": "ifc4_revit",
        "files": ["revit_arc", "revit_mep"],
        "pairs": [("discipline", "revit_arc", "revit_mep")],
        "scrambles": ["revit_arc"],
    },
    {
        "name": "ifc2x3_duplex",
        "files": ["duplex_arch", "duplex_mech", "duplex_mep"],
        "pairs": [
            ("discipline", "duplex_arch", "duplex_mech"),
            ("discipline", "duplex_mech", "duplex_mep"),
            ("discipline", "duplex_arch", "duplex_mep"),
        ],
        "scrambles": ["duplex_arch"],
    },
    {
        "name": "ifc2x3_medium",
        "files": ["advanced_project", "adv_changed", "basic_house"],
        "pairs": [
            ("revision", "advanced_project", "adv_changed"),
            ("unrelated", "basic_house", "advanced_project"),
        ],
        "scrambles": [],
    },
    {
        "name": "ifc2x3_large",
        "files": ["uni_house", "spanish", "real_world_big", "spanish_bad"],
        "pairs": [
            ("unrelated", "spanish", "uni_house"),
            # corruption probe: identical to spanish except one mangled ref
            ("corruption", "spanish", "spanish_bad"),
        ],
        "scrambles": [],
    },
    {
        # 11 independent student models; no two are the same project, so the
        # only honest pair kinds are same-file (implicit) and unrelated.
        "name": "gni_2025_fundamentals",
        "files": [
            "gni_190", "gni_9", "gni_50", "gni_62", "gni_149", "gni_18",
            "gni_115", "gni_89", "gni_169", "gni_77", "gni_68",
        ],
        "pairs": [
            ("unrelated", "gni_190", "gni_9"),
            ("unrelated", "gni_50", "gni_62"),
        ],
        "scrambles": ["gni_190"],
    },
    {
        # Same-building architecture + structure exports: discipline pairs,
        # not revisions — expect mostly-disjoint content, no broad matching.
        "name": "gni_2026_pairs",
        "files": [f"gni_p{n}_{short}" for n in (0, 3, 5, 7, 8) for short in ("arc", "str")],
        "pairs": [("discipline", f"gni_p{n}_arc", f"gni_p{n}_str") for n in (0, 3, 5, 7, 8)],
        "scrambles": [],
    },
]

_SCRAMBLE_NAMESPACE = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def is_usable(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, "missing"
    with path.open("rb") as fh:
        if fh.read(64).startswith(b"version https://git-lfs"):
            return False, "lfs_pointer"
    return True, "ok"


def file_stats(key: str, path: Path, bundle, seconds: float) -> dict:
    sigs = bundle.signatures
    classes = Counter(sig.canonical_class for sig in sigs.values())
    return {
        "key": key,
        "path": str(path),
        "tags": CORPUS[key].get("tags", []),
        "size_mb": round(path.stat().st_size / 1e6, 3),
        "schema": bundle.schema,
        "build_seconds": round(seconds, 2),
        "signatures": len(sigs),
        "guid_collisions": engine._guid_collision_count(sigs),
        "missing_guid": sum(1 for s in sigs.values() if not s.guid),
        "missing_centroid": sum(1 for s in sigs.values() if s.centroid is None),
        "missing_topology": sum(1 for s in sigs.values() if not s.vh_topology),
        "diagnostics": {
            "dangling_refs": bundle.diagnostics.dangling_refs,
            "cycle_breaks": bundle.diagnostics.cycle_breaks,
            "warnings": len(bundle.diagnostics.warnings),
        },
        "edge_stats": dict(bundle.edge_stats),
        "top_classes": dict(classes.most_common(12)),
    }


def _reason_aggregates(items: list[dict]) -> dict:
    out: dict[str, dict] = {}
    for item in items:
        reason = item["match"]["reason"]
        agg = out.setdefault(
            reason,
            {"count": 0, "placement_changed": 0, "max_abs_placement_delta_mm": 0.0, "change_scope": Counter()},
        )
        agg["count"] += 1
        agg["change_scope"][item["change_scope"]] += 1
        aspects = item["aspects"]
        if aspects["placement"] == "changed":
            agg["placement_changed"] += 1
            delta = aspects.get("placement_delta_mm")
            if delta is not None:
                agg["max_abs_placement_delta_mm"] = max(
                    agg["max_abs_placement_delta_mm"], max(abs(v) for v in delta)
                )
    for agg in out.values():
        agg["change_scope"] = dict(agg["change_scope"])
    return out


def pair_stats(kind: str, old_key: str, new_key: str, report: dict, seconds: float) -> dict:
    stats = report["stats"]
    matched_items = report["modified"] + report["unchanged"]
    cross_class = sum(1 for item in matched_items if item["old"]["class"] != item["new"]["class"])
    sections_top = {
        section: dict(Counter(item["class"] for item in report[section]).most_common(8))
        for section in ("added", "deleted")
    }
    entry = {
        "kind": kind,
        "old": old_key,
        "new": new_key,
        "schemas": report["schemas"],  # equal by policy on any successful diff
        "diff_seconds": round(seconds, 2),
        "sections": {k: stats[k] for k in ("added", "deleted", "modified", "unchanged")},
        "signatures": {"old": stats["old_signatures"], "new": stats["new_signatures"]},
        "matched_by_tier": stats["matcher_diagnostics"]["matched_by_tier"],
        "duplicate_guids": stats["matcher_diagnostics"]["duplicate_guids"],
        "guid_collisions": stats["guid_collisions"],
        "modified_change_scope": stats["modified_change_scope"],
        "cross_class_matches": cross_class,
        "matched_reason_detail": _reason_aggregates(report["modified"] + report["unchanged"]),
        "modified_reason_detail": _reason_aggregates(report["modified"]),
        "added_deleted_top_classes": sections_top,
    }
    return entry


def make_scrambled_pair(src: Path, workdir: Path) -> tuple[Path, Path]:
    """(clean, scrambled) copies differing only in GlobalId values."""
    import ifcopenshell
    import ifcopenshell.guid

    workdir.mkdir(parents=True, exist_ok=True)
    clean = workdir / f"{src.stem}-clean.ifc"
    scrambled = workdir / f"{src.stem}-scrambled.ifc"
    model = ifcopenshell.open(str(src))
    model.write(str(clean))
    for entity in model:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if isinstance(guid, str) and guid.strip():
            entity.GlobalId = ifcopenshell.guid.compress(uuid.uuid5(_SCRAMBLE_NAMESPACE, guid).hex)
    model.write(str(scrambled))
    return clean, scrambled


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="/tmp/athar_corpus_survey.json")
    parser.add_argument("--skip-large", action="store_true", help="skip files marked large (>100MB)")
    parser.add_argument("--include-bad", action="store_true", help="also parse intentionally corrupted files")
    args = parser.parse_args()

    out_path = Path(args.out)
    results: dict = {"generated_at": time.strftime("%Y-%m-%d %H:%M:%S"), "files": {}, "pairs": [], "errors": []}

    def flush() -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = out_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(results, indent=2, sort_keys=True))
        tmp.replace(out_path)

    total_start = time.perf_counter()
    for group in GROUPS:
        log(f"=== group {group['name']} ===")
        for key in group["files"]:
            spec = CORPUS[key]
            path = spec["path"]
            if spec.get("bad") and not args.include_bad:
                results["files"][key] = {"key": key, "path": str(path), "skipped": "bad_file"}
                continue
            if spec.get("large") and args.skip_large:
                results["files"][key] = {"key": key, "path": str(path), "skipped": "large"}
                continue
            usable, why = is_usable(path)
            if not usable:
                results["files"][key] = {"key": key, "path": str(path), "skipped": why}
                log(f"skip {key}: {why}")
                continue
            log(f"build {key} ({path.stat().st_size / 1e6:.1f} MB) ...")
            start = time.perf_counter()
            try:
                bundle = engine._load_bundle(str(path))
            except Exception as exc:  # noqa: BLE001 - survey records failures
                results["errors"].append({"stage": "build", "key": key, "error": f"{type(exc).__name__}: {exc}"})
                log(f"  FAILED: {type(exc).__name__}: {exc}")
                flush()
                continue
            seconds = time.perf_counter() - start
            results["files"][key] = file_stats(key, path, bundle, seconds)
            log(f"  {bundle.schema}, {len(bundle.signatures)} signatures, {seconds:.1f}s")

            start = time.perf_counter()
            report = diff_files(str(path), str(path))
            results["pairs"].append(pair_stats("same_file", key, key, report, time.perf_counter() - start))
            sections = results["pairs"][-1]["sections"]
            if sections["added"] or sections["deleted"] or sections["modified"]:
                log(f"  !! same-file diff NOT clean: {sections}")
            flush()

        for kind, old_key, new_key in group["pairs"]:
            old_info = results["files"].get(old_key, {})
            new_info = results["files"].get(new_key, {})
            if "signatures" not in old_info or "signatures" not in new_info:
                continue
            log(f"pair {kind}: {old_key} -> {new_key} ...")
            start = time.perf_counter()
            try:
                report = diff_files(str(CORPUS[old_key]["path"]), str(CORPUS[new_key]["path"]))
            except Exception as exc:  # noqa: BLE001
                results["errors"].append(
                    {"stage": "pair", "pair": f"{old_key}->{new_key}", "error": f"{type(exc).__name__}: {exc}"}
                )
                log(f"  FAILED: {type(exc).__name__}: {exc}")
                flush()
                continue
            entry = pair_stats(kind, old_key, new_key, report, time.perf_counter() - start)
            results["pairs"].append(entry)
            log(f"  sections={entry['sections']} tiers={entry['matched_by_tier']} {entry['diff_seconds']}s")
            flush()

        for key in group.get("scrambles", []):
            if "signatures" not in results["files"].get(key, {}):
                continue
            log(f"scramble pair: {key} ...")
            try:
                clean, scrambled = make_scrambled_pair(CORPUS[key]["path"], Path("/tmp/athar_corpus_scramble"))
                start = time.perf_counter()
                report = diff_files(str(clean), str(scrambled))
                entry = pair_stats("guid_scramble", key, f"{key}(scrambled)", report, time.perf_counter() - start)
                results["pairs"].append(entry)
                log(f"  sections={entry['sections']} tiers={entry['matched_by_tier']}")
            except Exception as exc:  # noqa: BLE001
                results["errors"].append({"stage": "scramble", "key": key, "error": f"{type(exc).__name__}: {exc}"})
                log(f"  FAILED: {type(exc).__name__}: {exc}")
            flush()

        engine._BUNDLE_CACHE.clear()

    results["total_seconds"] = round(time.perf_counter() - total_start, 1)
    flush()
    log(f"done in {results['total_seconds']}s -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
