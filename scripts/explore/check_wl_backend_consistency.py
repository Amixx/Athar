"""Check WL backend partition consistency against sha256 baseline."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys
import time
from typing import Any

from athar.graph.determinism import canonical_json
from athar.graph.graph_parser import parse_graph
from athar.diff.wl_refinement import wl_refine_with_scc_fallback

BACKENDS = ("auto", "sha256", "xxh3_64", "blake3", "blake2b_64")


def _partition_summary(labels: dict[int, str]) -> dict[str, Any]:
    buckets: dict[str, list[int]] = {}
    for step_id, label in labels.items():
        buckets.setdefault(label, []).append(step_id)
    groups = [sorted(group) for group in buckets.values()]
    groups.sort(key=lambda group: (len(group), group))
    digest = hashlib.sha256()
    digest.update(b"athar.partition.v1|")
    largest_group = 0
    for group in groups:
        group_size = len(group)
        if group_size > largest_group:
            largest_group = group_size
        digest.update(str(group_size).encode("ascii"))
        digest.update(b":")
        for step_id in group:
            digest.update(str(step_id).encode("ascii"))
            digest.update(b",")
        digest.update(b";")
    return {
        "group_count": len(groups),
        "largest_group_size": largest_group,
        "sha256": digest.hexdigest(),
    }


def _run_backend(graph: dict, backend: str) -> dict[str, Any]:
    colors, class_ids = wl_refine_with_scc_fallback(graph, round_hash=backend)
    return {
        "available": True,
        "color_partition": _partition_summary(colors),
        "class_partition": _partition_summary(class_ids),
        "entity_count": len(colors),
        "class_member_count": len(class_ids),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check WL backend partition consistency.")
    parser.add_argument(
        "--graph",
        action="append",
        default=[],
        help="Input IFC path (repeatable). Defaults to data/BasicHouse.ifc and data/AdvancedProject.ifc.",
    )
    parser.add_argument("--out", default=None, help="Optional output JSON path.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    graph_paths = [Path(p) for p in args.graph] if args.graph else [
        repo_root / "data" / "BasicHouse.ifc",
        repo_root / "data" / "AdvancedProject.ifc",
    ]

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "graphs": [],
    }

    total_graphs = len(graph_paths)
    for graph_idx, graph_path in enumerate(graph_paths, start=1):
        print(
            f"[wl-consistency] graph {graph_idx}/{total_graphs}: parsing {graph_path}",
            file=sys.stderr,
            flush=True,
        )
        parse_started = time.perf_counter()
        graph = parse_graph(str(graph_path), profile="semantic_stable")
        parse_elapsed_ms = (time.perf_counter() - parse_started) * 1000.0
        print(
            f"[wl-consistency] graph {graph_idx}/{total_graphs}: parsed in {parse_elapsed_ms:.1f} ms",
            file=sys.stderr,
            flush=True,
        )
        graph_report: dict[str, Any] = {
            "graph_path": str(graph_path),
            "backends": {},
            "consistency": {},
        }

        baseline_partition: dict[str, Any] | None = None
        baseline_class_partition: dict[str, Any] | None = None

        for backend_idx, backend in enumerate(BACKENDS, start=1):
            print(
                f"[wl-consistency] graph={graph_path.name} backend {backend_idx}/{len(BACKENDS)}: {backend}",
                file=sys.stderr,
                flush=True,
            )
            backend_started = time.perf_counter()
            try:
                backend_result = _run_backend(graph, backend)
            except ValueError as exc:
                backend_result = {"available": False, "error": str(exc)}
            elapsed_ms = (time.perf_counter() - backend_started) * 1000.0
            print(
                f"[wl-consistency] graph={graph_path.name} backend={backend} done in {elapsed_ms:.1f} ms (available={backend_result.get('available')})",
                file=sys.stderr,
                flush=True,
            )
            graph_report["backends"][backend] = backend_result

            if backend == "sha256" and backend_result.get("available"):
                baseline_partition = backend_result["color_partition"]
                baseline_class_partition = backend_result["class_partition"]

        for backend in BACKENDS:
            backend_result = graph_report["backends"][backend]
            if not backend_result.get("available"):
                graph_report["consistency"][backend] = {
                    "available": False,
                    "matches_sha256": None,
                    "class_partition_matches_sha256": None,
                }
                continue

            matches_colors = (
                baseline_partition is not None
                and backend_result["color_partition"]["sha256"] == baseline_partition["sha256"]
            )
            matches_classes = (
                baseline_class_partition is not None
                and backend_result["class_partition"]["sha256"] == baseline_class_partition["sha256"]
            )
            graph_report["consistency"][backend] = {
                "available": True,
                "matches_sha256": matches_colors,
                "class_partition_matches_sha256": matches_classes,
            }

        report["graphs"].append(graph_report)

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote WL backend consistency report to {out_path}")
    else:
        print(payload, end="")


if __name__ == "__main__":
    main()
