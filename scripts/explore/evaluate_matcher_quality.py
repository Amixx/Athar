"""Evaluate matcher stage quality on deterministic synthetic scenarios."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Callable

from athar.determinism import canonical_json
from athar.matcher_graph import propagate_matches_by_typed_path, secondary_match_unresolved
from athar.root_remap import plan_root_remap


@dataclass(frozen=True)
class Scenario:
    name: str
    stage: str
    expected_pairs: dict[Any, Any]
    run: Callable[[], tuple[dict[Any, Any], dict[str, Any]]]


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def _scenarios() -> list[Scenario]:
    return [
        Scenario(
            name="root_remap_signature_unique",
            stage="root_remap",
            expected_pairs={"OLD_A": "NEW_A", "OLD_B": "NEW_B"},
            run=_run_root_remap_signature_unique,
        ),
        Scenario(
            name="typed_path_unique_chain",
            stage="typed_path_propagation",
            expected_pairs={2: 102, 3: 103},
            run=_run_typed_path_unique_chain,
        ),
        Scenario(
            name="secondary_swapped_pairs",
            stage="secondary_match",
            expected_pairs={20: 221, 21: 220},
            run=_run_secondary_swapped_pairs,
        ),
        Scenario(
            name="secondary_ambiguous_rejected",
            stage="secondary_match",
            expected_pairs={},
            run=_run_secondary_ambiguous_rejected,
        ),
    ]


def _run_root_remap_signature_unique() -> tuple[dict[str, str], dict[str, Any]]:
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "OLD_A",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "OLD_A"},
                "Name": {"kind": "string", "value": "Wall A"},
            },
            "refs": [],
        },
        2: {
            "entity_type": "IfcWall",
            "global_id": "OLD_B",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "OLD_B"},
                "Name": {"kind": "string", "value": "Wall B"},
            },
            "refs": [],
        },
    })
    new_graph = _graph({
        3: {
            "entity_type": "IfcWall",
            "global_id": "NEW_A",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "NEW_A"},
                "Name": {"kind": "string", "value": "Wall A"},
            },
            "refs": [],
        },
        4: {
            "entity_type": "IfcWall",
            "global_id": "NEW_B",
            "attributes": {
                "GlobalId": {"kind": "string", "value": "NEW_B"},
                "Name": {"kind": "string", "value": "Wall B"},
            },
            "refs": [],
        },
    })
    out = plan_root_remap(old_graph, new_graph)
    return out["old_to_new"], {"ambiguous": out["ambiguous"], "method": out["method"]}


def _run_typed_path_unique_chain() -> tuple[dict[int, int], dict[str, Any]]:
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_A",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 2, "target_type": "IfcLocalPlacement"}],
        },
        2: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {},
            "refs": [{"path": "/RelativePlacement", "target": 3, "target_type": "IfcAxis2Placement3D"}],
        },
        3: {"entity_type": "IfcAxis2Placement3D", "attributes": {}, "refs": []},
    })
    new_graph = _graph({
        101: {
            "entity_type": "IfcWall",
            "global_id": "ROOT_B",
            "attributes": {},
            "refs": [{"path": "/ObjectPlacement", "target": 102, "target_type": "IfcLocalPlacement"}],
        },
        102: {
            "entity_type": "IfcLocalPlacement",
            "attributes": {},
            "refs": [{"path": "/RelativePlacement", "target": 103, "target_type": "IfcAxis2Placement3D"}],
        },
        103: {"entity_type": "IfcAxis2Placement3D", "attributes": {}, "refs": []},
    })
    out = propagate_matches_by_typed_path(old_graph, new_graph, {1: 101})
    return out["old_to_new"], {"ambiguous": out["ambiguous"]}


def _run_secondary_swapped_pairs() -> tuple[dict[int, int], dict[str, Any]]:
    old_graph = _graph({
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]},
                "Tag": {"kind": "string", "value": "A"},
            },
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "9"}]},
                "Tag": {"kind": "string", "value": "B"},
            },
            "refs": [],
        },
    })
    new_graph = _graph({
        220: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "9"}]},
                "Tag": {"kind": "string", "value": "B"},
            },
            "refs": [],
        },
        221: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]},
                "Tag": {"kind": "string", "value": "A"},
            },
            "refs": [],
        },
    })
    out = secondary_match_unresolved(old_graph, new_graph)
    return out["old_to_new"], {"ambiguous": out["ambiguous"]}


def _run_secondary_ambiguous_rejected() -> tuple[dict[int, int], dict[str, Any]]:
    old_graph = _graph({
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "1"}]}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "2"}]}},
            "refs": [],
        },
    })
    new_graph = _graph({
        220: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "5"}]}},
            "refs": [],
        },
        221: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {"Coordinates": {"kind": "list", "items": [{"kind": "real", "value": "6"}]}},
            "refs": [],
        },
    })
    out = secondary_match_unresolved(old_graph, new_graph)
    return out["old_to_new"], {"ambiguous": out["ambiguous"]}


def _metrics(expected: dict[Any, Any], predicted: dict[Any, Any]) -> dict[str, Any]:
    expected_items = set(expected.items())
    predicted_items = set(predicted.items())
    tp = len(expected_items & predicted_items)
    fp = len(predicted_items - expected_items)
    fn = len(expected_items - predicted_items)
    precision = 1.0 if tp + fp == 0 else tp / (tp + fp)
    recall = 1.0 if tp + fn == 0 else tp / (tp + fn)
    f1 = 0.0 if precision + recall == 0 else (2 * precision * recall) / (precision + recall)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "exact_match": predicted == expected,
    }


def _aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    totals = {"tp": 0, "fp": 0, "fn": 0, "scenarios": len(rows), "exact_match_scenarios": 0}
    for row in rows:
        m = row["metrics"]
        totals["tp"] += m["tp"]
        totals["fp"] += m["fp"]
        totals["fn"] += m["fn"]
        totals["exact_match_scenarios"] += 1 if m["exact_match"] else 0
    tp, fp, fn = totals["tp"], totals["fp"], totals["fn"]
    precision = 1.0 if tp + fp == 0 else tp / (tp + fp)
    recall = 1.0 if tp + fn == 0 else tp / (tp + fn)
    f1 = 0.0 if precision + recall == 0 else (2 * precision * recall) / (precision + recall)
    totals["precision"] = round(precision, 6)
    totals["recall"] = round(recall, 6)
    totals["f1"] = round(f1, 6)
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate deterministic matcher quality scenarios.")
    parser.add_argument("--out", default=None, help="Optional output JSON path.")
    args = parser.parse_args()

    scenarios = _scenarios()
    rows: list[dict[str, Any]] = []
    by_stage: dict[str, list[dict[str, Any]]] = {}
    suite_started = time.perf_counter()

    for index, scenario in enumerate(scenarios, start=1):
        print(
            f"[matcher-quality] scenario {index}/{len(scenarios)} start {scenario.name} ({scenario.stage})",
            file=sys.stderr,
            flush=True,
        )
        started = time.perf_counter()
        predicted, aux = scenario.run()
        metrics = _metrics(scenario.expected_pairs, predicted)
        row = {
            "name": scenario.name,
            "stage": scenario.stage,
            "expected_pairs": scenario.expected_pairs,
            "predicted_pairs": predicted,
            "metrics": metrics,
            "aux": aux,
        }
        rows.append(row)
        by_stage.setdefault(scenario.stage, []).append(row)
        print(
            f"[matcher-quality] scenario {index}/{len(scenarios)} done {scenario.name}: "
            f"exact={metrics['exact_match']} f1={metrics['f1']} elapsed={round(time.perf_counter() - started, 3)}s",
            file=sys.stderr,
            flush=True,
        )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario_count": len(rows),
        "overall": _aggregate(rows),
        "stages": {stage: _aggregate(stage_rows) for stage, stage_rows in sorted(by_stage.items())},
        "scenarios": rows,
    }

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote matcher quality report to {out_path}")
    else:
        print(payload, end="")
    print(
        f"[matcher-quality] completed scenarios={len(scenarios)} elapsed={round(time.perf_counter() - suite_started, 3)}s",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    main()
