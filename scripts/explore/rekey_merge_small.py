"""Three-way ifcmerge proof on a small model whose branches were re-exported with new STEP ids.

BASE is the model serialized by IfcOpenShell. LOCAL edits a property value on one wall and
moves a slab (a new location point, since the old one is shared); REMOTE renames another
wall; CONFLICT edits the same property value as LOCAL.
Each branch is then renumbered by a seeded permutation of all entity ids and written in
the new id order, as an exporter that does not keep ids would. ifcmerge runs on the raw
branches and on the branches rekeyed onto BASE.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import random
import re
import subprocess
import sys
import time

import ifcopenshell

PROPERTY = ("37cU2nGL100xirW00wMfms", "Constraints", "Unconnected Height")
MOVED = "37cU2nGL100xirW00wMfbq"
RENAMED = "37cU2nGL100xirW00wMfmn"
MOVE_X = 500.0
LOCAL_VALUE = 7500.0
REMOTE_VALUE = 7000.0
REMOTE_NAME = "Renamed by remote"


def prop(model):
    guid, pset_name, name = PROPERTY
    for rel in model.by_guid(guid).IsDefinedBy:
        if rel.is_a("IfcRelDefinesByProperties") and rel.RelatingPropertyDefinition.Name == pset_name:
            found = next(p for p in rel.RelatingPropertyDefinition.HasProperties if p.Name == name)
            if len(model.get_inverse(found)) != 1:
                raise ValueError(f"{PROPERTY} is shared, editing it would touch other elements")
            return found
    raise LookupError(PROPERTY)


def location(model, guid):
    return model.by_guid(guid).ObjectPlacement.RelativePlacement.Location


def edit_local(model):
    prop(model).NominalValue = model.create_entity("IfcLengthMeasure", LOCAL_VALUE)
    x, *rest = location(model, MOVED).Coordinates
    model.by_guid(MOVED).ObjectPlacement.RelativePlacement.Location = model.create_entity("IfcCartesianPoint", (x + MOVE_X, *rest))


def edit_remote(model):
    model.by_guid(RENAMED).Name = REMOTE_NAME


def edit_conflict(model):
    prop(model).NominalValue = model.create_entity("IfcLengthMeasure", REMOTE_VALUE)


ENTITY = re.compile(r"^#(\d+)\s*=(.*)$", re.S)


def renumber(source: Path, target: Path, seed: int) -> dict:
    """Give every entity a new id from a seeded shuffle and write entities in new-id order."""
    text = source.read_text()
    head, rest = text.split("DATA;\n", 1)
    body, tail = rest.split("ENDSEC;\n", 1)
    entities = []
    for line in body.splitlines():
        match = ENTITY.match(line)
        if not match:
            raise ValueError(f"expected one entity per line: {line[:80]}")
        entities.append((int(match.group(1)), match.group(2)))
    order = [i for i, _ in entities]
    random.Random(seed).shuffle(order)
    mapping = {old: new for new, old in enumerate(order, start=1)}

    def rewrite(value):
        parts = value.split("'")
        for k in range(0, len(parts), 2):
            parts[k] = re.sub(r"#(\d+)", lambda m: f"#{mapping[int(m.group(1))]}", parts[k])
        return "'".join(parts)

    lines = sorted((mapping[i], f"#{mapping[i]}={rewrite(v)}") for i, v in entities)
    target.write_text(head + "DATA;\n" + "".join(line + "\n" for _, line in lines) + "ENDSEC;\n" + tail)
    return {"entities": len(mapping), "unchanged_ids": sum(1 for a, b in mapping.items() if a == b)}


def run(command, log: Path):
    start = time.monotonic()
    result = subprocess.run([str(x) for x in command], capture_output=True, text=True)
    log.write_text(result.stdout + ("\n--- stderr ---\n" + result.stderr if result.stderr else ""))
    return result.returncode, round(time.monotonic() - start, 2)


def athar_diff(old: Path, new: Path, log: Path) -> dict:
    code, _ = run([sys.executable, "-m", "athar", old, new], log)
    if code:
        raise RuntimeError(f"athar diff failed: {log}")
    report = json.loads(log.read_text())
    changes = []
    for item in report["modified"]:
        aspects = sorted(k for k, v in item["aspects"].items() if v == "changed")
        changes.append({"guid": item["new"]["guid"], "aspects": aspects})
    for kind in ("added", "deleted"):
        for item in report[kind]:
            changes.append({"guid": item.get("guid"), "aspects": [kind]})
    return {"stats": {k: report["stats"][k] for k in ("added", "deleted", "modified", "unchanged")}, "changes": sorted(changes, key=lambda c: c["guid"] or "")}


def check_merged(path: Path, base: Path) -> dict:
    model = ifcopenshell.open(str(path))
    return {
        "opens": True,
        "entities": len(list(model)),
        "local_property": prop(model).NominalValue.wrappedValue == LOCAL_VALUE,
        "local_move": location(model, MOVED).Coordinates[0] == location(ifcopenshell.open(str(base)), MOVED).Coordinates[0] + MOVE_X,
        "remote_name": model.by_guid(RENAMED).Name == REMOTE_NAME,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--ifcmerge", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    out = args.evidence
    out.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()

    base = out / "base.ifc"
    ifcopenshell.open(str(args.model)).write(str(base))
    summary = {"model": str(args.model), "renumbering": {}, "merges": {}}
    expected = ifcopenshell.open(str(base))
    edit_local(expected)
    edit_remote(expected)
    expected_path = out / "expected-both.ifc"
    expected.write(str(expected_path))
    summary["expected"] = athar_diff(base, expected_path, out / "base-expected.json")
    branches = {"local": (edit_local, 1), "remote": (edit_remote, 2), "conflict": (edit_conflict, 3)}
    for name, (edit, seed) in branches.items():
        model = ifcopenshell.open(str(base))
        edit(model)
        edited = out / f"{name}-edited.ifc"
        model.write(str(edited))
        raw = out / f"{name}-raw.ifc"
        summary["renumbering"][name] = {"seed": seed, **renumber(edited, raw, seed)}
        invariance = athar_diff(edited, raw, out / f"{name}-renumber-invariance.json")
        if invariance["stats"]["added"] or invariance["stats"]["deleted"] or invariance["stats"]["modified"]:
            raise AssertionError(f"renumbering changed {name}: {invariance}")
        rekeyed = out / f"{name}-rekeyed.ifc"
        code, seconds = run([sys.executable, "-m", "athar", "rekey", base, raw, "-o", rekeyed], out / f"{name}-rekey.json")
        if code:
            raise RuntimeError(f"rekey failed: {raw}")
        stats = json.loads((out / f"{name}-rekey.json").read_text())
        summary["renumbering"][name]["rekey"] = {"seconds": seconds, **{k: stats[k] for k in ("entities", "matched", "fresh", "role_conflicts")}}
        summary["renumbering"][name]["rekeyed_text_diff_vs_edited"] = subprocess.run(
            ["diff", edited, rekeyed], capture_output=True, text=True
        ).stdout.count("\n> ")

    cases = {"raw": ("local-raw", "remote-raw"), "rekeyed": ("local-rekeyed", "remote-rekeyed"), "conflict-rekeyed": ("local-rekeyed", "conflict-rekeyed")}
    for label, (left, right) in cases.items():
        merged = out / f"{label}-merged.ifc"
        merged.unlink(missing_ok=True)
        code, seconds = run(["perl", args.ifcmerge, base, out / f"{left}.ifc", out / f"{right}.ifc", merged], out / f"{label}-ifcmerge.log")
        row = {"exit": code, "seconds": seconds}
        if code == 0:
            try:
                row.update(check_merged(merged, base))
                report = athar_diff(base, merged, out / f"{label}-base-merged.json")
                row["base_diff"] = report["stats"]
                row["changes"] = report["changes"]
                row["exactly_both_edits"] = report["changes"] == summary["expected"]["changes"]
                row["expected_vs_merged"] = athar_diff(expected_path, merged, out / f"{label}-expected-merged.json")["stats"]
            except Exception as error:
                row["error"] = f"{type(error).__name__}: {error}"
        else:
            row["report"] = (out / f"{label}-ifcmerge.log").read_text()[:600]
        summary["merges"][label] = row
    summary["seconds"] = round(time.monotonic() - started, 2)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
