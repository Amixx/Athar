"""Reproduce the local rekey corpus measurements and controlled merge demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

import ifcopenshell


def run(command, log):
    start = time.monotonic()
    with log.open("w") as stream:
        result = subprocess.run([str(x) for x in command], stdout=stream, stderr=subprocess.PIPE, text=True)
    log.with_suffix(log.suffix + ".stderr").write_text(result.stderr)
    return result.returncode, round(time.monotonic() - start, 2)


def diff(old, new, log):
    code, _ = run([sys.executable, "-m", "athar", old, new], log)
    if code:
        raise RuntimeError(f"Athar diff failed: {log}")
    return json.loads(log.read_text())


def text_diff(old, new, stem):
    git = ["git", "-c", "filter.lfs.required=false", "-c", "filter.lfs.clean=", "-c", "filter.lfs.process=", "diff", "--no-index"]
    for kind in ("stat", "numstat"):
        code, _ = run([*git, "--" + kind, old, new], stem.with_suffix("." + kind))
        if code not in (0, 1):
            raise RuntimeError(f"git diff failed: {stem}")
    line = stem.with_suffix(".numstat").read_text().split("\t")
    return {"insertions": int(line[0]), "deletions": int(line[1])} if len(line) > 1 else {"insertions": 0, "deletions": 0}


def series(args):
    rows = []
    for pair in args.pairs.split(","):
        a, b = pair.split("-")
        base, new = args.corpus / (a + ".ifc"), args.corpus / (b + ".ifc")
        prefix = args.evidence / ("series-" + pair)
        out = args.evidence / ("series-" + pair + ".ifc")
        code, seconds = run(["/usr/bin/time", "-l", sys.executable, "-m", "athar", "rekey", base, new, "-o", out], prefix.with_suffix(".json"))
        row = {"pair": pair, "exit": code, "seconds": seconds}
        if code == 0:
            stats = json.loads(prefix.with_suffix(".json").read_text())
            row.update({k: stats[k] for k in ("entities", "matched", "fresh", "matched_by", "role_conflicts")})
            row["raw"] = text_diff(base, new, args.evidence / ("series-" + pair + "-raw"))
            row["rekeyed"] = text_diff(base, out, args.evidence / ("series-" + pair + "-rekeyed"))
            report = diff(new, out, args.evidence / ("series-" + pair + "-invariance.json"))
            row["invariance"] = {s: report["stats"][s] for s in ("added", "deleted", "modified", "unchanged")}
        rows.append(row)
        (args.evidence / "series-summary.json").write_text(json.dumps(rows, indent=2))
        print(json.dumps(row), flush=True)


def merge(args):
    folder = args.evidence / "merge"
    folder.mkdir(exist_ok=True)
    base_path, local_path, remote_path = (folder / name for name in ("base.ifc", "local-raw.ifc", "remote-raw.ifc"))
    base = ifcopenshell.open(str(args.corpus / "r6.ifc"))
    local = ifcopenshell.open(str(args.corpus / "r7.ifc"))
    base.write(str(base_path))
    local.write(str(local_path))
    initial = diff(base_path, local_path, folder / "base-local.json")
    data_changes = [item for item in initial["modified"] if item["aspects"]["data"] == "changed"]
    if len(data_changes) != 1:
        raise ValueError("expected the r6/r7 single instance property edit")
    guid = data_changes[0]["new"]["guid"]

    def properties(model, product_guid):
        product = model.by_guid(product_guid)
        result = {}
        for rel in product.IsDefinedBy:
            if rel.is_a("IfcRelDefinesByProperties"):
                pset = rel.RelatingPropertyDefinition
                for prop in getattr(pset, "HasProperties", ()):
                    if prop.is_a("IfcPropertySingleValue"):
                        result[(pset.Name, prop.Name)] = prop
        return result

    item = data_changes[0]["property_deltas"]["added"][0]
    key = next(key for key in properties(local, guid) if key[1] == item["name"])
    local_value = item["new_value"]
    local_prop = properties(local, guid)[key]
    cls = local_prop.NominalValue.is_a()
    remote = ifcopenshell.open(str(local_path))
    for rel in remote.by_guid(guid).IsDefinedBy:
        if rel.is_a("IfcRelDefinesByProperties"):
            pset = rel.RelatingPropertyDefinition
            if pset.is_a("IfcPropertySet"):
                pset.HasProperties = tuple(prop for prop in pset.HasProperties if prop.Name != key[1])
    old_value = None
    victim = sorted(remote.by_type("IfcWall"), key=lambda e: e.GlobalId)[0]
    remote_guid, old_name = victim.GlobalId, victim.Name
    victim.Name = "Rekey independent branch edit"
    remote.write(str(remote_path))
    expected = ifcopenshell.open(str(local_path))
    expected.by_guid(remote_guid).Name = victim.Name
    expected_path = folder / "expected-both.ifc"
    expected.write(str(expected_path))

    summary = {"method": "r6 base and r7 actual instance edit; independent remote constructed by reverting that property in r7 and editing a different wall Name; all three serialized with IfcOpenShell to control lexical noise", "local": {"guid": guid, "property": key, "old": old_value, "new": local_value}, "remote": {"guid": remote_guid, "old_name": old_name, "new_name": victim.Name}, "merges": {}}
    diff(base_path, remote_path, folder / "base-remote.json")
    for label in ("raw", "rekeyed"):
        left, right = local_path, remote_path
        if label == "rekeyed":
            left, right = folder / "local-rekeyed.ifc", folder / "remote-rekeyed.ifc"
            for source, target in ((local_path, left), (remote_path, right)):
                code, _ = run([sys.executable, "-m", "athar", "rekey", base_path, source, "-o", target], target.with_suffix(".json"))
                if code:
                    raise RuntimeError(f"rekey failed: {source}")
                invariant = diff(source, target, target.with_suffix(".invariance.json"))
                assert all(invariant["stats"][s] == 0 for s in ("added", "deleted", "modified"))
        merged = folder / (label + "-merged.ifc")
        code, seconds = run(["perl", args.evidence / "ifcmerge" / "ifcmerge", base_path, left, right, merged], folder / (label + "-ifcmerge.log"))
        row = {"exit": code, "seconds": seconds}
        if code == 0:
            result = ifcopenshell.open(str(merged))
            row["both_edits"] = properties(result, guid)[key].NominalValue.wrappedValue == local_value and result.by_guid(remote_guid).Name == victim.Name
            report = diff(base_path, merged, folder / (label + "-base-merged.json"))
            row["base_diff"] = {s: report["stats"][s] for s in ("added", "deleted", "modified", "unchanged")}
            expected_diff = diff(expected_path, merged, folder / (label + "-expected-merged.json"))
            row["expected_diff"] = {s: expected_diff["stats"][s] for s in ("added", "deleted", "modified")}
        summary["merges"][label] = row
        (folder / "summary.json").write_text(json.dumps(summary, indent=2))
        print(label, json.dumps(row), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("series", "merge"))
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--pairs", default="r2-r3,r3-r4,r4-r5,r5-r6,r6-r7,r7-r8,r8-r9,r9-r10")
    args = parser.parse_args()
    {"series": series, "merge": merge}[args.mode](args)
