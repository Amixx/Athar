"""Headless port of Bonsai (BlenderBIM) IfcGit's diff algorithm, for benchmarking.

Bonsai's `tool.Ifcgit` (IfcOpenShell v0.8.0,
src/bonsai/bonsai/tool/ifcgit.py) implements the diff in two steps:
`ifc_diff_ids` and `get_modified_step_ids`. This module ports both
faithfully. The original imports `bpy` and only runs inside Blender, so it
cannot be invoked from a CLI or a benchmark subprocess as-is.

Step-id detection (`ifc_diff_ids`) is a `git diff --no-index` of the two
STEP files, regexed for `+#N=` / `-#N=` line prefixes. This is not a
simplification of IfcGit's algorithm — line-based git diff of STEP text
*is* the algorithm Bonsai uses to find changed entities.

Bonsai's own UI only highlights products in the viewport; it never computes
added/deleted/modified *counts*. This script adds that normalization for
comparability with other diff tools: an id is a candidate "added" or
"removed" product only if the corresponding entity (looked up in the NEW
model for added, the OLD model for removed) `is_a("IfcProduct")`; the
propagated set from `get_modified_step_ids` is reported as "modified" once
the added ids are excluded from it.

`git diff --no-index` honours `.gitattributes` clean/smudge filters, so
`ifc_diff_ids` disables `filter=lfs` for its subprocess call; without that,
LFS-tracked `.ifc` inputs would diff as pointer stubs, not STEP text.

Usage:
    python scripts/explore/ifcgit_diff_runner.py OLD.ifc NEW.ifc
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

import ifcopenshell

_INSERTED_RE = re.compile(r"^\+#([0-9]+)=")
_DELETED_RE = re.compile(r"^-#([0-9]+)=")


def ifc_diff_ids(path_old: str, path_new: str) -> dict[str, set[int]]:
    """Port of Bonsai `tool.Ifcgit.ifc_diff_ids`: step ids from a git text diff.

    `git diff --no-index` still applies clean/smudge filters from
    `.gitattributes` (e.g. `filter=lfs`) before diffing. This repo marks
    `*.ifc` as LFS-filtered, which would otherwise turn the diff into a
    comparison of two LFS pointer stubs instead of the STEP text. The
    `-c filter.lfs.*` overrides force literal byte content into the diff,
    which is what the pointer-free algorithm assumes.
    """
    completed = subprocess.run(
        [
            "git",
            "-c",
            "filter.lfs.process=",
            "-c",
            "filter.lfs.clean=cat",
            "-c",
            "filter.lfs.smudge=cat",
            "-c",
            "filter.lfs.required=false",
            "diff",
            "--no-color",
            "--no-index",
            "--",
            path_old,
            path_new,
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode not in (0, 1):
        raise RuntimeError(f"git diff failed (exit {completed.returncode}): {completed.stderr.strip()}")

    inserted: set[int] = set()
    deleted: set[int] = set()
    for line in completed.stdout.split("\n"):
        match = _INSERTED_RE.search(line)
        if match:
            inserted.add(int(match.group(1)))
            continue
        match = _DELETED_RE.search(line)
        if match:
            deleted.add(int(match.group(1)))

    modified = inserted.intersection(deleted)
    return {
        "modified": modified,
        "added": inserted.difference(modified),
        "removed": deleted.difference(modified),
    }


def get_modified_step_ids(model, step_ids: dict[str, set[int]]) -> set[int]:
    """Port of Bonsai `tool.Ifcgit.get_modified_step_ids`: propagate changed ids to products."""
    modified: set[int] = set()

    def collect(entity, depth: int = 0) -> None:
        if depth > 2:
            return
        if entity.is_a("IfcProduct"):
            modified.add(entity.id())
        elif entity.is_a("IfcProductDefinitionShape"):
            for product in entity.ShapeOfProduct:
                modified.add(product.id())
        elif entity.is_a("IfcObjectPlacement"):
            for product in entity.PlacesObject:
                modified.add(product.id())
        elif entity.is_a("IfcTypeProduct"):
            for rel in entity.Types:
                for obj in rel.RelatedObjects:
                    modified.add(obj.id())
        elif entity.is_a("IfcShapeRepresentation"):
            for prod_rep in entity.OfProductRepresentation:
                for product in prod_rep.ShapeOfProduct:
                    modified.add(product.id())
        elif entity.is_a("IfcRepresentationItem"):
            for referencing in model.get_inverse(entity):
                if referencing.is_a("IfcShapeRepresentation"):
                    collect(referencing, depth + 1)
        elif entity.is_a("IfcPropertySet"):
            for rel in entity.DefinesOccurrence:
                for obj in rel.RelatedObjects:
                    modified.add(obj.id())
        elif entity.is_a("IfcProperty"):
            for pset in entity.PartOfPset:
                collect(pset, depth + 1)

    for step_id in step_ids["modified"] | step_ids["added"]:
        try:
            entity = model.by_id(step_id)
        except Exception:
            continue
        collect(entity)

    return modified


def _product_ids(model, step_ids: set[int]) -> set[int]:
    products = set()
    for step_id in step_ids:
        try:
            entity = model.by_id(step_id)
        except Exception:
            continue
        if entity is not None and entity.is_a("IfcProduct"):
            products.add(step_id)
    return products


def run(path_old: str, path_new: str) -> dict:
    step_ids = ifc_diff_ids(path_old, path_new)

    model_old = ifcopenshell.open(path_old)
    model_new = ifcopenshell.open(path_new)

    propagated = get_modified_step_ids(model_new, step_ids)
    added_products = _product_ids(model_new, step_ids["added"])
    removed_products = _product_ids(model_old, step_ids["removed"])
    modified_products = propagated - added_products

    return {
        "tool": "ifcgit_port",
        "counts": {
            "added": len(added_products),
            "deleted": len(removed_products),
            "modified": len(modified_products),
        },
        "step_ids": {
            "added": len(step_ids["added"]),
            "removed": len(step_ids["removed"]),
            "modified": len(step_ids["modified"]),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old_ifc", help="Path to the old IFC file")
    parser.add_argument("new_ifc", help="Path to the new IFC file")
    args = parser.parse_args()

    try:
        result = run(args.old_ifc, args.new_ifc)
    except Exception as exc:  # noqa: BLE001 - reported as JSON, not a traceback
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
