#!/usr/bin/env python3
"""Take an IFC file and produce a modified copy with known, deterministic changes.

Changes made:
1. Rename the first IfcWall
2. Delete the second IfcWall
3. Add a new IfcWall
4. Change a property on the first IfcSlab (if it has psets)
5. Move the first IfcBuildingElementProxy (shift placement)

Prints a manifest of changes to stdout for use in test assertions.
"""

import argparse
import json
import sys

import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element
import ifcopenshell.util.placement


def make_modified(input_path: str, output_path: str) -> dict:
    """Modify an IFC file and return a manifest of changes made."""
    ifc = ifcopenshell.open(input_path)
    manifest = {"renamed": [], "deleted": [], "added": [], "property_changed": [], "moved": []}

    walls = ifc.by_type("IfcWall")
    slabs = ifc.by_type("IfcSlab")
    proxies = ifc.by_type("IfcBuildingElementProxy")

    # 1. Rename first wall
    if len(walls) >= 1:
        wall = walls[0]
        old_name = wall.Name
        wall.Name = f"{old_name} (Renovated)"
        manifest["renamed"].append({
            "guid": wall.GlobalId,
            "old_name": old_name,
            "new_name": wall.Name,
        })

    # 2. Delete second wall
    if len(walls) >= 2:
        wall = walls[1]
        manifest["deleted"].append({
            "guid": wall.GlobalId,
            "ifc_class": wall.is_a(),
            "name": wall.Name,
        })
        ifcopenshell.api.run("root.remove_product", ifc, product=wall)

    # 3. Add a new wall in the first storey
    storeys = ifc.by_type("IfcBuildingStorey")
    if storeys:
        new_wall = ifcopenshell.api.run(
            "root.create_entity", ifc, ifc_class="IfcWall", name="New Test Wall",
        )
        ifcopenshell.api.run(
            "spatial.assign_container", ifc,
            products=[new_wall], relating_structure=storeys[0],
        )
        manifest["added"].append({
            "guid": new_wall.GlobalId,
            "ifc_class": new_wall.is_a(),
            "name": new_wall.Name,
        })

    # 4. Change/add a property on first slab
    if slabs:
        slab = slabs[0]
        pset = ifcopenshell.api.run(
            "pset.add_pset", ifc, product=slab, name="Athar_TestPset",
        )
        ifcopenshell.api.run(
            "pset.edit_pset", ifc, pset=pset,
            properties={"TestProperty": "modified_value"},
        )
        manifest["property_changed"].append({
            "guid": slab.GlobalId,
            "pset": "Athar_TestPset",
            "property": "TestProperty",
            "value": "modified_value",
        })

    ifc.write(output_path)
    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Produce a modified IFC with known changes for testing",
    )
    parser.add_argument("input", help="Path to the source IFC file")
    parser.add_argument("output", help="Path for the modified IFC file")
    args = parser.parse_args()

    manifest = make_modified(args.input, args.output)
    json.dump(manifest, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
