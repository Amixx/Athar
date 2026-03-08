"""Inspect IfcSpace details and element property sets for room function inference.

Usage: python scripts/explore/inspect_spaces_and_psets.py <file.ifc>

Shows: space details with their contained elements, element type names,
and property sets that might hint at room function.
"""

from __future__ import annotations

import sys
from collections import Counter

import ifcopenshell
import ifcopenshell.util.element


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file.ifc>")
        sys.exit(1)

    path = sys.argv[1]
    ifc = ifcopenshell.open(path)
    print(f"File: {path}\n")

    # --- Spaces and their contents ---
    print("=== IfcSpace details ===")
    for space in ifc.by_type("IfcSpace"):
        print(f"\nSpace: '{space.Name}' ({space.GlobalId})")
        print(f"  LongName: {space.LongName}")
        psets = ifcopenshell.util.element.get_psets(space)
        for pset_name, props in psets.items():
            print(f"  PSet '{pset_name}':")
            for k, v in props.items():
                if k != "id":
                    print(f"    {k} = {v}")

    # --- What elements are in each storey? ---
    print("\n=== Elements per storey (type summary) ===")
    for rel in ifc.by_type("IfcRelContainedInSpatialStructure"):
        container = rel.RelatingStructure
        elements = rel.RelatedElements
        print(f"\n{container.is_a()} '{container.Name}':")
        # Show element type_name distribution
        type_names = Counter()
        for e in elements:
            etype = ifcopenshell.util.element.get_type(e)
            type_name = etype.Name if etype else "(no type)"
            type_names[f"{e.is_a()}:{type_name}"] += 1
        for key, count in type_names.most_common(15):
            print(f"  {count}x {key}")
        if len(type_names) > 15:
            print(f"  ... and {len(type_names) - 15} more type combos")

    # --- Sample property sets on a few walls/doors/windows ---
    print("\n=== Sample element property sets ===")
    for cls in ["IfcWallStandardCase", "IfcDoor", "IfcWindow", "IfcSlab"]:
        elements = ifc.by_type(cls)
        if elements:
            e = elements[0]
            print(f"\n{cls} '{e.Name}' ({e.GlobalId}):")
            psets = ifcopenshell.util.element.get_psets(e)
            for pset_name, props in psets.items():
                interesting = {k: v for k, v in props.items() if k != "id"}
                if interesting:
                    print(f"  PSet '{pset_name}': {list(interesting.keys())}")

    # --- IfcRelConnectsPathElements (wall connectivity) ---
    connects = ifc.by_type("IfcRelConnectsPathElements")
    print(f"\n=== IfcRelConnectsPathElements ({len(connects)} total) ===")
    for rel in connects[:5]:
        e1 = rel.RelatingElement
        e2 = rel.RelatedElement
        print(f"  '{e1.Name}' <-> '{e2.Name}'")


if __name__ == "__main__":
    main()
