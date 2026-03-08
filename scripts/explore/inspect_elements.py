"""Inspect all product elements in an IFC file with positions and relationships.

Usage:
    python scripts/explore/inspect_elements.py data/BasicHouse.ifc
    python scripts/explore/inspect_elements.py data/BasicHouse.ifc --type IfcWallStandardCase
"""

import argparse
import ifcopenshell


def main():
    parser = argparse.ArgumentParser(description="Inspect IFC product elements")
    parser.add_argument("ifc_file", help="Path to IFC file")
    parser.add_argument("--type", help="Filter by IFC class (e.g. IfcWallStandardCase)")
    args = parser.parse_args()

    ifc = ifcopenshell.open(args.ifc_file)

    # Header info
    print(f"Schema: {ifc.schema}")
    print(f"File: {args.ifc_file}")
    print()

    # Element type summary
    from collections import Counter
    products = ifc.by_type("IfcProduct")
    type_counts = Counter(e.is_a() for e in products)
    print(f"Product elements ({len(products)} total):")
    for t, c in type_counts.most_common():
        print(f"  {t}: {c}")
    print()

    # Detailed listing
    if args.type:
        classes = [args.type]
    else:
        classes = [
            "IfcWallStandardCase", "IfcWall", "IfcDoor", "IfcWindow",
            "IfcSlab", "IfcRoof", "IfcStair", "IfcFurnishingElement",
            "IfcFlowTerminal", "IfcBuildingElementProxy",
        ]

    for cls in classes:
        elements = ifc.by_type(cls)
        if not elements:
            continue
        print(f"{'=' * 60}")
        print(f"{cls} ({len(elements)})")
        print(f"{'=' * 60}")
        for e in elements:
            loc = e.ObjectPlacement.RelativePlacement.Location
            coords = list(loc.Coordinates)
            print(f"  {e.GlobalId}  {e.Name}")
            print(f"    Position: ({coords[0]:.0f}, {coords[1]:.0f}, {coords[2]:.0f})")

            # Show relationships
            for rel in ifc.by_type("IfcRelVoidsElement"):
                if rel.RelatingBuildingElement == e:
                    opening = rel.RelatedOpeningElement
                    # Find what fills this opening
                    for frel in ifc.by_type("IfcRelFillsElement"):
                        if frel.RelatingOpeningElement == opening:
                            filler = frel.RelatedBuildingElement
                            print(f"    Hosts: {filler.is_a()} {filler.Name}")
        print()


if __name__ == "__main__":
    main()
