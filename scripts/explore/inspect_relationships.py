"""Inspect IFC relationship types and spatial hierarchy in a file.

Usage: python scripts/explore/inspect_relationships.py <file.ifc>

Shows: all IfcRel* types with counts, spatial hierarchy (buildings/storeys/spaces),
product type counts, and samples of key relationships (voiding, filling, aggregation).
"""

from __future__ import annotations

import sys
from collections import Counter

import ifcopenshell


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file.ifc>")
        sys.exit(1)

    path = sys.argv[1]
    ifc = ifcopenshell.open(path)
    print(f"File: {path}")
    print(f"Schema: {ifc.schema}")
    print()

    # --- Relationship types ---
    rel_counts = Counter()
    for entity in ifc:
        t = entity.is_a()
        if t.startswith("IfcRel"):
            rel_counts[t] += 1

    print("=== Relationship types ===")
    for t, count in rel_counts.most_common():
        print(f"  {t}: {count}")
    print()

    # --- Spatial hierarchy ---
    print("=== Spatial hierarchy ===")
    for b in ifc.by_type("IfcBuilding"):
        print(f"  Building: {b.Name} ({b.GlobalId})")
    for s in ifc.by_type("IfcBuildingStorey"):
        print(f"  Storey: {s.Name} ({s.GlobalId})")
    spaces = ifc.by_type("IfcSpace")
    print(f"  Spaces: {len(spaces)}")
    for sp in spaces[:10]:
        print(f"    {sp.Name} ({sp.GlobalId})")
    if len(spaces) > 10:
        print(f"    ... and {len(spaces) - 10} more")
    print()

    # --- Product type counts ---
    print("=== Product counts ===")
    product_counts = Counter()
    for p in ifc.by_type("IfcProduct"):
        product_counts[p.is_a()] += 1
    for t, count in product_counts.most_common():
        print(f"  {t}: {count}")
    print()

    # --- Sample IfcRelVoidsElement ---
    voids = ifc.by_type("IfcRelVoidsElement")
    print(f"=== IfcRelVoidsElement ({len(voids)} total) ===")
    for rel in voids[:5]:
        parent = rel.RelatingBuildingElement
        opening = rel.RelatedOpeningElement
        print(f"  {parent.is_a()} '{parent.Name}' ({parent.GlobalId})"
              f" voided by {opening.is_a()} '{opening.Name}' ({opening.GlobalId})")
    if len(voids) > 5:
        print(f"  ... and {len(voids) - 5} more")
    print()

    # --- Sample IfcRelFillsElement ---
    fills = ifc.by_type("IfcRelFillsElement")
    print(f"=== IfcRelFillsElement ({len(fills)} total) ===")
    for rel in fills[:5]:
        opening = rel.RelatingOpeningElement
        filler = rel.RelatedBuildingElement
        host_rels = ifc.by_type("IfcRelVoidsElement")
        host = None
        for vr in host_rels:
            if vr.RelatedOpeningElement == opening:
                host = vr.RelatingBuildingElement
                break
        host_info = f" in {host.is_a()} '{host.Name}'" if host else ""
        print(f"  {filler.is_a()} '{filler.Name}' ({filler.GlobalId})"
              f" fills {opening.is_a()} '{opening.Name}'{host_info}")
    if len(fills) > 5:
        print(f"  ... and {len(fills) - 5} more")
    print()

    # --- Sample IfcRelAggregates ---
    aggs = ifc.by_type("IfcRelAggregates")
    print(f"=== IfcRelAggregates ({len(aggs)} total) ===")
    for rel in aggs[:5]:
        parent = rel.RelatingObject
        children = rel.RelatedObjects
        child_types = Counter(c.is_a() for c in children)
        child_str = ", ".join(f"{count} {t}" for t, count in child_types.most_common())
        print(f"  {parent.is_a()} '{parent.Name}' ({parent.GlobalId})"
              f" -> {child_str}")
    if len(aggs) > 5:
        print(f"  ... and {len(aggs) - 5} more")
    print()

    # --- IfcRelContainedInSpatialStructure sample ---
    contains = ifc.by_type("IfcRelContainedInSpatialStructure")
    print(f"=== IfcRelContainedInSpatialStructure ({len(contains)} total) ===")
    for rel in contains[:5]:
        container = rel.RelatingStructure
        elements = rel.RelatedElements
        el_types = Counter(e.is_a() for e in elements)
        el_str = ", ".join(f"{count} {t}" for t, count in el_types.most_common())
        print(f"  {container.is_a()} '{container.Name}' -> {el_str}")
    if len(contains) > 5:
        print(f"  ... and {len(contains) - 5} more")
    print()

    # --- Sample named elements with context ---
    print("=== Sample named IfcProducts (non-trivial names) ===")
    shown = 0
    for p in ifc.by_type("IfcProduct"):
        name = p.Name
        if name and not name.startswith(p.is_a().replace("Ifc", "")):
            container = None
            for rel in contains:
                if p in rel.RelatedElements:
                    container = rel.RelatingStructure.Name
                    break
            print(f"  {p.is_a()} '{name}' ({p.GlobalId}) in {container or '?'}")
            shown += 1
            if shown >= 15:
                break


if __name__ == "__main__":
    main()
