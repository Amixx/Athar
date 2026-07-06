"""Probe whether a rigid group move in a diff is one shared-ancestor placement
change or N independent identical moves.

For each GUID (stdin or args file), compares old vs new:
- the element's own IfcLocalPlacement.RelativePlacement (location coords)
- the PlacementRelTo ancestor chain, reporting the highest chain node whose
  relative placement coordinates differ
- assembly/group membership (Decomposes, HasAssignments)

Usage:
    .venv/bin/python scripts/explore/placement_cascade_probe.py OLD.ifc NEW.ifc GUIDS.txt
"""

import sys
from collections import Counter

import ifcopenshell


def loc(axis_placement):
    if axis_placement is None:
        return None
    location = getattr(axis_placement, "Location", None)
    coords = getattr(location, "Coordinates", None)
    return tuple(round(c, 6) for c in coords) if coords else None


def chain(product):
    nodes = []
    placement = product.ObjectPlacement
    while placement is not None:
        nodes.append(loc(getattr(placement, "RelativePlacement", None)))
        placement = getattr(placement, "PlacementRelTo", None)
    return nodes


def memberships(product):
    out = []
    for rel in getattr(product, "Decomposes", None) or []:
        parent = rel.RelatingObject
        out.append(f"assembly:{parent.is_a()}#{parent.id()}({(parent.Name or '')[:30]})")
    for rel in getattr(product, "HasAssignments", None) or []:
        if rel.is_a("IfcRelAssignsToGroup"):
            group = rel.RelatingGroup
            out.append(f"group:{group.is_a()}#{group.id()}({(group.Name or '')[:30]})")
    return out


def spatial_parent(product):
    for rel in getattr(product, "ContainedInStructure", None) or []:
        parent = rel.RelatingStructure
        return f"{parent.is_a()}({(parent.Name or '')[:30]})"
    return None


def main():
    old_path, new_path, guids_path = sys.argv[1:4]
    guids = [line.strip() for line in open(guids_path) if len(line.strip()) == 22]
    old_file = ifcopenshell.open(old_path)
    new_file = ifcopenshell.open(new_path)

    own_changed = 0
    inherited_only = 0
    change_depths = Counter()
    parents = Counter()
    member_of = Counter()
    for guid in guids:
        try:
            old_product = old_file.by_guid(guid)
            new_product = new_file.by_guid(guid)
        except RuntimeError:
            print(f"  {guid}: missing on one side")
            continue
        old_chain = chain(old_product)
        new_chain = chain(new_product)
        diffs = [
            i
            for i, (a, b) in enumerate(zip(old_chain, new_chain))
            if a != b
        ]
        if len(old_chain) != len(new_chain):
            diffs.append(min(len(old_chain), len(new_chain)))
        if not diffs:
            print(f"  {guid}: chains equal?! depth={len(old_chain)}")
            continue
        if 0 in diffs:
            own_changed += 1
        else:
            inherited_only += 1
        change_depths[tuple(diffs)] += 1
        parents[spatial_parent(new_product)] += 1
        for m in memberships(new_product):
            member_of[m] += 1

    print(f"\nelements probed: {len(guids)}")
    print(f"own placement changed:        {own_changed}")
    print(f"inherited-only (ancestor):    {inherited_only}")
    print("changed chain depths (0=own, higher=ancestor):")
    for depths, count in change_depths.most_common():
        print(f"  depth {depths}: {count}")
    print("spatial parents:")
    for name, count in parents.most_common():
        print(f"  {name}: {count}")
    print("assembly/group memberships:")
    if not member_of:
        print("  (none)")
    for name, count in member_of.most_common():
        print(f"  {name}: {count}")


if __name__ == "__main__":
    main()
