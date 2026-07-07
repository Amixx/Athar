"""Scan a consecutive export pair for elements re-contained into a different
IfcBuildingStorey (by GUID) with no other change — the blind spot of the
class-only / spatial_k=1 WL scheme. Also reports added/deleted openings and
doors and whether their host walls gain a context (Fills) neighbor.

Usage:
    python scripts/explore/roundtrip_recontain_scan.py <a.ifc> <b.ifc>
"""

import sys

import ifcopenshell


def containment(f):
    out = {}
    for rel in f.by_type("IfcRelContainedInSpatialStructure"):
        s = rel.RelatingStructure
        for el in rel.RelatedElements:
            out[el.GlobalId] = (s.GlobalId, s.is_a(), getattr(s, "Name", None))
    return out


def fills(f):
    out = {}
    for rel in f.by_type("IfcRelFillsElement"):
        out[rel.RelatingOpeningElement.GlobalId] = rel.RelatedBuildingElement.GlobalId
    return out


def main():
    a, b = sys.argv[1:3]
    fa, fb = ifcopenshell.open(a), ifcopenshell.open(b)
    ca, cb = containment(fa), containment(fb)
    moved = []
    for g in set(ca) & set(cb):
        if ca[g][0] != cb[g][0]:
            el = fb.by_guid(g)
            moved.append((el.is_a(), g, ca[g], cb[g]))
    print(f"{a} -> {b}")
    print(f"  re-contained elements: {len(moved)}")
    for cls, g, oa, ob in moved[:40]:
        same_cls = oa[1] == ob[1]
        print(f"    {cls} {g}: {oa[2]!r}({oa[1]}) -> {ob[2]!r}({ob[1]}) same_container_class={same_cls}")

    fa_ops, fb_ops = fills(fa), fills(fb)
    new_ops = set(fb_ops) - set(fa_ops)
    del_ops = set(fa_ops) - set(fb_ops)
    print(f"  new Fills openings: {len(new_ops)}  removed: {len(del_ops)}")
    for o in list(new_ops)[:10]:
        host = fb.by_guid(fb_ops[o])
        print(f"    opening {o} fills {host.is_a()} {fb_ops[o]}")


if __name__ == "__main__":
    main()
