"""Simulate WL vh_topology under the OLD scheme (seed=class|vh_geometry,
context_k=1, spatial_k=2) and the PROPOSED NEW scheme (seed=class-only,
context_k=1, spatial_k=1) directly from the two IFC files' real adjacency,
keyed by GlobalId. Validates OLD against the committed report's topology set,
then predicts the post-fix added/deleted/modified/scope counts.

Usage:
    python scripts/explore/roundtrip_topology_sim.py <a.ifc> <b.ifc> <report.json>
"""

import collections
import json
import sys

import ifcopenshell

SPATIAL_ROOTS = {"IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey", "IfcSpace"}


def canon_class(entity_type):
    return "IfcWall" if entity_type == "IfcWallStandardCase" else entity_type


def build(f):
    id2guid = {}
    guid2class = {}
    for e in f.by_type("IfcRoot"):
        try:
            g = e.GlobalId
        except Exception:
            continue
        id2guid[e.id()] = g
        guid2class[g] = canon_class(e.is_a())

    spatial = collections.defaultdict(set)
    context = collections.defaultdict(set)

    def gl(x):
        return id2guid.get(x)

    def link(adj, a, b):
        a, b = gl(a), gl(b)
        if a is None or b is None:
            return
        adj[a].add(b)
        adj[b].add(a)

    for rel in f.by_type("IfcRelContainedInSpatialStructure"):
        for el in rel.RelatedElements:
            link(spatial, el.id(), rel.RelatingStructure.id())
    for rel in f.by_type("IfcRelAggregates"):
        target = rel.RelatingObject
        if target.is_a() in SPATIAL_ROOTS:
            for obj in rel.RelatedObjects:
                link(spatial, obj.id(), target.id())
    for rel in f.by_type("IfcRelFillsElement"):
        link(context, rel.RelatingOpeningElement.id(), rel.RelatedBuildingElement.id())
    for name in ("IfcRelConnectsPathElements", "IfcRelConnectsElements"):
        for rel in f.by_type(name):
            if rel.RelatingElement and rel.RelatedElement:
                link(context, rel.RelatingElement.id(), rel.RelatedElement.id())
    for rel in f.by_type("IfcRelDefinesByType"):
        for obj in rel.RelatedObjects:
            link(context, obj.id(), rel.RelatingType.id())
    return spatial, context, guid2class


def within_k(adj, start, depth):
    seen = {start}
    frontier = [start]
    for _ in range(depth):
        nxt = []
        for node in frontier:
            for n in adj.get(node, ()):
                if n not in seen:
                    seen.add(n)
                    nxt.append(n)
        frontier = nxt
    seen.discard(start)
    return seen


def class_multiset(neigh_guids, guid2class):
    return tuple(sorted(guid2class.get(g, "?") for g in neigh_guids))


def main():
    a, b, report = sys.argv[1:4]
    fa, fb = ifcopenshell.open(a), ifcopenshell.open(b)
    sa, ca, cls_a = build(fa)
    sb, cb, cls_b = build(fb)
    r = json.load(open(report))

    both = {}
    for m in r["modified"]:
        both[m["new"]["guid"]] = m
    unchanged_guids = {u["new"]["guid"] for u in r["unchanged"]}
    all_both = set(both) | unchanged_guids
    added = {x["guid"] for x in r["added"]}
    deleted = {x["guid"] for x in r["deleted"]}
    geo_changed = {g for g, m in both.items() if m["aspects"]["geometry"] == "changed"}

    def old_flip(g):
        na = within_k(ca, g, 1) | within_k(sa, g, 2)
        nb = within_k(cb, g, 1) | within_k(sb, g, 2)
        if na != nb:
            return True
        if g in geo_changed:
            return True
        return bool((na | {g}) & geo_changed)

    old_pred = {g for g in all_both if old_flip(g)}
    report_topo = {m["new"]["guid"] for m in r["modified"] if m["aspects"]["topology"] == "changed"}

    def new_flip(g):
        ka = (class_multiset(within_k(ca, g, 1), cls_a), class_multiset(within_k(sa, g, 1), cls_a))
        kb = (class_multiset(within_k(cb, g, 1), cls_b), class_multiset(within_k(sb, g, 1), cls_b))
        return ka != kb

    new_topo = {g for g in all_both if new_flip(g)}

    new_mod = 0
    scope = collections.Counter()
    for g in all_both:
        m = both.get(g)
        geom = m and m["aspects"]["geometry"] == "changed"
        data = m and m["aspects"]["data"] == "changed"
        plac = m and m["aspects"]["placement"] == "changed"
        topo = g in new_topo
        self_changed = geom or data or plac
        if not (self_changed or topo):
            continue
        new_mod += 1
        if self_changed and topo:
            scope["mixed"] += 1
        elif topo:
            scope["transitive"] += 1
        else:
            scope["intrinsic"] += 1

    print(f"== {report.split('/')[-1]} ==")
    print(f"  OLD topo predicted={len(old_pred)}  report topo={len(report_topo)}  "
          f"match={len(old_pred & report_topo)}  pred_only={len(old_pred - report_topo)}  "
          f"report_only={len(report_topo - old_pred)}")
    print(f"  report counts: added={len(added)} deleted={len(deleted)} modified={len(r['modified'])}")
    print(f"  NEW topo flips={len(new_topo)}")
    print(f"  NEW predicted: added={len(added)} deleted={len(deleted)} modified={new_mod} scope={dict(scope)}")
    if new_topo:
        byc = collections.Counter(cls_b.get(g, cls_a.get(g, "?")) for g in new_topo)
        print(f"  NEW topo-flip classes: {dict(byc)}")
        for g in sorted(new_topo):
            reason_ctx = (class_multiset(within_k(ca, g, 1), cls_a) != class_multiset(within_k(cb, g, 1), cls_b))
            reason_spa = (class_multiset(within_k(sa, g, 1), cls_a) != class_multiset(within_k(sb, g, 1), cls_b))
            tag = ("ctx" if reason_ctx else "") + ("+spa" if reason_spa else "")
            cl = cls_b.get(g, cls_a.get(g, "?"))
            print(f"      {cl} {g} [{tag}]")


if __name__ == "__main__":
    main()
