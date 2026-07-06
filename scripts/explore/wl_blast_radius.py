"""Predict the WL vh_topology blast radius of seed elements and compare it
against a diff report's transitive-modified set.

Usage:
    python scripts/explore/wl_blast_radius.py <file.ifc> <report.json> <seed_guid> [<seed_guid> ...]

Mirrors the native pipeline: context_adj (undirected, context/topology edges),
spatial_adj (undirected, spatial edges incl. IfcRelAggregates onto spatial
roots), context_k=1, spatial_k=2. An entity's vh_topology flips iff a seed
lies within its BFS neighborhood.
"""

import collections
import json
import sys

import ifcopenshell

SPATIAL_ROOTS = {"IfcProject", "IfcSite", "IfcBuilding", "IfcBuildingStorey", "IfcSpace"}


def build_adjacency(f):
    spatial = collections.defaultdict(set)
    context = collections.defaultdict(set)

    def link(adj, a, b):
        adj[a].add(b)
        adj[b].add(a)

    for rel in f.by_type("IfcRelContainedInSpatialStructure"):
        for el in rel.RelatedElements:
            link(spatial, el.id(), rel.RelatingStructure.id())
    for rel in f.by_type("IfcRelAggregates"):
        target = rel.RelatingObject
        adj = spatial if target.is_a() in SPATIAL_ROOTS else None
        for obj in rel.RelatedObjects:
            if adj is not None:
                link(adj, obj.id(), target.id())
    for rel in f.by_type("IfcRelFillsElement"):
        link(context, rel.RelatingOpeningElement.id(), rel.RelatedBuildingElement.id())
    for name in ("IfcRelConnectsPathElements", "IfcRelConnectsElements"):
        for rel in f.by_type(name):
            if rel.RelatingElement and rel.RelatedElement:
                link(context, rel.RelatingElement.id(), rel.RelatedElement.id())
    for rel in f.by_type("IfcRelDefinesByType"):
        for obj in rel.RelatedObjects:
            link(context, obj.id(), rel.RelatingType.id())
    return spatial, context


def neighbors_within_k(adj, start, depth):
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
    return seen


def main():
    ifc_path, report_path, *seed_guids = sys.argv[1:]
    f = ifcopenshell.open(ifc_path)
    spatial, context = build_adjacency(f)

    seeds = [f.by_guid(g) for g in seed_guids]
    seed_ids = {s.id() for s in seeds}

    products = {p.id(): p for p in f.by_type("IfcProduct")}
    predicted = set()
    for pid in products:
        ctx_hood = neighbors_within_k(context, pid, 1)
        spa_hood = neighbors_within_k(spatial, pid, 2)
        if (ctx_hood | spa_hood) & seed_ids:
            predicted.add(pid)
    predicted_guids = {products[pid].GlobalId for pid in predicted} - set(seed_guids)

    report = json.loads(open(report_path).read())
    transitive = {
        m["new"]["guid"] for m in report["modified"] if m["change_scope"] == "transitive"
    }

    print(f"seeds: {seed_guids}")
    for s in seeds:
        storeys = [
            rel.RelatingStructure.Name
            for rel in f.by_type("IfcRelContainedInSpatialStructure")
            if s in rel.RelatedElements
        ]
        print(f"  {s.is_a()} {s.GlobalId} storey={storeys}")
    print(f"predicted blast radius (excl seeds): {len(predicted_guids)}")
    print(f"report transitive-modified:          {len(transitive)}")
    print(f"predicted & transitive: {len(predicted_guids & transitive)}")
    print(f"predicted only:  {len(predicted_guids - transitive)}")
    print(f"transitive only: {len(transitive - predicted_guids)}")
    for g in sorted(transitive - predicted_guids)[:20]:
        el = f.by_guid(g)
        print(f"  unexplained: {el.is_a()} {g} ({el.Name})")


if __name__ == "__main__":
    main()
