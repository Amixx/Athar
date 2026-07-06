"""Classify each geometry-aspect change on a report pair as tessellation
reorder-wobble (same mesh, permuted) vs a genuine content change.

For every product whose report `aspects.geometry == "changed"`, pull its
IfcTriangulatedFaceSet (or other rep items) from both files and compare:
  - the SET of vertex coordinate tuples (order-independent)
  - the SET of triangles normalized as frozensets of remapped-by-coordinate
    vertices (order-independent, index-independent)
If both sets match, the byte difference is pure reordering (wobble). If the
canonicalized mesh differs, it is a genuine geometry change.

Usage:
    python scripts/explore/roundtrip_wobble_classify.py <a.ifc> <b.ifc> <report.json>
"""

import json
import sys

import ifcopenshell


def mesh_of(el):
    rep = getattr(el, "Representation", None)
    if rep is None:
        return None
    meshes = []
    for shape in rep.Representations or []:
        for item in shape.Items or []:
            if item.is_a("IfcTriangulatedFaceSet"):
                coords = tuple(tuple(round(c, 6) for c in p) for p in item.Coordinates.CoordList)
                faces = item.CoordIndex
                meshes.append(("tri", coords, faces))
            else:
                meshes.append(("other", item.is_a(), None))
    return meshes


def canon_mesh(m):
    kind, a, b = m
    if kind != "tri":
        return ("other", a)
    coords, faces = a, b
    vset = frozenset(coords)
    tris = frozenset(
        frozenset((coords[i - 1] for i in tri)) for tri in faces
    )
    return ("tri", vset, tris, len(coords), len(faces))


def classify(fa, fb, guid):
    ea, eb = fa.by_guid(guid), fb.by_guid(guid)
    ma, mb = mesh_of(ea), mesh_of(eb)
    if ma is None or mb is None:
        return "no-rep", ""
    if len(ma) != len(mb):
        return "genuine", f"item-count {len(ma)}->{len(mb)}"
    verdicts = []
    for x, y in zip(ma, mb):
        cx, cy = canon_mesh(x), canon_mesh(y)
        if cx[0] != "tri" or cy[0] != "tri":
            verdicts.append("nontri" if cx == cy else "nontri-diff")
            continue
        same_v = cx[1] == cy[1]
        same_t = cx[2] == cy[2]
        nv = f"{cx[3]}v/{cy[3]}v" if not same_v else f"{cx[3]}v"
        nt = f"{cx[4]}t/{cy[4]}t" if cx[4] != cy[4] else f"{cx[4]}t"
        if same_v and same_t:
            verdicts.append(f"WOBBLE(perm) {nv} {nt}")
        elif same_v and not same_t:
            verdicts.append(f"WOBBLE(faces-reorder,verts-same) {nv} {nt}")
        else:
            vd = len(cx[1] ^ cy[1])
            verdicts.append(f"GENUINE verts-differ symdiff={vd} {nv} {nt}")
    kind = "wobble" if all(v.startswith("WOBBLE") for v in verdicts) else "genuine/mixed"
    return kind, "; ".join(verdicts)


def main():
    a, b, report = sys.argv[1:4]
    fa, fb = ifcopenshell.open(a), ifcopenshell.open(b)
    r = json.load(open(report))
    geo = [m for m in r["modified"] if m["aspects"]["geometry"] == "changed"]
    print(f"{report}: {len(geo)} geometry-changed")
    for m in geo:
        g = m["new"]["guid"]
        kind, detail = classify(fa, fb, g)
        print(f"  [{kind:12}] {m['new']['class']} {g} {m['new']['name']!r}")
        print(f"                 {detail}")


if __name__ == "__main__":
    main()
