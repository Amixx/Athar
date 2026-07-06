"""Walk the geometry representation subtree of given GUIDs in two IFC files
and diff it node-by-node to characterize export-to-export wobble.

Usage:
    python scripts/explore/roundtrip_wobble_geom.py <a.ifc> <b.ifc> <guid> [<guid> ...]

For each guid it dumps the flattened representation subtree (entity type +
raw non-reference attribute values) from both files, aligned by structural
path, and prints only the leaves that differ, with the raw float values so the
decimal place of any noise is visible.
"""

import sys

import ifcopenshell

EXPORT_DEPTH = 40


def walk(entity, seen, path, out):
    if entity is None:
        return
    key = entity.id()
    info = entity.get_info(recursive=False, include_identifier=False)
    cls = info.get("type")
    scalars = {}
    refs = {}
    for k, v in info.items():
        if k in ("type", "id", "GlobalId", "OwnerHistory"):
            continue
        if isinstance(v, ifcopenshell.entity_instance):
            refs[k] = v
        elif isinstance(v, (tuple, list)) and v and isinstance(v[0], ifcopenshell.entity_instance):
            refs[k] = v
        else:
            scalars[k] = v
    out.append((path, cls, scalars))
    if key in seen or len(path) > EXPORT_DEPTH:
        return
    seen.add(key)
    for k, v in refs.items():
        if isinstance(v, (tuple, list)):
            for i, item in enumerate(v):
                walk(item, seen, path + (f"{k}[{i}]:{item.is_a()}",), out)
        else:
            walk(v, seen, path + (f"{k}:{v.is_a()}",), out)


def subtree(f, guid):
    el = f.by_guid(guid)
    out = []
    rep = getattr(el, "Representation", None)
    walk(rep, set(), ("Representation",), out)
    return out, el


def diff_guid(fa, fb, guid):
    sa, ela = subtree(fa, guid)
    sb, elb = subtree(fb, guid)
    print(f"\n=== {guid}  {ela.is_a()} name={ela.Name!r} ===")
    print(f"    subtree nodes: A={len(sa)} B={len(sb)}")

    ma = {p: (c, s) for p, c, s in sa}
    mb = {p: (c, s) for p, c, s in sb}
    paths = sorted(set(ma) | set(mb))
    diffs = 0
    max_rel = 0.0
    for p in paths:
        a = ma.get(p)
        b = mb.get(p)
        if a is None:
            print(f"  ONLY-A missing in B: {'/'.join(p)}")
            diffs += 1
            continue
        if b is None:
            print(f"  ONLY-B missing in A: {'/'.join(p)}")
            diffs += 1
            continue
        ca, sca = a
        cb, scb = b
        if ca != cb:
            print(f"  CLASS {'/'.join(p)}: A={ca} B={cb}")
            diffs += 1
            continue
        for k in sorted(set(sca) | set(scb)):
            va = sca.get(k)
            vb = scb.get(k)
            if va != vb:
                rel = ""
                fa_ = _floats(va)
                fb_ = _floats(vb)
                if fa_ and fb_ and len(fa_) == len(fb_):
                    for x, y in zip(fa_, fb_):
                        d = abs(x - y)
                        denom = max(abs(x), abs(y), 1e-12)
                        max_rel = max(max_rel, d / denom)
                    rel = f"  maxabsdelta={max(abs(x-y) for x,y in zip(fa_,fb_)):.3e}"
                print(f"  LEAF {'/'.join(p)} .{k}: A={va!r} B={vb!r}{rel}")
                diffs += 1
    print(f"    total differing leaves: {diffs}  max_rel_float_delta={max_rel:.3e}")
    return diffs


def _floats(v):
    out = []
    def rec(x):
        if isinstance(x, bool):
            return
        if isinstance(x, (int, float)):
            out.append(float(x))
        elif isinstance(x, (tuple, list)):
            for i in x:
                rec(i)
    rec(v)
    return out


def main():
    a, b, *guids = sys.argv[1:]
    fa = ifcopenshell.open(a)
    fb = ifcopenshell.open(b)
    print(f"A={a}\nB={b}")
    for g in guids:
        diff_guid(fa, fb, g)


if __name__ == "__main__":
    main()
