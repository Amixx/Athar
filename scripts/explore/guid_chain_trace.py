import sys

import ifcopenshell

SAMPLE_CLASSES = ["IfcDoor", "IfcColumn", "IfcWallStandardCase", "IfcCurtainWall"]


def index(path):
    f = ifcopenshell.open(path)
    return {p.GlobalId: (p.id(), p.is_a(), (p.Name or "")[:40]) for p in f.by_type("IfcProduct")}


def main():
    paths = sys.argv[1:]
    indexes = [(p.rsplit("/", 1)[-1], index(p)) for p in paths]

    base_name, base = indexes[0]
    sample = []
    seen = set()
    for guid, (sid, cls, name) in base.items():
        if cls in SAMPLE_CLASSES and cls not in seen:
            sample.append((guid, cls, name))
            seen.add(cls)
    print("sample trace (guid -> express id per file):")
    for guid, cls, name in sample:
        row = [f"{cls} {guid} ({name})"]
        for fname, idx in indexes:
            hit = idx.get(guid)
            row.append(f"{fname}:#{hit[0]}" if hit else f"{fname}:MISSING")
        print("  " + "  ".join(row))

    print("\npairwise guid-set + numbering stability:")
    for (an, a), (bn, b) in zip(indexes, indexes[1:]):
        shared = set(a) & set(b)
        renum = sum(1 for g in shared if a[g][0] != b[g][0])
        recls = sum(1 for g in shared if a[g][1] != b[g][1])
        print(f"  {an} -> {bn}: shared {len(shared)}/{len(a)}->{len(b)}, "
              f"express-id changed {renum}, class changed {recls}")


if __name__ == "__main__":
    main()
