import collections
import sys

import ifcopenshell


def summarize(path):
    f = ifcopenshell.open(path)
    products = f.by_type("IfcProduct")
    counts = collections.Counter(p.is_a() for p in products)
    guids = {p.GlobalId for p in products}
    return counts, guids


def main():
    old_path, new_path = sys.argv[1], sys.argv[2]
    old_counts, old_guids = summarize(old_path)
    new_counts, new_guids = summarize(new_path)

    shared = old_guids & new_guids
    print(f"old products: {sum(old_counts.values())}  ({len(old_guids)} unique guids)")
    print(f"new products: {sum(new_counts.values())}  ({len(new_guids)} unique guids)")
    print(f"guid overlap: {len(shared)}  "
          f"({100 * len(shared) / max(len(old_guids), 1):.1f}% of old)")
    print()
    print(f"{'class':<40} {'old':>8} {'new':>8}")
    for cls in sorted(set(old_counts) | set(new_counts)):
        print(f"{cls:<40} {old_counts.get(cls, 0):>8} {new_counts.get(cls, 0):>8}")


if __name__ == "__main__":
    main()
