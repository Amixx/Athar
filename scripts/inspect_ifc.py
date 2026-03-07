#!/usr/bin/env python3
"""Print summary stats for an IFC file: schema, element types, counts, samples."""

import argparse
import sys
from collections import Counter

import ifcopenshell
import ifcopenshell.util.element


def inspect(filepath: str, show_samples: int = 3):
    ifc = ifcopenshell.open(filepath)
    print(f"File: {filepath}")
    print(f"Schema: {ifc.schema}")

    products = ifc.by_type("IfcProduct")
    print(f"Total IfcProduct entities: {len(products)}")

    types = Counter(p.is_a() for p in products)
    print(f"\nEntity types ({len(types)}):")
    for t, c in types.most_common():
        print(f"  {t}: {c}")

    # Show sample entities with properties
    if show_samples > 0:
        print(f"\nSample entities (first {show_samples}):")
        for product in products[:show_samples]:
            print(f"\n  [{product.is_a()}] {product.Name or '(unnamed)'}")
            print(f"    GlobalId: {product.GlobalId}")

            container = ifcopenshell.util.element.get_container(product)
            if container:
                print(f"    Container: {container.Name}")

            elem_type = ifcopenshell.util.element.get_type(product)
            if elem_type:
                print(f"    Type: {elem_type.Name}")

            psets = ifcopenshell.util.element.get_psets(product)
            if psets:
                print(f"    Property sets ({len(psets)}):")
                for pset_name, props in list(psets.items())[:3]:
                    prop_summary = ", ".join(
                        f"{k}={v!r}" for k, v in list(props.items())[:4]
                        if k != "id"
                    )
                    print(f"      {pset_name}: {prop_summary}")


def main():
    parser = argparse.ArgumentParser(description="Inspect an IFC file")
    parser.add_argument("file", help="Path to the IFC file")
    parser.add_argument(
        "-n", "--samples", type=int, default=3,
        help="Number of sample entities to show (default: 3)",
    )
    args = parser.parse_args()
    inspect(args.file, show_samples=args.samples)


if __name__ == "__main__":
    main()
