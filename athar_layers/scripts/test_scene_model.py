"""Test the scene model on IFC files.

Usage: python scripts/explore/test_scene_model.py <file.ifc> [file2.ifc ...]

Parses each file, builds a scene model, and prints the human-readable overview.
"""

from __future__ import annotations

import sys

from athar_layers.parser import parse
from athar_layers.scene import build_scene, print_scene


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file.ifc> [file2.ifc ...]")
        sys.exit(1)

    for path in sys.argv[1:]:
        print(f"\n{'#' * 60}")
        print(f"# {path}")
        print(f"{'#' * 60}\n")

        parsed = parse(path)
        scene = build_scene(parsed)
        print_scene(scene)


if __name__ == "__main__":
    main()
