"""Human-readable descriptions of placement changes."""

from __future__ import annotations

import math
import re

# Names that look auto-generated (e.g. "Group#17", "Proxy#3") — not useful for context
_AUTOGEN_NAME = re.compile(r"^.+#\d+$")


def describe_placement_change(
    old_matrix: list[list[float]],
    new_matrix: list[list[float]],
    all_entities: dict | None = None,
) -> str:
    """Describe a placement change in human terms.

    Args:
        old_matrix: 4x4 transformation matrix (old placement).
        new_matrix: 4x4 transformation matrix (new placement).
        all_entities: Optional dict of {guid: entity_dict} from the new model,
            used to find nearby named entities for relative descriptions.

    Returns:
        A string like "moved 7.6m northwest (from near 'house - site' to near 'tree')"
    """
    old_pos = _extract_position(old_matrix)
    new_pos = _extract_position(new_matrix)
    dx = new_pos[0] - old_pos[0]
    dy = new_pos[1] - old_pos[1]
    dz = new_pos[2] - old_pos[2]

    dist_h = math.sqrt(dx * dx + dy * dy)
    dist_3d = math.sqrt(dx * dx + dy * dy + dz * dz)

    parts = []

    # Distance and compass direction (horizontal plane)
    if dist_h < 1.0:
        # Sub-millimeter horizontal movement — vertical only or negligible
        if abs(dz) >= 1.0:
            parts.append(f"moved {_fmt_dist(abs(dz))} {'up' if dz > 0 else 'down'}")
        else:
            return "placement changed (negligible shift)"
    else:
        direction = _compass_direction(dx, dy)
        parts.append(f"moved {_fmt_dist(dist_h)} {direction}")
        if abs(dz) >= 100.0:  # Only mention vertical if ≥0.1m
            parts.append(f"{_fmt_dist(abs(dz))} {'up' if dz > 0 else 'down'}")

    result = ", ".join(parts)

    # Relative positioning: find nearest named entities at old and new positions
    if all_entities:
        old_near = _nearest_named(old_pos, all_entities, exclude_pos=old_pos)
        new_near = _nearest_named(new_pos, all_entities, exclude_pos=new_pos)
        if old_near and new_near and old_near != new_near:
            result += f" (from near '{old_near}' to near '{new_near}')"
        elif new_near:
            result += f" (now near '{new_near}')"

    return result


def describe_position(matrix: list[list[float]]) -> str:
    """Describe a position in human terms (for added/deleted entities)."""
    pos = _extract_position(matrix)
    return f"at ({pos[0]/1000:.1f}, {pos[1]/1000:.1f}, {pos[2]/1000:.1f})m"


def _extract_position(matrix: list[list[float]]) -> tuple[float, float, float]:
    """Extract translation (x, y, z) from a 4x4 matrix. Values in mm."""
    return (matrix[0][3], matrix[1][3], matrix[2][3])


def _fmt_dist(mm: float) -> str:
    """Format a distance in mm to a human-readable string."""
    m = mm / 1000.0
    if m < 0.01:
        return f"{mm:.0f}mm"
    if m < 1.0:
        return f"{m:.2f}m"
    return f"{m:.1f}m"


def _compass_direction(dx: float, dy: float) -> str:
    """Convert dx/dy displacement to a compass direction.

    Assumes IFC convention: +Y is north, +X is east.
    """
    angle = math.degrees(math.atan2(dx, dy))  # atan2(east, north)
    if angle < 0:
        angle += 360

    # 8-point compass: N, NE, E, SE, S, SW, W, NW
    directions = ["north", "northeast", "east", "southeast",
                  "south", "southwest", "west", "northwest"]
    idx = round(angle / 45) % 8
    return directions[idx]


def _nearest_named(
    pos: tuple[float, float, float],
    entities: dict,
    exclude_pos: tuple[float, float, float] | None = None,
    min_dist: float = 100.0,  # ignore entities closer than 100mm (same spot)
) -> str | None:
    """Find the nearest named entity to a position.

    Skips entities at the exact same position (the entity itself).
    Returns the name, or None if nothing suitable found.
    """
    best_name = None
    best_dist = float("inf")

    for guid, ent in entities.items():
        name = ent.get("name")
        if not name or _AUTOGEN_NAME.match(name):
            continue
        placement = ent.get("placement")
        if placement is None:
            continue
        epos = _extract_position(placement)

        # Skip if this is at the same position as the entity we're describing
        if exclude_pos:
            d = math.sqrt(sum((a - b) ** 2 for a, b in zip(epos, exclude_pos)))
            if d < min_dist:
                continue

        d = math.sqrt(sum((a - b) ** 2 for a, b in zip(pos, epos)))
        if d < best_dist:
            best_dist = d
            best_name = name

    return best_name
