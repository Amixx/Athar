"""Build a Scene Model from parsed IFC data.

Transforms raw IFC entity data + relationships into a human-oriented
representation: spatial hierarchy, hosting chains (wall→opening→door),
element labels, and compass-based wall orientation.

Input: output of parser.parse() (dict with entities + relationships).
Output: dict with storeys, elements with human labels, hosting graph,
        and element compass orientation where applicable.
"""

from __future__ import annotations

import math
import re

# --- IFC class → human-readable category ---

_CLASS_CATEGORY = {
    "IfcWall": "wall",
    "IfcWallStandardCase": "wall",
    "IfcCurtainWall": "curtain wall",
    "IfcDoor": "door",
    "IfcWindow": "window",
    "IfcSlab": "floor slab",
    "IfcRoof": "roof",
    "IfcStair": "stair",
    "IfcStairFlight": "stair flight",
    "IfcRamp": "ramp",
    "IfcRampFlight": "ramp flight",
    "IfcRailing": "railing",
    "IfcColumn": "column",
    "IfcBeam": "beam",
    "IfcMember": "structural member",
    "IfcPlate": "plate",
    "IfcFooting": "footing",
    "IfcPile": "pile",
    "IfcFurnishingElement": "furniture",
    "IfcFurniture": "furniture",
    "IfcFlowTerminal": "fixture",
    "IfcSanitaryTerminal": "sanitary fixture",
    "IfcFlowSegment": "pipe/duct",
    "IfcDistributionPort": "port",
    "IfcOpeningElement": "opening",
    "IfcBuildingElementProxy": "element",
    "IfcGeographicElement": "landscape element",
    "IfcSite": "site element",
}


def human_class(ifc_class: str) -> str:
    """Convert an IFC class name to a human-readable category name."""
    return _CLASS_CATEGORY.get(ifc_class, ifc_class.replace("Ifc", "").lower())

# Regex for Revit-style names: "Family Name:Type Name:ElementId"
_REVIT_NAME_RE = re.compile(r"^(.+):(.+):(\d+)$")
# Simpler pattern: "Name:ElementId" (e.g. "Surface:1310134")
_REVIT_NAME_SHORT_RE = re.compile(r"^(.+):(\d{4,})$")

# Names that are auto-generated and not useful
_AUTOGEN_NAME_RE = re.compile(r"^.+#\d+$")

# Manufacturer/catalog prefixes like "M_", "P_"
_MFR_PREFIX_RE = re.compile(r"^[A-Z]_")
# Dimension patterns: "1220 x 1830 x 0610mm" or "1525 x 762mm"
_DIMENSION_RE = re.compile(r"\d+\s*x\s*\d+(\s*x\s*\d+)*\s*mm\b")


def build_scene(parsed: dict) -> dict:
    """Build a scene model from parser output.

    Returns:
        {
            "storeys": [
                { "guid", "name", "elevation", "element_guids": [...] },
                ...
            ],
            "elements": {
                guid: {
                    "guid", "ifc_class", "category", "name", "label",
                    "storey", "position", "is_external",
                    "host_guid", "hosted_guids", "parent_guid", "child_guids",
                    "orientation",  # compass direction for walls
                },
                ...
            },
        }
    """
    entities = parsed["entities"]
    rels = parsed.get("relationships", {})

    # Build relationship lookups
    hosting = _build_hosting_graph(rels, entities)
    aggregation = _build_aggregation_graph(rels, entities)
    storey_elements = _build_storey_map(rels, entities)

    # Build storey list
    storeys = _build_storeys(entities, storey_elements)

    # Determine which storey each element belongs to (including nested)
    guid_to_storey = {}
    for storey_guid, element_guids in storey_elements.items():
        storey_name = entities.get(storey_guid, {}).get("name")
        for guid in element_guids:
            guid_to_storey[guid] = storey_name
    # Elements contained via aggregation inherit parent's storey
    for guid, parent_guid in aggregation["child_to_parent"].items():
        if guid not in guid_to_storey and parent_guid in guid_to_storey:
            guid_to_storey[guid] = guid_to_storey[parent_guid]

    # Build scene elements
    elements = {}
    for guid, ent in entities.items():
        ifc_class = ent["ifc_class"]

        # Skip spatial structure entities (storeys, building, site, spaces, project)
        if ifc_class in ("IfcBuilding", "IfcBuildingStorey", "IfcProject"):
            continue

        category = human_class(ifc_class)
        name = ent.get("name")
        is_external = _is_external(ent)
        position = _extract_position(ent.get("placement"))
        orientation = _wall_orientation(ent) if category == "wall" else None

        el = {
            "guid": guid,
            "ifc_class": ifc_class,
            "category": category,
            "name": clean_name(name),
            "storey": guid_to_storey.get(guid) or ent.get("container"),
            "position": position,
            "is_external": is_external,
            "host_guid": hosting["element_to_host"].get(guid),
            "hosted_guids": hosting["host_to_elements"].get(guid, []),
            "parent_guid": aggregation["child_to_parent"].get(guid),
            "child_guids": aggregation["parent_to_children"].get(guid, []),
            "orientation": orientation,
        }

        # Generate human label
        el["label"] = _generate_label(el, entities, hosting, guid_to_storey)

        elements[guid] = el

    return {
        "storeys": storeys,
        "elements": elements,
    }


def _build_hosting_graph(rels: dict, entities: dict) -> dict:
    """Build wall→opening→door/window hosting chains.

    Uses voids (wall→opening) and fills (opening→door/window) to create
    a direct mapping: door/window → host wall, wall → hosted doors/windows.
    """
    # opening_guid → wall_guid
    opening_to_wall: dict[str, str] = {}
    for void in rels.get("voids", []):
        opening_to_wall[void["opening_guid"]] = void["element_guid"]

    # opening_guid → filler_guid (door/window)
    opening_to_filler: dict[str, str] = {}
    for fill in rels.get("fills", []):
        opening_to_filler[fill["opening_guid"]] = fill["element_guid"]

    # Resolve: filler → wall (skip the opening intermediary)
    element_to_host: dict[str, str] = {}
    host_to_elements: dict[str, list[str]] = {}

    for opening_guid, filler_guid in opening_to_filler.items():
        wall_guid = opening_to_wall.get(opening_guid)
        if wall_guid and filler_guid in entities:
            element_to_host[filler_guid] = wall_guid
            host_to_elements.setdefault(wall_guid, []).append(filler_guid)

    # Also map openings to their parent wall
    for opening_guid, wall_guid in opening_to_wall.items():
        if opening_guid in entities:
            element_to_host[opening_guid] = wall_guid

    return {
        "element_to_host": element_to_host,
        "host_to_elements": host_to_elements,
    }


def _build_aggregation_graph(rels: dict, entities: dict) -> dict:
    """Build parent→children aggregation (e.g. stair→flights, roof→slabs)."""
    parent_to_children: dict[str, list[str]] = {}
    child_to_parent: dict[str, str] = {}

    for agg in rels.get("aggregates", []):
        parent_guid = agg["parent_guid"]
        for child_guid in agg["child_guids"]:
            if child_guid in entities:
                child_to_parent[child_guid] = parent_guid
                parent_to_children.setdefault(parent_guid, []).append(child_guid)

    return {
        "parent_to_children": parent_to_children,
        "child_to_parent": child_to_parent,
    }


def _build_storey_map(rels: dict, entities: dict) -> dict[str, list[str]]:
    """Map storey GUIDs to their contained element GUIDs."""
    result: dict[str, list[str]] = {}
    for container_guid, element_guids in rels.get("spatial_children", {}).items():
        container = entities.get(container_guid, {})
        if container.get("ifc_class") in ("IfcBuildingStorey", "IfcSpace"):
            result[container_guid] = [g for g in element_guids if g in entities]
    return result


def _build_storeys(entities: dict, storey_elements: dict) -> list[dict]:
    """Build a sorted list of storey dicts."""
    storeys = []
    for guid, ent in entities.items():
        if ent["ifc_class"] != "IfcBuildingStorey":
            continue
        elevation = None
        placement = ent.get("placement")
        if placement:
            elevation = placement[2][3]  # Z component of translation
        storeys.append({
            "guid": guid,
            "name": ent["name"],
            "elevation": elevation,
            "element_guids": storey_elements.get(guid, []),
        })
    storeys.sort(key=lambda s: s["elevation"] or 0)
    return storeys


def clean_name(name: str | None) -> str | None:
    """Clean up IFC element names.

    Strips Revit element IDs from "Family:Type:12345" names and
    returns None for auto-generated names.
    """
    if not name:
        return None
    if _AUTOGEN_NAME_RE.match(name):
        return None

    m = _REVIT_NAME_RE.match(name)
    if m:
        family, type_name, _ = m.groups()
        # If family == type, just return one
        if family == type_name:
            return _humanize_product_name(family)
        return _humanize_product_name(f"{family}: {type_name}")

    m = _REVIT_NAME_SHORT_RE.match(name)
    if m:
        return _humanize_product_name(m.group(1))

    return name


def _humanize_product_name(name: str) -> str:
    """Strip manufacturer prefixes, dimensions, and normalize for display.

    'M_Dresser: 1220 x 1830 x 0610mm' → 'dresser'
    'M_Chair-Executive' → 'executive chair'
    'M_Bed-Box: 1525 x 2007mm - Queen' → 'bed box queen'
    """
    name = _MFR_PREFIX_RE.sub("", name)
    name = _DIMENSION_RE.sub("", name)
    # Clean leftover separators: ": - ", trailing ":", etc.
    name = re.sub(r":\s*-\s*", " ", name)
    name = re.sub(r":\s*", " ", name)
    name = name.strip(" -")
    name = name.lower()
    name = name.replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name or None


def _is_external(ent: dict) -> bool | None:
    """Check if an element is external (from Pset_*Common.IsExternal).

    Also infers from name patterns (e.g. Swedish "Yttervägg" = exterior wall).
    """
    psets = ent.get("property_sets", {})
    for pset_name, props in psets.items():
        if "IsExternal" in props:
            return bool(props["IsExternal"])

    # Infer from name
    name = (ent.get("name") or "").lower()
    type_name = (ent.get("type_name") or "").lower()
    combined = f"{name} {type_name}"

    exterior_hints = ["exterior", "ytter", "outer", "façade", "facade", "external"]
    interior_hints = ["interior", "inner", "inre"]
    for hint in exterior_hints:
        if hint in combined:
            return True
    for hint in interior_hints:
        if hint in combined:
            return False

    return None


def _extract_position(placement: list[list[float]] | None) -> dict | None:
    """Extract x/y/z position in meters from a 4x4 matrix."""
    if not placement:
        return None
    return {
        "x": round(placement[0][3] / 1000, 2),
        "y": round(placement[1][3] / 1000, 2),
        "z": round(placement[2][3] / 1000, 2),
    }


def _wall_orientation(ent: dict) -> str | None:
    """Determine compass orientation of a wall from its placement matrix.

    Uses the local X-axis direction of the wall (which runs along the wall's
    length in IFC convention). The wall "faces" perpendicular to this.

    Returns a compass direction like "north-facing" or "east-facing", or None.
    """
    placement = ent.get("placement")
    if not placement:
        return None

    # The wall's local X-axis direction (first column of rotation matrix)
    # tells us which way the wall runs. The wall faces perpendicular to this.
    wall_dir_x = placement[0][0]
    wall_dir_y = placement[1][0]

    # Normal to wall direction (perpendicular, 90° rotation)
    normal_x = -wall_dir_y
    normal_y = wall_dir_x

    length = math.sqrt(normal_x * normal_x + normal_y * normal_y)
    if length < 0.001:
        return None

    # IFC convention: +Y = north, +X = east
    angle = math.degrees(math.atan2(normal_x, normal_y))
    if angle < 0:
        angle += 360

    directions = [
        "north-facing", "northeast-facing", "east-facing", "southeast-facing",
        "south-facing", "southwest-facing", "west-facing", "northwest-facing",
    ]
    idx = round(angle / 45) % 8
    return directions[idx]


def _generate_label(el: dict, entities: dict, hosting: dict,
                    guid_to_storey: dict) -> str:
    """Generate a human-readable label for an element.

    Combines category, exterior/interior status, orientation, host context,
    and storey into a descriptive label.

    Examples:
        "exterior north-facing wall (Floor 0)"
        "interior door in interior wall (Floor 0)"
        "window in exterior east-facing wall (Floor 0)"
        "kitchen counter (Floor 0)"
    """
    category = el["category"]
    parts = []

    # Exterior/interior qualifier for walls, doors, windows
    if category == "wall":
        if el["is_external"] is True:
            parts.append("exterior")
        elif el["is_external"] is False:
            parts.append("interior")
        if el["orientation"]:
            parts.append(el["orientation"])
        parts.append("wall")

    elif category in ("door", "window"):
        # Describe by host wall context
        host_guid = el["host_guid"]
        if host_guid:
            host = entities.get(host_guid, {})
            host_external = _is_external(host)
            if host_external is True:
                parts.append("exterior")
            elif host_external is False:
                parts.append("interior")
        parts.append(category)
        # Add host wall context
        if host_guid and host_guid in entities:
            host_ent = entities[host_guid]
            host_scene = {
                "orientation": _wall_orientation(host_ent),
                "is_external": _is_external(host_ent),
            }
            if host_scene["orientation"]:
                parts.append(f"in {host_scene['orientation']} wall")

    elif category == "opening":
        # Openings are not interesting on their own — label by host
        host_guid = el["host_guid"]
        if host_guid and host_guid in entities:
            parts.append("opening in wall")
        else:
            parts.append("opening")

    elif category in ("furniture", "fixture", "element"):
        # Use the cleaned name if meaningful
        name = el["name"]
        if name:
            parts.append(name)
        else:
            parts.append(category)

    elif category in ("floor slab", "roof", "stair", "railing"):
        parts.append(category)

    else:
        parts.append(category)

    label = " ".join(parts)

    # Add storey suffix
    storey = el["storey"]
    if storey:
        label += f" on {storey}"

    return label


def print_scene(scene: dict) -> None:
    """Print a human-readable overview of the scene model."""
    print("=" * 60)
    print("SCENE MODEL")
    print("=" * 60)
    print()

    elements = scene["elements"]

    for storey in scene["storeys"]:
        print(f"📐 {storey['name']}"
              f" (elevation: {storey['elevation']}mm"
              f", {len(storey['element_guids'])} elements)")

        # Group elements by category
        storey_guids = set(storey["element_guids"])
        by_category: dict[str, list[dict]] = {}
        for guid in storey_guids:
            el = elements.get(guid)
            if not el:
                continue
            cat = el["category"]
            by_category.setdefault(cat, []).append(el)

        for cat in sorted(by_category):
            els = by_category[cat]
            print(f"  {cat} ({len(els)}):")
            # Show up to 5, summarize the rest
            for el in els[:5]:
                host_info = ""
                if el["host_guid"] and el["host_guid"] in elements:
                    host = elements[el["host_guid"]]
                    host_info = f" → in {host['label']}"
                hosted_info = ""
                if el["hosted_guids"]:
                    hosted_names = []
                    for hg in el["hosted_guids"][:3]:
                        h = elements.get(hg)
                        if h:
                            hosted_names.append(h["category"])
                    if hosted_names:
                        hosted_info = f" [hosts: {', '.join(hosted_names)}]"
                print(f"    • {el['label']}{host_info}{hosted_info}")
            if len(els) > 5:
                print(f"    ... and {len(els) - 5} more")
        print()

    # Elements not in any storey
    all_storey_guids = set()
    for s in scene["storeys"]:
        all_storey_guids.update(s["element_guids"])
    orphans = [
        el for guid, el in elements.items()
        if guid not in all_storey_guids
        and el["category"] not in ("site element", "opening", "port",
                                    "structural member", "stair flight")
    ]
    if orphans:
        print(f"📦 Not assigned to storey ({len(orphans)}):")
        by_cat: dict[str, int] = {}
        for el in orphans:
            by_cat[el["category"]] = by_cat.get(el["category"], 0) + 1
        for cat, count in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")
