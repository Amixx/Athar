"""Generate 3 versions of BasicHouse.ifc for testing the diff workflow.

Creates:
  tests/fixtures/house_v1.ifc — Original (copy of BasicHouse.ifc)
  tests/fixtures/house_v2.ifc — Renovation: move furniture, remove some windows, add door property
  tests/fixtures/house_v2_scrambled.ifc — Same as v2, but all GlobalIds deterministically rewritten
  tests/fixtures/house_v3.ifc — Further changes: delete interior wall + its door, move exterior door

The changes are designed to exercise different diff capabilities:
  v1→v2: property changes, element deletions, furniture moves (bulk movement)
  v2→v3: structural deletion (wall + door), placement change on remaining element

Usage:
    python scripts/generate_house_versions.py
"""

import shutil
import uuid
import ifcopenshell
import ifcopenshell.guid


SRC = "data/BasicHouse.ifc"
V1 = "tests/fixtures/house_v1.ifc"
V2 = "tests/fixtures/house_v2.ifc"
V2_SCRAMBLED = "tests/fixtures/house_v2_scrambled.ifc"
V3 = "tests/fixtures/house_v3.ifc"
_GUID_SCRAMBLE_NAMESPACE = uuid.UUID("f43a7c2b-8fd4-4d2d-9898-8891d75432b2")


def _remove_element_and_relationships(ifc, element):
    """Remove an element and its associated relationships cleanly.

    Handles IfcRelFillsElement, IfcRelVoidsElement, IfcRelDefinesByProperties,
    IfcRelContainedInSpatialStructure, IfcRelAssociatesMaterial, and
    IfcRelDefinesByType references.
    """
    guid = element.GlobalId

    # Remove from IfcRelFillsElement (door/window → opening)
    for rel in list(ifc.by_type("IfcRelFillsElement")):
        if rel.RelatedBuildingElement == element:
            opening = rel.RelatingOpeningElement
            ifc.remove(rel)
            # Also remove the opening and its void relationship
            for vrel in list(ifc.by_type("IfcRelVoidsElement")):
                if vrel.RelatedOpeningElement == opening:
                    ifc.remove(vrel)
                    break
            ifc.remove(opening)

    # Remove from IfcRelVoidsElement (wall with openings)
    for rel in list(ifc.by_type("IfcRelVoidsElement")):
        if rel.RelatingBuildingElement == element:
            opening = rel.RelatedOpeningElement
            # Remove any filler in this opening
            for frel in list(ifc.by_type("IfcRelFillsElement")):
                if frel.RelatingOpeningElement == opening:
                    filler = frel.RelatedBuildingElement
                    ifc.remove(frel)
                    ifc.remove(filler)
            ifc.remove(rel)
            ifc.remove(opening)

    # Remove from IfcRelContainedInSpatialStructure
    for rel in list(ifc.by_type("IfcRelContainedInSpatialStructure")):
        related = list(rel.RelatedElements)
        if element in related:
            related.remove(element)
            if related:
                rel.RelatedElements = related
            else:
                ifc.remove(rel)

    # Remove from IfcRelDefinesByProperties
    for rel in list(ifc.by_type("IfcRelDefinesByProperties")):
        related = list(rel.RelatedObjects)
        if element in related:
            related.remove(element)
            if related:
                rel.RelatedObjects = related
            else:
                ifc.remove(rel)

    # Remove from IfcRelAssociatesMaterial
    for rel in list(ifc.by_type("IfcRelAssociatesMaterial")):
        related = list(rel.RelatedObjects)
        if element in related:
            related.remove(element)
            if related:
                rel.RelatedObjects = related
            else:
                ifc.remove(rel)

    # Remove from IfcRelDefinesByType
    for rel in list(ifc.by_type("IfcRelDefinesByType")):
        related = list(rel.RelatedObjects)
        if element in related:
            related.remove(element)
            if related:
                rel.RelatedObjects = related
            else:
                ifc.remove(rel)

    ifc.remove(element)
    print(f"  Removed: {guid}")


def _move_element(ifc, element, dx=0, dy=0, dz=0):
    """Shift an element's placement by (dx, dy, dz) in mm.

    Creates a new IfcCartesianPoint to avoid corrupting shared placement
    objects (IFC reuses origin points across many elements).
    """
    axis_placement = element.ObjectPlacement.RelativePlacement
    old_loc = axis_placement.Location
    coords = list(old_loc.Coordinates)
    coords[0] += dx
    coords[1] += dy
    coords[2] += dz
    new_loc = ifc.create_entity("IfcCartesianPoint", Coordinates=tuple(coords))
    axis_placement.Location = new_loc


def _find_by_guid(ifc, guid):
    """Find an element by GlobalId."""
    return ifc.by_guid(guid)


def _scramble_guid(guid):
    """Deterministically rewrite an IFC compressed GlobalId."""
    return ifcopenshell.guid.compress(
        uuid.uuid5(_GUID_SCRAMBLE_NAMESPACE, guid).hex,
    )


BASE_TIMESTAMP = "2020-11-30T10:34:18"
V2_TIMESTAMP = "2020-11-30T11:05:00"   # ~30 min later
V3_TIMESTAMP = "2020-11-30T11:38:00"   # ~1 hour later


def _set_timestamp(path, timestamp):
    """Set the IFC header timestamp on a file."""
    ifc = ifcopenshell.open(path)
    ifc.header.file_name.time_stamp = timestamp
    ifc.write(path)


def generate_v1():
    """V1: exact copy of BasicHouse."""
    shutil.copy(SRC, V1)
    _set_timestamp(V1, BASE_TIMESTAMP)
    print(f"Created {V1} (copy of {SRC})")


def generate_v2():
    """V2: Renovation phase.

    Changes from v1:
    - Remove 3 windows from south exterior wall (simplify facade)
    - Move 6 bedroom furniture items 2m east (rearrange bedroom)
    - Add IsExternal property to a door that's missing it
    """
    shutil.copy(SRC, V2)
    ifc = ifcopenshell.open(V2)

    print(f"\nGenerating {V2}:")

    # 1. Remove 3 windows from the south exterior wall (qBm)
    #    Windows: 2DedXznHnDaeAWsrTB_q8F, q8E, q8D
    print("\n  Removing 3 south-facing windows:")
    for wguid in [
        "2DedXznHnDaeAWsrTB_q8F",
        "2DedXznHnDaeAWsrTB_q8E",
        "2DedXznHnDaeAWsrTB_q8D",
    ]:
        w = _find_by_guid(ifc, wguid)
        _remove_element_and_relationships(ifc, w)

    # 2. Move bedroom furniture 2m east (bulk movement test)
    #    First bedroom furniture: bed, cabinet, desk, chair, dressers
    print("\n  Moving bedroom furniture 2m east:")
    bedroom_guids = [
        "2DedXznHnDaeAWsrTB_qBW",  # Bed-Standard
        "2DedXznHnDaeAWsrTB_q8V",  # Cabinet-File
        "2DedXznHnDaeAWsrTB_q8U",  # Desk
        "2DedXznHnDaeAWsrTB_q8T",  # Chair-Executive
        "2DedXznHnDaeAWsrTB_q8S",  # Dresser
        "2DedXznHnDaeAWsrTB_q8R",  # Dresser
    ]
    for guid in bedroom_guids:
        el = _find_by_guid(ifc, guid)
        _move_element(ifc, el, dx=2000)  # 2m east
        print(f"  Moved: {guid} ({el.Name})")

    # 3. Modify a property: change a door's fire rating
    print("\n  Modifying door property:")
    door = _find_by_guid(ifc, "2DedXznHnDaeAWsrTB_qBb")  # First interior door
    for rel in ifc.by_type("IfcRelDefinesByProperties"):
        if door in rel.RelatedObjects:
            pset = rel.RelatingPropertyDefinition
            if hasattr(pset, "HasProperties"):
                for prop in pset.HasProperties:
                    if prop.Name == "FireRating":
                        old_val = prop.NominalValue.wrappedValue if prop.NominalValue else None
                        prop.NominalValue = ifc.create_entity(
                            "IfcLabel", "EI 30"
                        )
                        print(f"  Changed FireRating: {old_val} → EI 30")

    ifc.write(V2)
    _set_timestamp(V2, V2_TIMESTAMP)
    print(f"\n  Written {V2}")

    # Verify
    ifc2 = ifcopenshell.open(V2)
    print(f"  Verification: {len(ifc2.by_type('IfcWindow'))} windows"
          f" (was 19, expected 16)")


def generate_v3():
    """V3: Further modifications on top of v2.

    Changes from v2:
    - Remove interior wall (qBg, Innervägg at -1181,1952) and its contents
    - Move the front exterior door (qBY → q8y) 1.5m south
    - Remove 5 furniture items from living room (test deletion)
    """
    shutil.copy(V2, V3)
    ifc = ifcopenshell.open(V3)

    print(f"\nGenerating {V3}:")

    # 1. Remove interior wall (open up floor plan)
    print("\n  Removing interior wall (open floor plan):")
    wall_guid = "2DedXznHnDaeAWsrTB_qBg"  # Innervägg at (-1181, 1952)
    wall = _find_by_guid(ifc, wall_guid)
    _remove_element_and_relationships(ifc, wall)

    # 2. Move exterior door 1.5m south
    print("\n  Moving exterior door 1.5m south:")
    door_guid = "2DedXznHnDaeAWsrTB_q8y"  # Ytterdörr
    door = _find_by_guid(ifc, door_guid)
    _move_element(ifc, door, dy=-1500)  # 1.5m south
    print(f"  Moved: {door_guid} ({door.Name})")

    # 3. Remove 5 living room furniture items
    print("\n  Removing living room furniture:")
    living_room_guids = [
        "2DedXznHnDaeAWsrTB_q8M",  # Bed-Box (queen)
        "2DedXznHnDaeAWsrTB_q8L",  # Cabinet-File
        "2DedXznHnDaeAWsrTB_q8K",  # Cabinet-File
        "2DedXznHnDaeAWsrTB_q8J",  # Dresser
        "2DedXznHnDaeAWsrTB_q8I",  # Another item
    ]
    for guid in living_room_guids:
        try:
            el = _find_by_guid(ifc, guid)
            _remove_element_and_relationships(ifc, el)
        except RuntimeError:
            print(f"  Warning: {guid} not found, skipping")

    ifc.write(V3)
    _set_timestamp(V3, V3_TIMESTAMP)
    print(f"\n  Written {V3}")

    # Verify
    ifc2 = ifcopenshell.open(V3)
    print(f"  Verification: {len(ifc2.by_type('IfcWallStandardCase'))} walls"
          f" (was 13, expected 12)")
    print(f"  Verification: {len(ifc2.by_type('IfcFurnishingElement'))} furniture"
          f" (was 71, expected 66)")


def generate_v2_scrambled():
    """Generate a GUID-scrambled variant of v2 for worst-case matching benchmarks."""
    shutil.copy(V2, V2_SCRAMBLED)
    ifc = ifcopenshell.open(V2_SCRAMBLED)

    print(f"\nGenerating {V2_SCRAMBLED}:")
    rewritten = 0
    for entity in ifc:
        if not hasattr(entity, "GlobalId"):
            continue
        guid = entity.GlobalId
        if not isinstance(guid, str) or guid.strip() == "":
            continue
        entity.GlobalId = _scramble_guid(guid)
        rewritten += 1

    ifc.write(V2_SCRAMBLED)
    _set_timestamp(V2_SCRAMBLED, V2_TIMESTAMP)
    print(f"  Rewrote {rewritten} GlobalIds")
    print(f"  Written {V2_SCRAMBLED}")


def main():
    print("Generating BasicHouse versions for diff testing\n")
    generate_v1()
    generate_v2()
    generate_v2_scrambled()
    generate_v3()
    print("\n✓ All versions generated:")
    print(f"  {V1} — Original")
    print(f"  {V2} — Renovation (windows removed, furniture moved, door property changed)")
    print(f"  {V2_SCRAMBLED} — Renovation with deterministically scrambled GlobalIds")
    print(f"  {V3} — Further changes (wall removed, door moved, furniture deleted)")


if __name__ == "__main__":
    main()
