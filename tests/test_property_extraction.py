"""Property extraction keeps same-named values from different psets and the type apart."""

from __future__ import annotations

import ifcopenshell
import ifcopenshell.api

from athar.bottom.properties import extract_properties
from athar.bottom.types import PropertyEntry


def test_same_property_name_in_two_psets_and_the_type_are_separate_entries():
    f = ifcopenshell.file(schema="IFC4")
    run = ifcopenshell.api.run
    wall_type = run("root.create_entity", f, ifc_class="IfcWallType", name="WT")
    type_pset = run("pset.add_pset", f, product=wall_type, name="Pset_WallCommon")
    run("pset.edit_pset", f, pset=type_pset, properties={"IsExternal": True})
    wall = run("root.create_entity", f, ifc_class="IfcWall", name="W")
    run("type.assign_type", f, related_objects=[wall], relating_type=wall_type)
    occurrence_pset = run("pset.add_pset", f, product=wall, name="Pset_WallCommon")
    run("pset.edit_pset", f, pset=occurrence_pset, properties={"IsExternal": False, "FireRating": "EI30"})
    custom = run("pset.add_pset", f, product=wall, name="Custom_Fire")
    run("pset.edit_pset", f, pset=custom, properties={"FireRating": "REI60"})

    entries = sorted(extract_properties(f)[wall.id()], key=lambda e: (e.pset, e.name, e.inherited))
    assert entries == [
        PropertyEntry(pset="Custom_Fire", name="FireRating", value="REI60", inherited=False),
        PropertyEntry(pset="Pset_WallCommon", name="FireRating", value="EI30", inherited=False),
        PropertyEntry(pset="Pset_WallCommon", name="IsExternal", value=False, inherited=False),
        PropertyEntry(pset="Pset_WallCommon", name="IsExternal", value=True, inherited=True),
    ]
