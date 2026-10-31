"""Mutation-harness behaviour on edge-case seeds."""

from __future__ import annotations

import ifcopenshell

from athar_dev import ifc_mutations as mutations


def test_pset_edit_skips_logical_values_that_read_as_strings() -> None:
    f = ifcopenshell.file(schema="IFC4")
    proxy = f.create_entity("IfcBuildingElementProxy", GlobalId="0proxy0proxy0proxy0pr1")
    pset = f.create_entity(
        "IfcPropertySet",
        GlobalId="0pset00pset00pset00ps1",
        Name="Pset_Test",
        HasProperties=[
            f.create_entity("IfcPropertySingleValue", Name="Flag", NominalValue=f.create_entity("IfcLogical", "UNKNOWN")),
            f.create_entity("IfcPropertySingleValue", Name="Note", NominalValue=f.create_entity("IfcLabel", "a")),
        ],
    )
    f.create_entity(
        "IfcRelDefinesByProperties",
        GlobalId="0rel000rel000rel000re1",
        RelatedObjects=[proxy],
        RelatingPropertyDefinition=pset,
    )

    mutation = mutations.edit_pset_value(f)

    assert mutation.victim_guid == proxy.GlobalId
    flag, note = pset.HasProperties
    assert flag.NominalValue.wrappedValue == "UNKNOWN"
    assert note.NominalValue.wrappedValue == "a [athar-edited]"
