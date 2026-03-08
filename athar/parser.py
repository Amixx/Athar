"""Extract structured entity data from IFC files."""

from __future__ import annotations

import ifcopenshell
import ifcopenshell.util.placement
import ifcopenshell.util.element


def parse(filepath: str) -> dict:
    """Parse an IFC file into a dict with file metadata and entities.

    Returns:
        {
            "metadata": { schema, timestamp, author, organization,
                          application, originating_system },
            "entities": { guid: entity_dict, ... },
            "relationships": {
                "voids": [ {element_guid, opening_guid}, ... ],
                "fills": [ {opening_guid, element_guid}, ... ],
                "aggregates": [ {parent_guid, child_guids}, ... ],
                "spatial_children": { container_guid: [child_guids] },
            },
        }
    """
    ifc = ifcopenshell.open(filepath)

    # Build guid → group names mapping from IfcRelAssignsToGroup
    group_map = _build_group_map(ifc)

    entities = {}
    for element in ifc.by_type("IfcProduct"):
        guid = element.GlobalId
        entities[guid] = _extract_element(element, group_map.get(guid, []))

    return {
        "metadata": _extract_metadata(ifc),
        "entities": entities,
        "relationships": _extract_relationships(ifc),
    }


def _extract_relationships(ifc) -> dict:
    """Extract structural relationships between entities.

    Returns hosting (voids/fills), aggregation, and spatial containment
    relationships needed for scene model construction.
    """
    # IfcRelVoidsElement: wall → opening
    voids = []
    for rel in ifc.by_type("IfcRelVoidsElement"):
        element = rel.RelatingBuildingElement
        opening = rel.RelatedOpeningElement
        if hasattr(element, "GlobalId") and hasattr(opening, "GlobalId"):
            voids.append({
                "element_guid": element.GlobalId,
                "opening_guid": opening.GlobalId,
            })

    # IfcRelFillsElement: opening → door/window
    fills = []
    for rel in ifc.by_type("IfcRelFillsElement"):
        opening = rel.RelatingOpeningElement
        filler = rel.RelatedBuildingElement
        if hasattr(opening, "GlobalId") and hasattr(filler, "GlobalId"):
            fills.append({
                "opening_guid": opening.GlobalId,
                "element_guid": filler.GlobalId,
            })

    # IfcRelAggregates: parent → children (e.g. stair → flights + members)
    aggregates = []
    for rel in ifc.by_type("IfcRelAggregates"):
        parent = rel.RelatingObject
        if not hasattr(parent, "GlobalId"):
            continue
        child_guids = [
            c.GlobalId for c in rel.RelatedObjects
            if hasattr(c, "GlobalId")
        ]
        if child_guids:
            aggregates.append({
                "parent_guid": parent.GlobalId,
                "child_guids": child_guids,
            })

    # IfcRelContainedInSpatialStructure: spatial container → elements
    spatial_children: dict[str, list[str]] = {}
    for rel in ifc.by_type("IfcRelContainedInSpatialStructure"):
        container = rel.RelatingStructure
        if not hasattr(container, "GlobalId"):
            continue
        guids = [
            e.GlobalId for e in rel.RelatedElements
            if hasattr(e, "GlobalId")
        ]
        if guids:
            spatial_children.setdefault(container.GlobalId, []).extend(guids)

    return {
        "voids": voids,
        "fills": fills,
        "aggregates": aggregates,
        "spatial_children": spatial_children,
    }


def _extract_metadata(ifc) -> dict:
    """Extract file-level metadata from IFC header and entities."""
    h = ifc.header
    meta = {
        "schema": ifc.schema,
        "timestamp": h.file_name.time_stamp if h.file_name.time_stamp else None,
        "author": _first_or_none(h.file_name.author),
        "organization": _first_or_none(h.file_name.organization),
        "originating_system": h.file_name.originating_system or None,
    }

    # Application info from IfcApplication entities
    apps = ifc.by_type("IfcApplication")
    if apps:
        app = apps[-1]  # last one is typically the editing app
        meta["application"] = f"{app.ApplicationFullName} {app.Version}"
    else:
        meta["application"] = None

    # Author info from IfcPerson (if header author is blank)
    if not meta["author"] or meta["author"] == "":
        persons = ifc.by_type("IfcPerson")
        if persons:
            p = persons[0]
            parts = [p.GivenName, p.FamilyName]
            meta["author"] = " ".join(x for x in parts if x) or None

    # Organization from IfcOrganization (if header org is blank)
    if not meta["organization"] or meta["organization"] == "":
        orgs = ifc.by_type("IfcOrganization")
        if orgs:
            meta["organization"] = orgs[0].Name or None

    return meta


def _first_or_none(val) -> str | None:
    """Extract first element from header tuple fields."""
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        return val[0] if val and val[0] else None
    return val or None


def _build_group_map(ifc) -> dict[str, list[str]]:
    """Build a mapping of entity GlobalId → list of group names."""
    group_map: dict[str, list[str]] = {}
    for rel in ifc.by_type("IfcRelAssignsToGroup"):
        group = rel.RelatingGroup
        name = group.Name
        if not name:
            continue
        # Strip Revit's trailing element ID (e.g. "Model Group:Mockup Buildings NDS:379102")
        parts = name.split(":")
        if len(parts) >= 2 and parts[-1].isdigit():
            name = ":".join(parts[:-1])
        for member in rel.RelatedObjects:
            guid = getattr(member, "GlobalId", None)
            if guid:
                group_map.setdefault(guid, []).append(name)
    return group_map


def _extract_element(element, groups: list[str] | None = None) -> dict:
    return {
        "ifc_class": element.is_a(),
        "name": element.Name,
        "attributes": _extract_attributes(element),
        "property_sets": _extract_psets(element),
        "placement": _extract_placement(element),
        "container": _extract_container(element),
        "type_name": _extract_type(element),
        "owner_history": _extract_owner_history(element),
        "groups": groups or [],
    }


def _extract_attributes(element) -> dict:
    """Extract direct IFC attributes (excluding GlobalId, OwnerHistory)."""
    skip = {"GlobalId", "OwnerHistory", "ObjectPlacement", "Representation"}
    result = {}
    for i, attr in enumerate(element):
        attr_name = element.attribute_name(i)
        if attr_name in skip:
            continue
        result[attr_name] = _simplify(attr)
    return result


def _extract_psets(element) -> dict[str, dict]:
    """Extract all property sets and their properties."""
    psets = ifcopenshell.util.element.get_psets(element)
    # Remove internal 'id' keys that ifcopenshell adds
    cleaned = {}
    for pset_name, props in psets.items():
        cleaned[pset_name] = {k: v for k, v in props.items() if k != "id"}
    return cleaned


def _extract_placement(element) -> list[list[float]] | None:
    """Extract the 4x4 placement matrix as nested lists."""
    if element.ObjectPlacement is None:
        return None
    try:
        matrix = ifcopenshell.util.placement.get_local_placement(
            element.ObjectPlacement
        )
        return matrix.tolist()
    except Exception:
        return None


def _extract_container(element) -> str | None:
    """Get the name of the spatial container (e.g. which storey)."""
    container = ifcopenshell.util.element.get_container(element)
    if container is None:
        return None
    return container.Name


def _extract_type(element) -> str | None:
    """Get the type name assigned to this element."""
    element_type = ifcopenshell.util.element.get_type(element)
    if element_type is None:
        return None
    return element_type.Name


def _extract_owner_history(element) -> dict | None:
    """Extract OwnerHistory: who last modified this element and when."""
    oh = element.OwnerHistory
    if oh is None:
        return None
    result = {
        "change_action": oh.ChangeAction,
        "created": oh.CreationDate,
        "modified": oh.LastModifiedDate,
    }
    if oh.OwningUser:
        person = oh.OwningUser.ThePerson
        org = oh.OwningUser.TheOrganization
        parts = [person.GivenName, person.FamilyName] if person else []
        result["user"] = " ".join(x for x in parts if x) or None
        result["organization"] = org.Name if org else None
    if oh.OwningApplication:
        a = oh.OwningApplication
        result["application"] = f"{a.ApplicationFullName} {a.Version}"
    return result


def _simplify(value):
    """Convert IFC attribute values to JSON-serializable form."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, tuple):
        return [_simplify(v) for v in value]
    if hasattr(value, "is_a"):
        # It's an IFC entity reference — return a lightweight representation
        if hasattr(value, "GlobalId"):
            return {"ref": value.GlobalId}
        return {"entity": value.is_a(), "id": value.id()}
    return str(value)
