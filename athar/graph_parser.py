"""Full-graph IFC parser for the graph diff engine.

This extracts every instance with explicit attributes, plus a typed
edge list labeled by canonical JSON-Pointer-like paths.
"""

from __future__ import annotations

from typing import Any

import ifcopenshell
from ifcopenshell.util import schema as schema_util

from .canonical_values import (
    PROFILE_RAW_EXACT,
    canonical_scalar,
    canonical_simple,
    canonical_string,
)


def parse_graph(filepath: str, *, profile: str = PROFILE_RAW_EXACT) -> dict:
    """Parse a full IFC model into graph-friendly IR."""
    ifc = ifcopenshell.open(filepath)
    entities: dict[int, dict] = {}

    for ent in ifc:
        entities[ent.id()] = _extract_entity(ent, profile=profile)

    diagnostics = _collect_diagnostics(entities)
    return {
        "metadata": {
            "schema": ifc.schema,
            "timestamp": ifc.header.file_name.time_stamp or None,
            "diagnostics": diagnostics,
        },
        "entities": entities,
    }


def count_dangling_refs(graph: dict) -> int:
    """Count references whose targets are missing from the entity map."""
    entities = graph.get("entities", {})
    count = 0
    for entity in entities.values():
        for ref in entity.get("refs", []):
            target = ref.get("target")
            if target is not None and target not in entities:
                count += 1
    return count


def _collect_diagnostics(entities: dict[int, dict], *, sample_size: int = 10) -> dict[str, Any]:
    dangling = []
    for step_id, entity in entities.items():
        for ref in entity.get("refs", []):
            target = ref.get("target")
            if target is None or target in entities:
                continue
            dangling.append({
                "source_step": step_id,
                "path": ref.get("path", ""),
                "target_step": target,
                "target_type": ref.get("target_type"),
            })
    dangling.sort(
        key=lambda item: (
            item["source_step"],
            item["path"],
            item["target_step"],
            item.get("target_type") or "",
        )
    )
    return {
        "dangling_refs": len(dangling),
        "dangling_refs_sample": dangling[:sample_size],
    }


def _extract_entity(ent, *, profile: str) -> dict:
    decl = schema_util.get_declaration(ent)
    attributes: dict[str, Any] = {}
    refs: list[dict[str, Any]] = []

    for i in range(len(ent)):
        attr_decl = decl.attribute_by_index(i)
        attr_name = attr_decl.name()
        attr_type = attr_decl.type_of_attribute()
        attr_value = ent[i]
        attributes[attr_name] = _canonicalize_value(
            attr_value,
            attr_type,
            path=f"/{attr_name}",
            refs=refs,
            profile=profile,
        )

    return {
        "step_id": ent.id(),
        "entity_type": ent.is_a(),
        "global_id": getattr(ent, "GlobalId", None),
        "attributes": attributes,
        "refs": refs,
    }


def _canonicalize_value(
    value: Any,
    attr_type,
    *,
    path: str,
    refs: list[dict[str, Any]],
    profile: str,
) -> dict[str, Any]:
    if value is None:
        return {"kind": "null"}

    base_type = _unwrap_named_type(attr_type)

    if _is_select_type(base_type):
        branch_name = _select_branch_name(value)
        select_path = f"{path}@{branch_name}" if branch_name else path
        branch_type = _select_branch_type(base_type, branch_name)
        if branch_type is None:
            inner = _canonicalize_untyped(
                value, path=select_path, refs=refs, profile=profile
            )
        else:
            inner = _canonicalize_value(
                value,
                branch_type,
                path=select_path,
                refs=refs,
                profile=profile,
            )
        return {"kind": "select", "type": branch_name, "value": inner}

    if _is_aggregation_type(base_type):
        items = []
        item_type = base_type.type_of_element()
        for idx, item in enumerate(value):
            items.append(
                _canonicalize_value(
                    item,
                    item_type,
                    path=f"{path}/{idx}",
                    refs=refs,
                    profile=profile,
                )
            )
        agg_kind = base_type.type_of_aggregation_string()
        if agg_kind in {"set", "bag"}:
            items.sort(key=canonical_string)
        return {"kind": agg_kind, "items": items}

    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return canonical_simple(
                value.is_a(), _wrapper_value(value), profile=profile
            )
        refs.append({
            "path": path,
            "target": value.id(),
            "target_type": value.is_a(),
        })
        return {"kind": "ref", "id": value.id()}

    return canonical_scalar(value, profile=profile)


def _canonicalize_untyped(
    value: Any,
    *,
    path: str,
    refs: list[dict[str, Any]],
    profile: str,
) -> dict[str, Any]:
    if value is None:
        return {"kind": "null"}
    if isinstance(value, (list, tuple)):
        items = [
            _canonicalize_untyped(
                item, path=f"{path}/{idx}", refs=refs, profile=profile
            )
            for idx, item in enumerate(value)
        ]
        return {"kind": "list", "items": items}
    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return canonical_simple(
                value.is_a(), _wrapper_value(value), profile=profile
            )
        refs.append({
            "path": path,
            "target": value.id(),
            "target_type": value.is_a(),
        })
        return {"kind": "ref", "id": value.id()}
    return canonical_scalar(value, profile=profile)


def _is_ifc_entity(value: Any) -> bool:
    return hasattr(value, "is_a") and callable(value.is_a)


def _is_simple_wrapper(value: Any) -> bool:
    if not _is_ifc_entity(value):
        return False
    if len(value) != 1:
        return False
    return value.attribute_name(0) == "wrappedValue"


def _wrapper_value(value: Any) -> Any:
    return value[0]


def _unwrap_named_type(attr_type):
    current = attr_type
    seen: set[int] = set()
    while hasattr(current, "declared_type"):
        try:
            next_type = current.declared_type()
        except Exception:
            break
        if next_type is None or next_type is current:
            break
        ident = id(next_type)
        if ident in seen:
            break
        seen.add(ident)
        current = next_type
    return current


def _is_select_type(attr_type) -> bool:
    return hasattr(attr_type, "select_list")


def _is_aggregation_type(attr_type) -> bool:
    return hasattr(attr_type, "type_of_aggregation_string")


def _select_branch_name(value: Any) -> str | None:
    if _is_ifc_entity(value):
        return value.is_a()
    return None


def _select_branch_type(select_type, branch_name: str | None):
    if not branch_name:
        return None
    for candidate in select_type.select_list():
        try:
            name = candidate.name()
        except Exception:
            continue
        if name == branch_name:
            return candidate
    return None
