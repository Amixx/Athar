"""Full-graph IFC parser for the graph diff engine.

This extracts every instance with explicit attributes, plus a typed
edge list labeled by canonical JSON-Pointer-like paths.
"""

from __future__ import annotations

import math
from typing import Any

import ifcopenshell
from ifcopenshell.util import schema as schema_util

from .canonical_values import (
    PROFILE_RAW_EXACT,
    canonical_scalar,
    canonical_simple,
    canonical_string,
)
from .profile_policy import validate_profile


def parse_graph(filepath: str, *, profile: str = PROFILE_RAW_EXACT) -> dict:
    """Parse a full IFC model into graph-friendly IR."""
    validate_profile(profile)
    ifc = ifcopenshell.open(filepath)
    unit_context = _extract_unit_context(ifc)
    entities: dict[int, dict] = {}

    for ent in ifc:
        entities[ent.id()] = _extract_entity(
            ent,
            profile=profile,
            unit_context=unit_context,
        )

    diagnostics = _collect_diagnostics(entities)
    return {
        "metadata": {
            "schema": ifc.schema,
            "timestamp": ifc.header.file_name.time_stamp or None,
            "diagnostics": diagnostics,
            "units": unit_context,
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


def _extract_entity(ent, *, profile: str, unit_context: dict[str, Any]) -> dict:
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
            unit_context=unit_context,
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
    unit_context: dict[str, Any],
    measure_type: str | None = None,
) -> dict[str, Any]:
    if value is None:
        return {"kind": "null"}

    base_type = _unwrap_named_type(attr_type)
    resolved_measure_type = measure_type or _measure_type_from_attr_type(base_type)

    if _is_select_type(base_type):
        branch_name = _select_branch_name(value)
        select_path = f"{path}@{branch_name}" if branch_name else path
        branch_type = _select_branch_type(base_type, branch_name)
        if branch_type is None:
            inner = _canonicalize_untyped(
                value,
                path=select_path,
                refs=refs,
                profile=profile,
                unit_context=unit_context,
            )
        else:
            inner = _canonicalize_value(
                value,
                branch_type,
                path=select_path,
                refs=refs,
                profile=profile,
                unit_context=unit_context,
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
                    unit_context=unit_context,
                    measure_type=_measure_type_from_attr_type(item_type),
                )
            )
        agg_kind = base_type.type_of_aggregation_string()
        if agg_kind in {"set", "bag"}:
            items.sort(key=canonical_string)
        return {"kind": agg_kind, "items": items}

    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return canonical_simple(
                value.is_a(),
                _wrapper_value(value),
                profile=profile,
                measure_type=value.is_a(),
                unit_context=unit_context,
            )
        refs.append({
            "path": path,
            "target": value.id(),
            "target_type": value.is_a(),
        })
        return {"kind": "ref", "id": value.id()}

    return canonical_scalar(
        value,
        profile=profile,
        measure_type=resolved_measure_type,
        unit_context=unit_context,
    )


def _canonicalize_untyped(
    value: Any,
    *,
    path: str,
    refs: list[dict[str, Any]],
    profile: str,
    unit_context: dict[str, Any],
) -> dict[str, Any]:
    if value is None:
        return {"kind": "null"}
    if isinstance(value, (list, tuple)):
        items = [
            _canonicalize_untyped(
                item,
                path=f"{path}/{idx}",
                refs=refs,
                profile=profile,
                unit_context=unit_context,
            )
            for idx, item in enumerate(value)
        ]
        return {"kind": "list", "items": items}
    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return canonical_simple(
                value.is_a(),
                _wrapper_value(value),
                profile=profile,
                measure_type=value.is_a(),
                unit_context=unit_context,
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


def _measure_type_from_attr_type(attr_type) -> str | None:
    if attr_type is None:
        return None
    current = attr_type
    if _is_aggregation_type(current):
        try:
            return _measure_type_from_attr_type(current.type_of_element())
        except Exception:
            return None
    if _is_select_type(current):
        return None

    seen: set[int] = set()
    while current is not None:
        name = _safe_type_name(current)
        if isinstance(name, str) and name.endswith("Measure"):
            return name
        if not hasattr(current, "declared_type"):
            break
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
    return None


def _safe_type_name(attr_type) -> str | None:
    if not hasattr(attr_type, "name"):
        return None
    try:
        name = attr_type.name()
    except Exception:
        return None
    return name if isinstance(name, str) and name else None


def _extract_unit_context(ifc) -> dict[str, Any]:
    unit_factors: dict[str, float] = {}
    assignments = ifc.by_type("IfcUnitAssignment")
    if not assignments:
        return {"unit_factors": unit_factors}
    for unit in assignments[0].Units:
        unit_type = getattr(unit, "UnitType", None)
        if not isinstance(unit_type, str) or not unit_type:
            continue
        factor = _unit_factor(unit)
        if not (isinstance(factor, (int, float)) and math.isfinite(float(factor)) and float(factor) > 0):
            continue
        unit_factors[unit_type] = float(factor)
    return {
        "unit_factors": {key: unit_factors[key] for key in sorted(unit_factors)},
    }


def _unit_factor(unit) -> float:
    if unit is None or not hasattr(unit, "is_a"):
        return 1.0
    if unit.is_a("IfcSIUnit"):
        return _si_unit_factor(unit)
    if unit.is_a("IfcConversionBasedUnit") or unit.is_a("IfcConversionBasedUnitWithOffset"):
        conversion = getattr(unit, "ConversionFactor", None)
        if conversion is None:
            return 1.0
        base = _unit_factor(getattr(conversion, "UnitComponent", None))
        value = _float_value_component(getattr(conversion, "ValueComponent", None))
        if value is None:
            return base
        return value * base
    if unit.is_a("IfcDerivedUnit"):
        factor = 1.0
        for element in getattr(unit, "Elements", []) or []:
            unit_factor = _unit_factor(getattr(element, "Unit", None))
            exponent = int(getattr(element, "Exponent", 0) or 0)
            factor *= unit_factor ** exponent
        return factor
    return 1.0


def _si_unit_factor(unit) -> float:
    prefix_factor = _prefix_factor(getattr(unit, "Prefix", None))
    unit_type = getattr(unit, "UnitType", None)
    if unit_type == "AREAUNIT":
        return prefix_factor ** 2
    if unit_type == "VOLUMEUNIT":
        return prefix_factor ** 3
    return prefix_factor


def _prefix_factor(prefix: str | None) -> float:
    if prefix is None:
        return 1.0
    factors = {
        "ATTO": 1e-18,
        "FEMTO": 1e-15,
        "PICO": 1e-12,
        "NANO": 1e-9,
        "MICRO": 1e-6,
        "MILLI": 1e-3,
        "CENTI": 1e-2,
        "DECI": 1e-1,
        "DECA": 1e1,
        "HECTO": 1e2,
        "KILO": 1e3,
        "MEGA": 1e6,
        "GIGA": 1e9,
        "TERA": 1e12,
    }
    return factors.get(str(prefix), 1.0)


def _float_value_component(value_component) -> float | None:
    if value_component is None:
        return None
    wrapped = getattr(value_component, "wrappedValue", None)
    if isinstance(wrapped, (int, float)):
        return float(wrapped)
    if isinstance(value_component, (int, float)):
        return float(value_component)
    try:
        return float(value_component)
    except Exception:
        return None
