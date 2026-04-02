"""Schema-aware IFC parser for the Phase 1 bottom layer."""

from __future__ import annotations

import math
import unicodedata
from typing import Any
import json

import ifcopenshell
from ifcopenshell.util import schema as schema_util

from .constants import SUPPORTED_SCHEMA_PREFIXES
from .index import build_step_index
from .link_inversion import invert_entity_refs
from .schema_dict import canonical_class_name
from .types import EntityRef, ParseDiagnostics, ParseResult, ParsedEntity

_LENGTH_MEASURES = {
    "IFCLENGTHMEASURE",
    "IFCPOSITIVELENGTHMEASURE",
    "IFCNONNEGATIVELENGTHMEASURE",
}
_ANGLE_MEASURES = {
    "IFCPLANEANGLEMEASURE",
    "IFCPOSITIVEPLANEANGLEMEASURE",
}
_AREA_MEASURES = {"IFCAREAMEASURE"}
_VOLUME_MEASURES = {"IFCVOLUMEMEASURE"}
_DEFAULT_SCALE = 1_000_000
_DIRECTION_SCALE = 100_000
_NULL_TOKEN = "\x00"
_DERIVED_TOKEN = "\x01"


def parse_ifc(filepath: str) -> ParseResult:
    """Parse IFC into deterministic entities + refs + reverse refs."""
    ifc = ifcopenshell.open(filepath)
    schema = str(ifc.schema or "")
    _assert_supported_schema(schema)

    unit_context = _extract_unit_context(ifc)
    entities: dict[int, ParsedEntity] = {}
    diagnostics = ParseDiagnostics()

    for ent in ifc:
        parsed = _extract_entity(ent, unit_context=unit_context)
        entities[parsed.step_id] = parsed

    incoming = invert_entity_refs(entities)
    diagnostics.dangling_refs = sum(
        1
        for e in entities.values()
        for ref in e.refs
        if ref.target_step not in entities
    )

    return ParseResult(
        filepath=filepath,
        schema=schema,
        index=build_step_index(filepath),
        entities=entities,
        incoming_refs=incoming,
        unit_context=unit_context,
        diagnostics=diagnostics,
    )


def _assert_supported_schema(schema: str) -> None:
    if not any(schema.startswith(prefix) for prefix in SUPPORTED_SCHEMA_PREFIXES):
        allowed = ", ".join(SUPPORTED_SCHEMA_PREFIXES)
        raise ValueError(f"Unsupported schema: {schema!r}. Phase 1 supports {allowed}.")


def _extract_entity(ent, *, unit_context: dict[str, Any]) -> ParsedEntity:
    decl = schema_util.get_declaration(ent)
    attrs: dict[str, Any] = {}
    refs: list[EntityRef] = []

    for i in range(len(ent)):
        attr_decl = decl.attribute_by_index(i)
        attr_name = attr_decl.name()
        attr_type = attr_decl.type_of_attribute()
        attrs[attr_name] = _canonicalize_value(
            source_step=ent.id(),
            source_type=ent.is_a(),
            value=ent[i],
            attr_type=attr_type,
            attr_name=attr_name,
            path=f"/{attr_name}",
            refs=refs,
            unit_context=unit_context,
        )

    return ParsedEntity(
        step_id=ent.id(),
        entity_type=ent.is_a(),
        canonical_class=canonical_class_name(ent.is_a()),
        global_id=getattr(ent, "GlobalId", None),
        attributes=attrs,
        refs=refs,
        is_product=_entity_is_a(ent, "IfcProduct"),
        is_spatial=_is_spatial_entity(ent),
    )


def _canonicalize_value(
    *,
    source_step: int,
    source_type: str,
    value: Any,
    attr_type,
    attr_name: str,
    path: str,
    refs: list[EntityRef],
    unit_context: dict[str, Any],
) -> Any:
    if value is None:
        return {"kind": "null", "token": _NULL_TOKEN}
    if value == "*":
        return {"kind": "derived", "token": _DERIVED_TOKEN}

    base_type = _unwrap_named_type(attr_type)
    measure_type = _measure_type_from_attr_type(base_type)

    if _is_select_type(base_type):
        branch_name = _select_branch_name(value)
        branch_type = _select_branch_type(base_type, branch_name)
        resolved_type = branch_type if branch_type is not None else base_type
        if resolved_type is base_type:
            inner = _canonicalize_untyped_value(
                source_step=source_step,
                source_type=source_type,
                value=value,
                attr_name=attr_name,
                path=f"{path}@{branch_name}" if branch_name else path,
                refs=refs,
                unit_context=unit_context,
            )
        else:
            inner = _canonicalize_value(
                source_step=source_step,
                source_type=source_type,
                value=value,
                attr_type=resolved_type,
                attr_name=attr_name,
                path=f"{path}@{branch_name}" if branch_name else path,
                refs=refs,
                unit_context=unit_context,
            )
        return {"kind": "select", "type": branch_name, "value": inner}

    if _is_aggregation_type(base_type):
        item_type = base_type.type_of_element()
        kind = base_type.type_of_aggregation_string()
        items = [
            _canonicalize_value(
                source_step=source_step,
                source_type=source_type,
                value=item,
                attr_type=item_type,
                attr_name=attr_name,
                path=f"{path}/{idx}",
                refs=refs,
                unit_context=unit_context,
            )
            for idx, item in enumerate(value)
        ]
        if kind in {"set", "bag"}:
            items.sort(key=_canonical_sort_key)
        return {"kind": kind, "items": items}

    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return {
                "kind": "simple",
                "type": value.is_a(),
                "value": _canonicalize_scalar(
                    _wrapper_value(value),
                    measure_type=value.is_a(),
                    attr_name=attr_name,
                    unit_context=unit_context,
                ),
            }
        refs.append(
            EntityRef(
                source_step=source_step,
                target_step=value.id(),
                source_type=source_type,
                target_type=value.is_a(),
                attr_name=attr_name,
                path=path,
            )
        )
        # STEP id is intentionally excluded for determinism and renumber invariance.
        return {"kind": "ref"}

    return _canonicalize_scalar(
        value,
        measure_type=measure_type,
        attr_name=attr_name,
        unit_context=unit_context,
    )


def _canonicalize_scalar(
    value: Any,
    *,
    measure_type: str | None,
    attr_name: str,
    unit_context: dict[str, Any],
) -> Any:
    if value is None:
        return {"kind": "null", "token": _NULL_TOKEN}
    if isinstance(value, bool):
        return {"kind": "bool", "value": value}
    if isinstance(value, int):
        return {"kind": "int", "value": value}
    if isinstance(value, float):
        quantized = _quantize_real(value, measure_type=measure_type, attr_name=attr_name, unit_context=unit_context)
        return {"kind": "real_q", "value": quantized}
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value.strip())
        upper = normalized.upper()
        # Limit string->bool coercion to explicit STEP logical/boolean tokens.
        # Plain string literals like "0"/"1"/"TRUE"/"FALSE" must remain strings.
        if upper in {".TRUE.", ".T."}:
            return {"kind": "bool", "value": True}
        if upper in {".FALSE.", ".F."}:
            return {"kind": "bool", "value": False}
        return {"kind": "string", "value": normalized}
    return {"kind": "string", "value": unicodedata.normalize("NFC", str(value))}


def _is_spatial_entity(ent: Any) -> bool:
    """Detect spatial roots across IFC4 and IFC2X3 schema families."""
    return _entity_is_a(ent, "IfcSpatialElement") or _entity_is_a(ent, "IfcSpatialStructureElement")


def _entity_is_a(ent: Any, type_name: str) -> bool:
    try:
        return bool(ent.is_a(type_name))
    except Exception:
        return False


def _canonicalize_untyped_value(
    *,
    source_step: int,
    source_type: str,
    value: Any,
    attr_name: str,
    path: str,
    refs: list[EntityRef],
    unit_context: dict[str, Any],
) -> Any:
    if value is None:
        return {"kind": "null", "token": _NULL_TOKEN}
    if isinstance(value, (list, tuple)):
        return {
            "kind": "list",
            "items": [
                _canonicalize_untyped_value(
                    source_step=source_step,
                    source_type=source_type,
                    value=item,
                    attr_name=attr_name,
                    path=f"{path}/{idx}",
                    refs=refs,
                    unit_context=unit_context,
                )
                for idx, item in enumerate(value)
            ],
        }
    if _is_ifc_entity(value):
        if _is_simple_wrapper(value):
            return {
                "kind": "simple",
                "type": value.is_a(),
                "value": _canonicalize_scalar(
                    _wrapper_value(value),
                    measure_type=value.is_a(),
                    attr_name=attr_name,
                    unit_context=unit_context,
                ),
            }
        refs.append(
            EntityRef(
                source_step=source_step,
                target_step=value.id(),
                source_type=source_type,
                target_type=value.is_a(),
                attr_name=attr_name,
                path=path,
            )
        )
        return {"kind": "ref"}
    return _canonicalize_scalar(
        value,
        measure_type=None,
        attr_name=attr_name,
        unit_context=unit_context,
    )


def _quantize_real(
    value: float,
    *,
    measure_type: str | None,
    attr_name: str,
    unit_context: dict[str, Any],
) -> int:
    if not math.isfinite(value):
        return 0

    measure = (measure_type or "").upper()
    scale = _DEFAULT_SCALE
    unit_type = None
    if measure in _LENGTH_MEASURES:
        unit_type = "LENGTHUNIT"
        scale = 1_000_000
    elif measure in _AREA_MEASURES:
        unit_type = "AREAUNIT"
        scale = 1_000_000
    elif measure in _VOLUME_MEASURES:
        unit_type = "VOLUMEUNIT"
        scale = 1_000_000
    elif measure in _ANGLE_MEASURES:
        unit_type = "PLANEANGLEUNIT"
        scale = 1_000_000
    elif "DIRECTION" in attr_name.upper():
        scale = _DIRECTION_SCALE

    converted = value * _unit_scale_for_type(unit_type, unit_context)
    # Python round() is banker's rounding (half-to-even).
    return int(round(converted * scale))


def _unit_scale_for_type(unit_type: str | None, unit_context: dict[str, Any]) -> float:
    if not unit_type:
        return 1.0
    factors = unit_context.get("unit_factors")
    if not isinstance(factors, dict):
        return 1.0
    value = factors.get(unit_type)
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    return 1.0


def _canonical_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _is_ifc_entity(value: Any) -> bool:
    return hasattr(value, "is_a") and callable(value.is_a)


def _is_simple_wrapper(value: Any) -> bool:
    if not _is_ifc_entity(value):
        return False
    if len(value) != 1:
        return False
    try:
        return value.attribute_name(0) == "wrappedValue"
    except Exception:
        return False


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
        marker = id(next_type)
        if marker in seen:
            break
        seen.add(marker)
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
            if candidate.name() == branch_name:
                return candidate
        except Exception:
            continue
    return None


def _measure_type_from_attr_type(attr_type) -> str | None:
    if attr_type is None:
        return None
    if _is_aggregation_type(attr_type):
        try:
            return _measure_type_from_attr_type(attr_type.type_of_element())
        except Exception:
            return None
    if _is_select_type(attr_type):
        return None

    current = attr_type
    seen: set[int] = set()
    while current is not None:
        name = _safe_type_name(current)
        if isinstance(name, str) and name.upper().endswith("MEASURE"):
            return name
        if not hasattr(current, "declared_type"):
            break
        try:
            next_type = current.declared_type()
        except Exception:
            break
        if next_type is None or next_type is current:
            break
        marker = id(next_type)
        if marker in seen:
            break
        seen.add(marker)
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
    return {"unit_factors": {k: unit_factors[k] for k in sorted(unit_factors)}}


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
        return base if value is None else value * base
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
