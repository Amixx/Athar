"""Schema and unit introspection for the native bottom layer.

File-independent schema glue the native parser needs from ifcopenshell:

- ``require_readable_file`` — rejects a path that is not a readable regular
  file.
- ``open_ifc`` — the one place ifcopenshell opens a file: checks the path with
  ``require_readable_file`` and turns parse failures into an ``IfcParseError``
  that names the file and the cause.
- ``_assert_supported_schema`` — guard the engine's supported schema families.
- the measure-type helpers (``_measure_type_from_attr_type`` and the measure
  sets), consumed by ``native_schema`` to precompute per-attribute descriptors.
- ``_extract_unit_context`` — resolve ``IfcUnitAssignment`` factors so Rust can
  unit-normalize quantized reals.

Per-entity parsing and canonicalization live in Rust (``athar/_native``); Python
never materializes the file's entities.
"""

from __future__ import annotations

import errno
import math
import os
import re
import stat
from functools import lru_cache
from typing import Any

from .constants import SUPPORTED_SCHEMAS

_MAX_TYPE_CHAIN_HOPS = 32
_MAX_IFCOPENSHELL_ID = 2**31 - 1
_LOG_ERROR = re.compile(r"^\[error\] \[[^\]]*\] (.*)$", re.MULTILINE)
_INVALID_ID_TOKEN = re.compile(r"^token (\d+) at offset (\d+) invalid instance name$")


_MAX_CAUSES = 5
_MAX_RAW_LOG_CHARS = 2000


class IfcParseError(ValueError):
    """An IFC file ifcopenshell cannot parse."""


def require_readable_file(path: str | os.PathLike[str]) -> None:
    """Raise an ``OSError`` naming ``path`` unless it is a regular file this process can read.

    ifcopenshell crashes the interpreter on a file it cannot open, and opening
    a FIFO blocks, so the check runs before anything opens the path.
    """
    mode = os.stat(path).st_mode
    if stat.S_ISDIR(mode):
        raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)
    if not stat.S_ISREG(mode):
        raise OSError(errno.EINVAL, "Not a regular file", path)
    with open(path, "rb"):
        pass


def open_ifc(path: str):
    """Open ``path`` with ifcopenshell after ``require_readable_file``."""
    import ifcopenshell

    require_readable_file(path)

    # ifcopenshell reports the cause only in its process-wide log, which
    # get_log() drains; drop earlier entries so only this parse's remain.
    ifcopenshell.get_log()
    try:
        return ifcopenshell.open(path)
    except ifcopenshell.Error as exc:
        log = ifcopenshell.get_log()
        cause = _parse_failure_cause(log) or f"{exc}: {log.strip()[:_MAX_RAW_LOG_CHARS] or 'no log output'}"
        raise IfcParseError(f"Cannot parse IFC file {path}: {cause}") from exc


def _parse_failure_cause(log: str) -> str:
    causes = []
    for message in _LOG_ERROR.findall(log):
        token = _INVALID_ID_TOKEN.match(message)
        if token and int(token.group(1)) > _MAX_IFCOPENSHELL_ID:
            causes.insert(
                0,
                f"entity id #{token.group(1)} at byte offset {token.group(2)} exceeds "
                f"{_MAX_IFCOPENSHELL_ID}, the largest id ifcopenshell accepts",
            )
        else:
            causes.append(message)
    shown = "; ".join(causes[:_MAX_CAUSES])
    if len(causes) > _MAX_CAUSES:
        shown += f" (+{len(causes) - _MAX_CAUSES} more)"
    return shown


def _assert_supported_schema(schema: str) -> None:
    if schema not in SUPPORTED_SCHEMAS:
        allowed = ", ".join(SUPPORTED_SCHEMAS)
        raise ValueError(f"Unsupported schema: {schema!r}. Current engine supports {allowed}.")


def _is_select_type(attr_type) -> bool:
    return hasattr(attr_type, "select_list")


def _is_aggregation_type(attr_type) -> bool:
    return hasattr(attr_type, "type_of_aggregation_string")


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
    for _ in range(_MAX_TYPE_CHAIN_HOPS):
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
        current = next_type
    return None


def measure_unit_type(schema_name: str, measure: str) -> str | None:
    """The unit type of the file unit that scales ``measure`` (``IfcThermalTransmittanceMeasure`` ->
    ``THERMALTRANSMITTANCEUNIT``), or ``None`` for a measure without a unit type in the schema."""
    import ifcopenshell.util.unit as units

    unit_type = units.get_measure_unit_type(measure)
    return unit_type if unit_type in _unit_types(schema_name) else None


@lru_cache(maxsize=8)
def _unit_types(schema_name: str) -> frozenset[str]:
    from ifcopenshell import ifcopenshell_wrapper

    schema = ifcopenshell_wrapper.schema_by_name(schema_name)
    names = (schema.declaration_by_name(enum).enumeration_items() for enum in ("IfcUnitEnum", "IfcDerivedUnitEnum"))
    return frozenset(name for items in names for name in items) - {"USERDEFINED"}


def _safe_type_name(attr_type) -> str | None:
    if not hasattr(attr_type, "name"):
        return None
    try:
        name = attr_type.name()
    except Exception:
        return None
    return name if isinstance(name, str) and name else None


def _extract_unit_context(ifc) -> dict[str, Any]:
    """SI factors per unit type of the file's unit assignment.

    Both mappings contain only units with a finite positive conversion factor.
    Native factors normalize every measure with a unit type; property factors
    label supported property measures in SI.
    """
    unit_factors: dict[str, float] = {}
    property_unit_factors: dict[str, float] = {}
    assignments = ifc.by_type("IfcUnitAssignment")
    if assignments:
        for unit in assignments[0].Units:
            unit_type = getattr(unit, "UnitType", None)
            if not isinstance(unit_type, str) or not unit_type:
                continue
            factor = unit_factor(unit)
            if factor is None:
                unit_factors.pop(unit_type, None)
                property_unit_factors.pop(unit_type, None)
                continue
            unit_factors[unit_type] = factor
            property_unit_factors[unit_type] = factor
    return {
        "unit_factors": {k: unit_factors[k] for k in sorted(unit_factors)},
        "property_unit_factors": {k: property_unit_factors[k] for k in sorted(property_unit_factors)},
        "record_unit_factors": _map_conversion_unit_factors(ifc),
    }


def _map_conversion_unit_factors(ifc) -> dict[int, dict[str, float]]:
    """Length factor per ``IfcMapConversion`` whose target CRS sets ``MapUnit``.

    The conversion's lengths are in that unit; without ``MapUnit`` they use the
    project length unit and the conversion has no entry.
    """
    if ifc.schema == "IFC2X3":
        return {}
    factors: dict[int, dict[str, float]] = {}
    for conversion in ifc.by_type("IfcMapConversion"):
        crs = conversion.TargetCRS
        map_unit = crs.MapUnit if crs is not None and crs.is_a("IfcProjectedCRS") else None
        if map_unit is None:
            continue
        step_id = conversion.id()
        if getattr(map_unit, "UnitType", None) != "LENGTHUNIT":
            raise ValueError(f"#{step_id} IfcMapConversion: TargetCRS MapUnit is not a length unit")
        factor = unit_factor(map_unit)
        if factor is None:
            raise ValueError(f"#{step_id} IfcMapConversion: TargetCRS MapUnit has no positive length factor")
        factors[step_id] = {"LENGTHUNIT": factor}
    return factors


def unit_factor(unit) -> float | None:
    """The factor from ``unit`` to SI, mass in kilograms; ``None`` without a multiplicative one."""
    import ifcopenshell.util.unit as units

    pending = [(unit, 1)]
    grams = 0
    while pending:
        component, exponent = pending.pop()
        if component is None:
            return None
        if component.is_a("IfcSIUnit"):
            if component.Prefix is not None and component.Prefix not in units.prefixes:
                return None
            if component.Name == "GRAM":
                grams += exponent
        elif component.is_a("IfcConversionBasedUnit"):
            if component.is_a("IfcConversionBasedUnitWithOffset") and component.ConversionOffset:
                return None
            conversion = component.ConversionFactor
            if conversion is None or conversion.ValueComponent is None:
                return None
            pending.append((conversion.UnitComponent, exponent))
        elif component.is_a("IfcDerivedUnit"):
            pending.extend((element.Unit, exponent * element.Exponent) for element in component.Elements)
        else:
            return None
    # IfcOpenShell scales mass relative to the gram.
    factor = units.get_unit_scale(unit) / 1000.0**grams
    return float(factor) if math.isfinite(factor) and factor > 0 else None
