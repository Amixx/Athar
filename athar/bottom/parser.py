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
from typing import Any

from .constants import SUPPORTED_SCHEMAS

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


def _safe_type_name(attr_type) -> str | None:
    if not hasattr(attr_type, "name"):
        return None
    try:
        name = attr_type.name()
    except Exception:
        return None
    return name if isinstance(name, str) and name else None


def _extract_unit_context(ifc) -> dict[str, Any]:
    """Factors per unit type of the file's unit assignment.

    ``unit_factors`` covers every unit; ``property_unit_factors`` only the unit
    types whose unit is an ``IfcSIUnit`` or an ``IfcConversionBasedUnit``,
    the ones a value can be converted from to SI.
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
            if not (isinstance(factor, (int, float)) and math.isfinite(float(factor)) and float(factor) > 0):
                continue
            unit_factors[unit_type] = float(factor)
            if unit.is_a("IfcSIUnit") or unit.is_a("IfcConversionBasedUnit"):
                property_unit_factors[unit_type] = float(factor)
            else:
                property_unit_factors.pop(unit_type, None)
    return {
        "unit_factors": {k: unit_factors[k] for k in sorted(unit_factors)},
        "property_unit_factors": {k: property_unit_factors[k] for k in sorted(property_unit_factors)},
    }


def unit_factor(unit) -> float:
    if unit is None or not hasattr(unit, "is_a"):
        return 1.0
    if unit.is_a("IfcSIUnit"):
        return _si_unit_factor(unit)
    if unit.is_a("IfcConversionBasedUnit") or unit.is_a("IfcConversionBasedUnitWithOffset"):
        conversion = getattr(unit, "ConversionFactor", None)
        if conversion is None:
            return 1.0
        base = unit_factor(getattr(conversion, "UnitComponent", None))
        value = _float_value_component(getattr(conversion, "ValueComponent", None))
        return base if value is None else value * base
    if unit.is_a("IfcDerivedUnit"):
        factor = 1.0
        for element in getattr(unit, "Elements", []) or []:
            element_factor = unit_factor(getattr(element, "Unit", None))
            exponent = int(getattr(element, "Exponent", 0) or 0)
            factor *= element_factor ** exponent
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
