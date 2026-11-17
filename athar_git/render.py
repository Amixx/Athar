"""Deterministic renderers for Athar JSON reports."""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from decimal import Decimal
from typing import Callable, Iterable

DEFAULT_MAX_ITEMS = 80
MAX_TEXT_CHARS = 200


def markdown_code(value: object) -> str:
    text = str(value).replace("\n", " ").replace("\r", " ")
    fence = "`" * (1 + max((len(run) for run in re.findall(r"`+", text)), default=0))
    return f"{fence} {text} {fence}" if "`" in text or text.startswith(" ") or text.endswith(" ") else f"{fence}{text}{fence}"


def clip(value: object) -> str:
    text = str(value)
    if len(text) <= MAX_TEXT_CHARS:
        return text
    return f"{text[:MAX_TEXT_CHARS]}...[{len(text) - MAX_TEXT_CHARS:,} more characters]"


def markdown_block(text: str) -> str:
    fence = "`" * max(3, 1 + max((len(run) for run in re.findall(r"`+", text)), default=0))
    return f"{fence}text\n{text}\n{fence}"


def render_text_report(
    report: dict,
    *,
    max_items: int = DEFAULT_MAX_ITEMS,
    max_modified: int | None = None,
    max_cohorts: int | None = None,
    details: bool = True,
) -> str:
    """Render a compact, deterministic terminal summary.

    ``max_items`` caps the deleted and added lists, ``max_modified`` and
    ``max_cohorts`` (both default ``max_items``) the modified list and the
    placement-cohort lines; ``details=False`` drops the per-entity rows under
    each modified entity.
    """

    max_items = max(0, int(max_items))
    max_modified = max_items if max_modified is None else max(0, int(max_modified))
    max_cohorts = max_items if max_cohorts is None else max(0, int(max_cohorts))
    stats = report.get("stats", {})
    schemas = report.get("schemas", {})
    lines: list[str] = [
        "Athar IFC diff",
        f"Schema: {_escape_name(clip(schemas.get('old', '?')))} -> {_escape_name(clip(schemas.get('new', '?')))}",
        _summary_line(stats),
    ]

    scope = stats.get("modified_change_scope") or {}
    if any(scope.get(name, 0) for name in ("intrinsic", "mixed", "transitive")):
        lines.append(
            "Modified scope: "
            + ", ".join(
                f"{name} {scope.get(name, 0)}"
                for name in ("intrinsic", "mixed", "transitive")
                if scope.get(name, 0)
            )
        )

    matcher = (stats.get("matcher_diagnostics") or {}).get("tiers") or {}
    if matcher:
        lines.append(
            "Matches: "
            + ", ".join(f"{name} {matcher[name]}" for name in sorted(matcher) if matcher[name])
        )

    lines.extend(_class_count_lines(report, quote=str))
    orphan_line = _orphan_property_set_line(report, quote=_quoted)
    if orphan_line:
        lines.append(orphan_line)

    cohorts = sorted(
        (cohort for cohort in stats.get("placement_cohorts") or [] if int(cohort.get("count", 0)) >= 2),
        key=lambda cohort: -int(cohort["count"]),
    )
    cohort_members = {member for cohort in cohorts for member in cohort.get("members", [])}
    for cohort in cohorts[:max_cohorts]:
        lines.append(f"{_number(cohort['count'])} elements moved by {_vector_mm(cohort.get('delta_mm'))}")
    hidden_cohorts = cohorts[max_cohorts:]
    if hidden_cohorts:
        moved = sum(int(cohort["count"]) for cohort in hidden_cohorts)
        lines.append(
            f"... and {_number(len(hidden_cohorts))} more placement cohorts ({_number(moved)} elements) not shown"
        )

    for section, marker in (("deleted", "-"), ("added", "+"), ("modified", "~")):
        items = report.get(section, [])
        if section == "modified" and cohort_members:
            items = [
                item for item in items
                if (item.get("new") or {}).get("step_id") not in cohort_members
                or any(item.get("aspects", {}).get(name) == "changed" for name in ("data", "geometry", "topology"))
            ]
        if not items:
            continue
        lines.append("")
        lines.append(f"{section.title()} ({len(items)})")
        shown = items[: max_modified if section == "modified" else max_items]
        hidden_details = 0
        for item in shown:
            head, *rows = _render_item(
                section, marker, item,
                suppress_placement=section == "modified" and (item.get("new") or {}).get("step_id") in cohort_members,
            )
            lines.append(head)
            if details:
                lines.extend(rows)
            else:
                hidden_details += len(rows)
        if hidden_details:
            lines.append(f"... {hidden_details} detail rows not shown")
        hidden = len(items) - len(shown)
        if hidden > 0:
            lines.append(f"... and {hidden} more {section} not shown")

    if stats.get("added", 0) == stats.get("deleted", 0) == stats.get("modified", 0) == 0 and not orphan_line:
        lines.append("")
        lines.append("No semantic entity changes.")

    return "\n".join(lines) + "\n"


def render_markdown_report(
    report: dict,
    *,
    max_items: int = DEFAULT_MAX_ITEMS,
    max_modified: int | None = None,
    max_cohorts: int | None = None,
    details: bool = True,
) -> str:
    """Render a compact Markdown summary suitable for PR comments."""

    stats = report.get("stats", {})
    schemas = report.get("schemas", {})
    lines: list[str] = [
        f"Schema: {markdown_code(clip(schemas.get('old', '?')))} -> {markdown_code(clip(schemas.get('new', '?')))}",
        "",
        "| Added | Deleted | Modified | Unchanged | Old signatures | New signatures |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        "| "
        + " | ".join(
            _number(stats.get(name, 0))
            for name in ("added", "deleted", "modified", "unchanged", "old_signatures", "new_signatures")
        )
        + " |",
    ]

    scope = stats.get("modified_change_scope") or {}
    scope_parts = [
        f"`{name}` {scope.get(name, 0)}"
        for name in ("intrinsic", "mixed", "transitive")
        if scope.get(name, 0)
    ]
    if scope_parts:
        lines.extend(["", "Modified scope: " + ", ".join(scope_parts)])

    matcher = (stats.get("matcher_diagnostics") or {}).get("tiers") or {}
    matcher_parts = [f"`{name}` {matcher[name]}" for name in sorted(matcher) if matcher[name]]
    if matcher_parts:
        lines.append("Matches: " + ", ".join(matcher_parts))

    class_lines = _class_count_lines(report, quote=markdown_code)
    if class_lines:
        lines.append("")
        lines.extend(class_lines)

    orphan_line = _orphan_property_set_line(report, quote=lambda name: markdown_code(clip(name)))
    if orphan_line:
        lines.extend(["", orphan_line])

    changed_total = sum(int(stats.get(name, 0) or 0) for name in ("added", "deleted", "modified"))
    if changed_total == 0:
        if not orphan_line:
            lines.extend(["", "No semantic entity changes."])
    else:
        lines.extend(["", "<details>", "<summary>Changed entities</summary>", ""])
        text = render_text_report(
            report, max_items=max_items, max_modified=max_modified, max_cohorts=max_cohorts, details=details
        ).rstrip()
        lines.append(markdown_block(text))
        lines.extend(["", "</details>"])

    return "\n".join(lines) + "\n"


def _summary_line(stats: dict) -> str:
    return (
        "Summary: "
        f"+{_number(stats.get('added', 0))} "
        f"-{_number(stats.get('deleted', 0))} "
        f"~{_number(stats.get('modified', 0))} "
        f"={_number(stats.get('unchanged', 0))} "
        f"(old {_number(stats.get('old_signatures', 0))}, new {_number(stats.get('new_signatures', 0))})"
    )


def _orphan_property_set_line(report: dict, *, quote: Callable[[str], str]) -> str | None:
    orphans = report.get("orphan_property_sets") or {}
    added = orphans.get("added") or []
    removed = orphans.get("removed") or []
    if not added and not removed:
        return None
    names = Counter(item.get("name") or "?" for item in [*added, *removed])
    top = ", ".join(
        f"{quote(name)} {_number(count)}"
        for name, count in sorted(names.items(), key=lambda pair: (-pair[1], pair[0]))[:5]
    )
    return f"Unattached property sets: +{len(added)} -{len(removed)} ({top})"


def _class_count_lines(report: dict, *, quote: Callable[[str], str]) -> list[str]:
    lines: list[str] = []
    for section in ("modified", "added", "deleted"):
        counts = _class_counts(report.get(section, []), section)
        if counts:
            rendered = ", ".join(f"{quote(name)} {_number(count)}" for name, count in counts)
            lines.append(f"{section.title()} by class: {rendered}")
    return lines


def _class_counts(items: Iterable[dict], section: str) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for item in items:
        if section == "modified":
            entity = item.get("new") or item.get("old") or {}
        else:
            entity = item
        counter[entity.get("class", "?")] += 1
    return sorted(counter.items(), key=lambda pair: (-pair[1], pair[0]))


def _render_item(section: str, marker: str, item: dict, *, suppress_placement: bool = False) -> list[str]:
    if section == "modified":
        old = item.get("old", {})
        new = item.get("new", {})
        aspects = item.get("aspects", {})
        changed = [
            name for name in ("geometry", "data", "topology", "placement")
            if aspects.get(name) == "changed" and not (name == "placement" and suppress_placement)
        ]
        detail = ", ".join(changed) if changed else "no aspect changes"
        placement = None if suppress_placement else _placement_delta(aspects.get("placement_delta_mm"))
        if placement:
            detail = f"{detail}, {placement}"
        match = item.get("match", {})
        reason = match.get("reason", "?")
        score = match.get("score", "?")
        lines = [f"{marker} {_entity_label(new)} [{detail}]"]
        if reason != "guid":
            lines.append(f"  match: {reason} score {score}; old {_entity_ref(old)} -> new {_entity_ref(new)}")
        if placement:
            lines.append(f"  placement vector: {_vector_mm(aspects['placement_delta_mm'])}")
        property_deltas = item.get("property_deltas")
        lines.extend(_property_delta_lines(property_deltas))
        lines.extend(_data_delta_lines(item.get("data_delta"), covered=property_deltas))
        return lines
    return [f"{marker} {_entity_label(item)}"]


def _property_delta_lines(deltas: dict | None) -> list[str]:
    if not deltas:
        return []
    lines: list[str] = []
    for kind, entries in (("added", deltas.get("added")), ("removed", deltas.get("removed")), ("changed", deltas.get("changed"))):
        for entry in entries or []:
            if kind == "changed":
                lines.append(f"  property: {_property_label(entry)}: {_render_value(entry.get('old_value'))} -> {_render_value(entry.get('new_value'))}")
            elif kind == "added":
                lines.append(f"  property added: {_property_label(entry)} = {_render_value(entry.get('new_value'))}")
            else:
                lines.append(f"  property removed: {_property_label(entry)} = {_render_value(entry.get('old_value'))}")
    for entry in deltas.get("count_changed") or []:
        lines.append(
            f"  property count: {_property_label(entry)}: "
            f"{entry['old_count']} -> {entry['new_count']}"
        )
    return lines


_PROPERTY_FACT_PATH = re.compile(
    r"(IfcProperty(?:SingleValue|EnumeratedValue)\.(?:(Name)|NominalValue|EnumerationValues)"
    r"|IfcQuantity(?:Length|Area|Volume|Count|Weight|Time)\.(?:(Name)|LengthValue|AreaValue|VolumeValue|CountValue|WeightValue|TimeValue))"
    r"(?:\[\d+\])?"
)


def _data_delta_lines(data_delta: list[dict] | None, *, covered: dict | None, limit: int = 5) -> list[str]:
    if not data_delta:
        return []
    rows = data_delta
    if covered and _property_deltas_account_for(data_delta, covered):
        rows = [entry for entry in data_delta if not _PROPERTY_FACT_PATH.fullmatch(str(entry.get("path", "")))]
    lines: list[str] = []
    shown = rows[:limit]
    for entry in shown:
        path = _escape_name(clip(entry.get("path", "?")))
        old = _render_value(entry.get("old"))
        new = _render_value(entry.get("new"))
        quantized = " (quantized; human value unavailable)" if re.match(r"IfcQuantity\w+\.\w+Value", str(path)) else ""
        lines.append(f"  data: {path}{quantized}: {old} -> {new}")
    hidden = len(rows) - len(shown)
    if hidden > 0:
        lines.append(f"  data: ... and {hidden} more value deltas")
    return lines


def _property_deltas_account_for(data_delta: list[dict], deltas: dict) -> bool:
    """Whether the property deltas explain every raw property and quantity fact change.

    Raw fact paths carry no property set or property name, and their ``[n]``
    suffix numbers equal paths in value order, so one insertion shifts every
    later index. Each path is therefore compared as the multiset of values it
    lost and gained. Every lost and gained value must pair one to one with a
    value the property deltas name: names and string, integer and boolean
    values must be equal; reals, quantities and enumeration lists, whose raw
    form is quantized, pair with any fact.
    """
    lost: dict[str, Counter[str]] = {"name": Counter(), "value": Counter()}
    gained: dict[str, Counter[str]] = {"name": Counter(), "value": Counter()}
    by_path: dict[str, tuple[Counter[str], Counter[str]]] = {}
    for entry in data_delta:
        match = _PROPERTY_FACT_PATH.fullmatch(str(entry.get("path", "")))
        if not match:
            continue
        old_values, new_values = by_path.setdefault(match.group(1), (Counter(), Counter()))
        if entry.get("old") is not None:
            old_values[str(entry["old"])] += 1
        if entry.get("new") is not None:
            new_values[str(entry["new"])] += 1
    for path, (old_values, new_values) in by_path.items():
        kind = "name" if path.endswith(".Name") else "value"
        lost[kind] += old_values - new_values
        gained[kind] += new_values - old_values

    expected_lost: dict[str, list[object]] = {"name": [], "value": []}
    expected_gained: dict[str, list[object]] = {"name": [], "value": []}
    for entry in deltas.get("changed") or []:
        expected_lost["value"].append(entry.get("old_value"))
        expected_gained["value"].append(entry.get("new_value"))
    for entry in deltas.get("added") or []:
        expected_gained["name"].append(entry.get("name"))
        expected_gained["value"].append(entry.get("new_value"))
    for entry in deltas.get("removed") or []:
        expected_lost["name"].append(entry.get("name"))
        expected_lost["value"].append(entry.get("old_value"))
    for entry in deltas.get("count_changed") or []:
        difference = int(entry["new_count"]) - int(entry["old_count"])
        side = expected_gained if difference > 0 else expected_lost
        side["name"].extend([entry["name"]] * abs(difference))
        side["value"].extend([None] * abs(difference))
    return all(
        _pairs_one_to_one(raw[kind], expected[kind], kind)
        for raw, expected in ((lost, expected_lost), (gained, expected_gained))
        for kind in ("name", "value")
    )


def _pairs_one_to_one(raw: Counter[str], expected: list[object], kind: str) -> bool:
    if sum(raw.values()) != len(expected):
        return False
    remaining = Counter(raw)
    for value in expected:
        same = _fact_comparator(value, kind)
        if same is None:
            continue
        fact = next((fact for fact, count in remaining.items() if count and same(fact)), None)
        if fact is None:
            return False
        remaining[fact] -= 1
    return True


_BOOLEAN_FACT_TYPES = frozenset({"IFCBOOLEAN", "IFCLOGICAL"})
_STRING_FACT_TYPES = frozenset(
    {
        "IFCLABEL",
        "IFCTEXT",
        "IFCIDENTIFIER",
        "IFCDESCRIPTIVEMEASURE",
        "IFCURIREFERENCE",
        "IFCIRIREFERENCE",
        "IFCDATE",
        "IFCTIME",
        "IFCDATETIME",
        "IFCDURATION",
    }
)
_INTEGER_FACT_TYPES = frozenset({"IFCINTEGER", "IFCPOSITIVEINTEGER", "IFCCOUNTMEASURE", "IFCTIMESTAMP"})


def _fact_comparator(value: object, kind: str) -> Callable[[str], bool] | None:
    """How a raw fact is compared with a property delta value; None pairs it with any fact."""
    if kind == "name":
        return lambda fact: fact == _nfc(value)
    if isinstance(value, bool):
        return lambda fact: _typed_fact(fact, _BOOLEAN_FACT_TYPES) == ("true" if value else "false")
    if isinstance(value, str):
        return lambda fact: _typed_fact(fact, _STRING_FACT_TYPES) == _nfc(value)
    if isinstance(value, int):
        return lambda fact: ": " not in fact or _typed_fact(fact, _INTEGER_FACT_TYPES) == str(value)
    return None


def _typed_fact(fact: str, types: frozenset[str]) -> str | None:
    """The text after the type prefix of a ``TYPE: text`` fact whose type is in ``types``."""
    prefix, separator, text = fact.partition(": ")
    return text if separator and prefix.upper() in types else None


def _nfc(value: object) -> str:
    return unicodedata.normalize("NFC", str(value).strip())


def _property_label(entry: dict) -> str:
    return f"{_escape_name(clip(entry.get('pset', '?')))}.{_escape_name(clip(entry.get('name', '?')))}"


def _render_value(value: object) -> str:
    if value is None:
        return "<missing>"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _number(value, grouped=False)
    return _escape_name(clip(value))


def _number(value: int | float | Decimal, *, grouped: bool = True) -> str:
    separator = "," if grouped else ""
    if isinstance(value, int):
        return format(value, separator)
    rendered = format(Decimal(str(value)), f"{separator}f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _vector_mm(delta: object) -> str:
    if not isinstance(delta, (list, tuple)) or len(delta) != 3:
        return "?"
    return "(" + ", ".join(_number(Decimal(str(value)) / 1000) for value in delta) + ") m"


def _entity_label(entity: dict) -> str:
    cls = entity.get("class", "?")
    step = entity.get("step_id", "?")
    name = entity.get("name")
    if name:
        return f"{cls} {_quoted(name)} #{step}"
    return f"{cls} #{step}"


def _entity_ref(entity: dict) -> str:
    guid = entity.get("guid")
    suffix = f" {_escape_name(clip(guid))}" if guid else ""
    return f"#{entity.get('step_id', '?')}{suffix}"


def _quoted(value: object) -> str:
    return f'"{_escape_name(clip(value))}"'


def _escape_name(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")


def _placement_delta(delta: object) -> str | None:
    if not isinstance(delta, (list, tuple)) or len(delta) != 3:
        return None
    try:
        values = [float(value) for value in delta]
    except (TypeError, ValueError):
        return None
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        return None
    if norm >= 1000:
        return f"placement delta {_number(round(norm / 1000, 3))} m"
    return f"placement delta {norm:.3f} mm"
