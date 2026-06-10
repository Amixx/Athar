"""Deterministic terminal renderer for Athar JSON reports."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Iterable

DEFAULT_MAX_ITEMS = 80


def render_text_report(report: dict, *, max_items: int = DEFAULT_MAX_ITEMS) -> str:
    """Render a compact, deterministic terminal summary."""

    max_items = max(0, int(max_items))
    stats = report.get("stats", {})
    schemas = report.get("schemas", {})
    lines: list[str] = [
        "Athar IFC diff",
        f"Schema: {schemas.get('old', '?')} -> {schemas.get('new', '?')}",
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

    lines.extend(_class_count_lines(report))

    remaining = max_items
    for section, marker in (("modified", "~"), ("added", "+"), ("deleted", "-")):
        items = report.get(section, [])
        if not items:
            continue
        lines.append("")
        lines.append(f"{section.title()} ({len(items)})")
        shown = items[:remaining]
        for item in shown:
            lines.extend(_render_item(section, marker, item))
        remaining -= len(shown)
        hidden = len(items) - len(shown)
        if hidden > 0:
            lines.append(f"... and {hidden} more {section}")
        if remaining <= 0:
            hidden_total = sum(len(report.get(name, [])) for name in ("modified", "added", "deleted")) - max_items
            if hidden_total > 0 and hidden <= 0:
                lines.append(f"... and {hidden_total} more changed entities")
            break

    if stats.get("added", 0) == stats.get("deleted", 0) == stats.get("modified", 0) == 0:
        lines.append("")
        lines.append("No semantic entity changes.")

    return "\n".join(lines) + "\n"


def _summary_line(stats: dict) -> str:
    return (
        "Summary: "
        f"+{stats.get('added', 0)} "
        f"-{stats.get('deleted', 0)} "
        f"~{stats.get('modified', 0)} "
        f"={stats.get('unchanged', 0)} "
        f"(old {stats.get('old_signatures', 0)}, new {stats.get('new_signatures', 0)})"
    )


def _class_count_lines(report: dict) -> list[str]:
    lines: list[str] = []
    for section in ("modified", "added", "deleted"):
        counts = _class_counts(report.get(section, []), section)
        if counts:
            rendered = ", ".join(f"{name} {count}" for name, count in counts)
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


def _render_item(section: str, marker: str, item: dict) -> list[str]:
    if section == "modified":
        old = item.get("old", {})
        new = item.get("new", {})
        aspects = item.get("aspects", {})
        changed = [name for name in ("geometry", "data", "topology", "placement") if aspects.get(name) == "changed"]
        detail = ", ".join(changed) if changed else "no aspect changes"
        placement = _placement_delta(aspects.get("placement_delta_mm"))
        if placement:
            detail = f"{detail}, {placement}"
        match = item.get("match", {})
        reason = match.get("reason", "?")
        score = match.get("score", "?")
        return [
            f"{marker} {_entity_label(new)} [{detail}]",
            f"  match: {reason} score {score}; old {_entity_ref(old)} -> new {_entity_ref(new)}",
        ]
    return [f"{marker} {_entity_label(item)}"]


def _entity_label(entity: dict) -> str:
    cls = entity.get("class", "?")
    step = entity.get("step_id", "?")
    name = entity.get("name")
    if name:
        return f'{cls} "{_escape_name(str(name))}" #{step}'
    return f"{cls} #{step}"


def _entity_ref(entity: dict) -> str:
    guid = entity.get("guid")
    suffix = f" {guid}" if guid else ""
    return f"#{entity.get('step_id', '?')}{suffix}"


def _escape_name(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


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
    return f"placement delta {norm:.3f} mm"
