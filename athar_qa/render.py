"""Human-readable renderers for QA gate verdicts and review history."""

from __future__ import annotations

from typing import Any

_ASPECT_ORDER = ("added", "deleted", "modified", "unchanged")


def render_check_report(result: dict[str, Any], *, title: str = "Athar QA gate") -> str:
    """Render an :func:`athar.check.evaluate_report` verdict as text."""
    violations = result.get("violations") or []
    ok = result.get("ok", not violations)

    lines: list[str] = []
    if ok:
        lines.append(f"{title}: PASS")
    else:
        count = len(violations)
        lines.append(f"{title}: FAIL ({count} violation{'s' if count != 1 else ''})")

    summary = result.get("summary")
    if summary:
        lines.append(_summary_line(summary))

    for violation in violations:
        lines.append("")
        lines.append(f"✗ {violation.get('message', violation.get('code', 'violation'))}")
        lines.extend(_violation_detail_lines(violation))

    return "\n".join(lines) + "\n"


def render_history(events: list[dict[str, Any]], *, title: str = "Review history") -> str:
    """Render the append-only store event log as a readable timeline."""
    lines: list[str] = [title]
    if not events:
        lines.append("  (no events)")
        return "\n".join(lines) + "\n"

    pass_count = 0
    fail_count = 0
    for event in events:
        lines.append("  " + _event_line(event))
        if event.get("event") == "review":
            if event.get("verdict") == "pass":
                pass_count += 1
            elif event.get("verdict") == "fail":
                fail_count += 1

    lines.append("")
    lines.append(f"Reviews: {pass_count} passed, {fail_count} failed ({len(events)} events total)")
    return "\n".join(lines) + "\n"


def _summary_line(summary: dict[str, Any]) -> str:
    parts = [f"{name} {summary.get(name, 0)}" for name in _ASPECT_ORDER if name in summary]
    return "Summary: " + ", ".join(parts) if parts else "Summary: (none)"


def _violation_detail_lines(violation: dict[str, Any]) -> list[str]:
    code = violation.get("code")
    if code == "schema_changed":
        return [f"  schema: {violation.get('old')} -> {violation.get('new')}"]

    lines: list[str] = []
    for entry in violation.get("affected") or []:
        lines.append("  - " + _affected_line(code, entry))
    hidden = (violation.get("count") or 0) - len(violation.get("affected") or [])
    if hidden > 0:
        lines.append(f"  ... and {hidden} more")
    return lines


def _affected_line(code: str | None, entry: dict[str, Any]) -> str:
    label = _entity_label(entry)
    if code == "property_value_changed":
        return f"{label}: {entry.get('property')} {_value(entry.get('old_value'))} -> {_value(entry.get('new_value'))}"
    if code == "property_removed":
        removed = ", ".join(str(name) for name in entry.get("removed_properties") or [])
        return f"{label}: removed {removed}"
    if code == "placement_delta_limit":
        return f"{label}: placement delta {entry.get('placement_delta_mm')} mm (limit {entry.get('limit')})"
    return label


def _entity_label(entry: dict[str, Any]) -> str:
    cls = entry.get("class", "?")
    step = entry.get("step_id", "?")
    name = entry.get("name")
    guid = entry.get("guid")
    label = f'{cls} "{name}"' if name else str(cls)
    suffix = f" {guid}" if guid else ""
    return f"{label} #{step}{suffix}"


def _event_line(event: dict[str, Any]) -> str:
    eid = event.get("id", "?")
    at = event.get("at", "?")
    actor = event.get("actor", "?")
    model_key = event.get("model_key", "?")
    kind = event.get("event", "?")

    if kind == "review":
        verdict = (event.get("verdict") or "?").upper()
        stats = (event.get("diff") or {}).get("stats") or {}
        policy = " [policy]" if event.get("has_policy") else ""
        return f"{eid}  {at}  review {verdict}{policy}  {model_key} by {actor}  ({_stats_brief(stats)})"
    if kind == "approval":
        return f"{eid}  {at}  approval  {model_key} by {actor}  (review {event.get('review_id')})"
    if kind == "rejection":
        return f"{eid}  {at}  rejection  {model_key} by {actor}  (review {event.get('review_id')})"
    if kind == "baseline_set":
        artifact = event.get("artifact") or {}
        return f"{eid}  {at}  baseline_set  {model_key} by {actor}  ({artifact.get('schema', '?')})"
    return f"{eid}  {at}  {kind}  {model_key} by {actor}"


def _stats_brief(stats: dict[str, Any]) -> str:
    return (
        f"+{stats.get('added', 0)} -{stats.get('deleted', 0)} "
        f"~{stats.get('modified', 0)} ={stats.get('unchanged', 0)}"
    )


def _value(value: Any) -> str:
    if value is None:
        return "<missing>"
    if isinstance(value, list):
        return "[" + ", ".join(str(item) for item in value) + "]"
    return str(value)
