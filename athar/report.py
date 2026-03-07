"""Generate Markdown reports from Athar diff results."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path


def _parse_timestamp(ts: str | None) -> datetime | None:
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _format_relative_time(delta_seconds: float) -> str:
    seconds = abs(delta_seconds)
    if seconds < 60:
        return f"{int(seconds)} seconds later"
    minutes = seconds / 60
    if minutes < 60:
        m = int(round(minutes))
        return f"{m} minute{'s' if m != 1 else ''} later"
    hours = minutes / 60
    if hours < 24:
        h = int(round(hours))
        return f"{h} hour{'s' if h != 1 else ''} later"
    days = hours / 24
    if days < 30:
        d = int(round(days))
        return f"{d} day{'s' if d != 1 else ''} later"
    months = days / 30.44
    if months < 12:
        m = int(round(months))
        return f"{m} month{'s' if m != 1 else ''} later"
    years = days / 365.25
    y = round(years, 1)
    if y == int(y):
        y = int(y)
    return f"{y} year{'s' if y != 1 else ''} later"


def _format_date(ts: str | None, ref_dt: datetime | None = None) -> str:
    if not ts:
        return "?"
    dt = _parse_timestamp(ts)
    if dt is None:
        return ts
    if ref_dt is None:
        return dt.strftime("%-d %b %Y, %H:%M")
    dt_naive = dt.replace(tzinfo=None)
    ref_naive = ref_dt.replace(tzinfo=None)
    delta = (dt_naive - ref_naive).total_seconds()
    return _format_relative_time(delta)


def _stats_line(summary: dict) -> str:
    """Build a one-line stats string like '5 added, 3 deleted, 2 changed'."""
    parts = []
    if summary["added"]:
        parts.append(f"**{summary['added']} added**")
    if summary["deleted"]:
        parts.append(f"**{summary['deleted']} deleted**")
    if summary.get("bulk_moved"):
        parts.append(f"**{summary['bulk_moved']} bulk-moved**")
    if summary["changed"]:
        parts.append(f"**{summary['changed']} changed**")
    parts.append(f"{summary['unchanged']} unchanged")
    return ", ".join(parts)


def _class_breakdown_table(entities: list[dict], label: str) -> str:
    """Build a markdown table showing IFC class counts."""
    if not entities:
        return ""
    counts = Counter(e["ifc_class"] for e in entities)
    lines = [f"| IFC Class | Count |", f"|-----------|-------|"]
    for cls, count in counts.most_common():
        lines.append(f"| {cls} | {count} |")
    return "\n".join(lines)


def _entity_table(entities: list[dict], columns: list[str] | None = None) -> str:
    """Build a markdown table listing entities."""
    if not entities:
        return ""
    lines = [
        "| IFC Class | Name | GlobalId |",
        "|-----------|------|----------|",
    ]
    for e in entities:
        name = e.get("name") or "*(unnamed)*"
        lines.append(f"| {e['ifc_class']} | {name} | `{e['guid']}` |")
    return "\n".join(lines)


def _changes_section(result: dict, verbose: bool = False) -> list[str]:
    """Generate markdown lines for all change categories."""
    lines: list[str] = []

    # Bulk movements
    for bm in result.get("bulk_movements", []):
        groups = bm.get("groups", [])
        group_note = f" in *{groups[0]}*" if groups else ""
        lines.append(f"### 🔀 Bulk Movement — {bm['count']} entities{group_note}")
        lines.append("")
        lines.append(f"**{bm['description']}**")
        lines.append("")
        class_parts = ", ".join(
            f"{count} {cls}" for cls, count in bm["class_breakdown"].items()
        )
        lines.append(f"Breakdown: {class_parts}")
        lines.append("")

    # Added
    if result["added"]:
        lines.append(f"### ➕ Added ({len(result['added'])} entities)")
        lines.append("")
        if verbose:
            lines.append(_entity_table(result["added"]))
        else:
            lines.append(_class_breakdown_table(result["added"], "Added"))
        lines.append("")

    # Deleted
    if result["deleted"]:
        lines.append(f"### ➖ Deleted ({len(result['deleted'])} entities)")
        lines.append("")
        if verbose:
            lines.append(_entity_table(result["deleted"]))
        else:
            lines.append(_class_breakdown_table(result["deleted"], "Deleted"))
        lines.append("")

    # Changed
    if result["changed"]:
        lines.append(f"### ✏️ Changed ({len(result['changed'])} entities)")
        lines.append("")
        if verbose:
            lines.append("| IFC Class | Name | GlobalId | Fields Changed |")
            lines.append("|-----------|------|----------|----------------|")
            for e in result["changed"]:
                name = e.get("name") or "*(unnamed)*"
                field_descs = []
                for c in e["changes"]:
                    if c["field"] == "placement":
                        field_descs.append(c.get("description", "placement changed"))
                    else:
                        field_descs.append(f"{c['field']}: {c['old']} → {c['new']}")
                fields_str = "; ".join(field_descs)
                lines.append(
                    f"| {e['ifc_class']} | {name} | `{e['guid']}` | {fields_str} |"
                )
        else:
            lines.append(
                _class_breakdown_table(result["changed"], "Changed")
            )
            lines.append("")
            # Top changed fields
            field_counts: Counter = Counter()
            for e in result["changed"]:
                for c in e["changes"]:
                    field_counts[c["field"]] += 1
            if field_counts:
                lines.append("**Most frequently changed fields:**")
                lines.append("")
                for field, count in field_counts.most_common(10):
                    lines.append(f"- `{field}` ({count}×)")
        lines.append("")

    return lines


def report_two_file(result: dict, old_path: str, new_path: str,
                    verbose: bool = False) -> str:
    """Generate a Markdown report for a two-file diff."""
    meta = result.get("metadata", {})
    old_meta = meta.get("old", {})
    new_meta = meta.get("new", {})
    old_ts = old_meta.get("timestamp")
    new_ts = new_meta.get("timestamp")
    old_dt = _parse_timestamp(old_ts)

    lines: list[str] = []
    lines.append("# IFC Diff Report")
    lines.append("")
    lines.append("| | File | Date | Application |")
    lines.append("|---|------|------|-------------|")
    lines.append(
        f"| **Old** | `{Path(old_path).name}` | {_format_date(old_ts)} "
        f"| {old_meta.get('application', '?')} |"
    )
    lines.append(
        f"| **New** | `{Path(new_path).name}` | {_format_date(new_ts, ref_dt=old_dt)} "
        f"| {new_meta.get('application', '?')} |"
    )
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(_stats_line(result["summary"]))
    lines.append("")

    lines.append("## Changes")
    lines.append("")
    lines.extend(_changes_section(result, verbose=verbose))

    lines.append("---")
    lines.append("*Generated by [Athar](https://github.com/imantsliepins/athar)*")
    lines.append("")

    return "\n".join(lines)


def report_folder(groups_data: list[dict], verbose: bool = False) -> str:
    """Generate a Markdown report for folder mode.

    groups_data is a list of dicts, each with:
      - label: str (group name)
      - files: list[Path] (version files)
      - steps: list[dict] with {old_name, new_name, result}
      - cumulative: dict|None (cumulative diff result, if 3+ versions)
      - parsed_first_meta: dict (metadata of the first version, for ref time)
    """
    lines: list[str] = []
    lines.append("# IFC Version History Report")
    lines.append("")

    for gi, group in enumerate(groups_data):
        label = group["label"]
        files = group["files"]
        steps = group["steps"]
        cumulative = group.get("cumulative")
        first_meta = group.get("parsed_first_meta", {})
        first_ts = first_meta.get("timestamp")
        ref_dt = _parse_timestamp(first_ts)

        if len(groups_data) > 1:
            lines.append(f"## {label}")
        else:
            lines.append(f"## Model: {label}")
        lines.append("")

        # Version list
        lines.append(f"**{len(files)} versions detected:**")
        lines.append("")
        for i, f in enumerate(files):
            lines.append(f"{i + 1}. `{f.name}`")
        lines.append("")

        # Cumulative summary at top if available
        if cumulative:
            lines.append("### 📊 Cumulative Summary")
            lines.append(f"*`{files[0].name}` → `{files[-1].name}`*")
            lines.append("")
            lines.append(_stats_line(cumulative["summary"]))
            lines.append("")
            lines.extend(_changes_section(cumulative, verbose=verbose))

        # Step-by-step diffs
        for i, step in enumerate(steps):
            result = step["result"]
            meta = result.get("metadata", {})
            old_meta = meta.get("old", {})
            new_meta = meta.get("new", {})
            old_ts = old_meta.get("timestamp")
            new_ts = new_meta.get("timestamp")

            old_dt = _parse_timestamp(old_ts)
            new_label = _format_date(new_ts, ref_dt=old_dt)
            app = new_meta.get("application", "?")

            lines.append(
                f"### Step {i + 1}: `{step['old_name']}` → `{step['new_name']}`"
            )
            lines.append("")
            if i == 0:
                lines.append(f"*{_format_date(old_ts)} · {new_label} · via {app}*")
            else:
                lines.append(f"*{new_label} · via {app}*")
            lines.append("")
            lines.append(_stats_line(result["summary"]))
            lines.append("")
            lines.extend(_changes_section(result, verbose=verbose))

        if gi < len(groups_data) - 1:
            lines.append("---")
            lines.append("")

    lines.append("---")
    lines.append("*Generated by [Athar](https://github.com/imantsliepins/athar)*")
    lines.append("")

    return "\n".join(lines)
