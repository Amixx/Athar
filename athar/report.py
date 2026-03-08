"""Generate Markdown reports from Athar diff results."""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from athar.scene import clean_name, human_class


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
    if seconds < 5:
        return "same session"
    if seconds < 60:
        return f"{int(seconds)}s later"
    minutes = seconds / 60
    if minutes < 60:
        m = int(round(minutes))
        return f"{m} min later"
    hours = minutes / 60
    if hours < 24:
        h = int(round(hours))
        return f"{h}h later"
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


def _plural(n: int, word: str) -> str:
    """'1 entity' vs '2 entities', '1 wall' vs '3 walls'."""
    if word.endswith("y") and word[-2] not in "aeiou":
        return f"{n} {word[:-1]}ie{'s' if n != 1 else ''}" if n != 1 else f"1 {word}"
    suffix = "es" if word.endswith(("s", "sh", "ch", "x")) else "s"
    return f"{n} {word}" if n == 1 else f"{n} {word}{suffix}"


def _clean_field_name(field: str, qualified: bool = False) -> str:
    """Clean verbose pset field names for display.

    Default (qualified=False): just the property name.
      'pset.Pset_DoorCommon.FireRating' → 'FireRating'
      'pset.Pset_WallCommon.IsExternal' → 'IsExternal'

    With qualified=True: include pset context.
      'pset.Pset_DoorCommon.FireRating' → 'FireRating (door)'
    """
    m = re.match(r'^pset\.Pset_(\w+?)Common\.(.+)$', field)
    if m:
        if qualified:
            return f"{m.group(2)} ({m.group(1).lower()})"
        return m.group(2)
    m = re.match(r'^pset\.(\w+)\.(.+)$', field)
    if m:
        if qualified:
            pset = re.sub(r'^Pset_', '', m.group(1))
            return f"{m.group(2)} ({pset})"
        return m.group(2)
    return field


# Revit default placeholder values that mean "not set"
_PLACEHOLDER_VALUES = {
    "Door: Fire Rating", "Wall: Fire Rating", "Window: Fire Rating",
    "Fire Rating", "",
}


def _clean_value(val) -> str:
    """Clean a property value for display. Treat Revit placeholders as 'none'."""
    s = str(val) if val is not None else ""
    if s in _PLACEHOLDER_VALUES:
        return "none"
    return s


def _stats_line(summary: dict) -> str:
    """Build a one-line stats string like '+5 added, -3 deleted, ~2 changed'."""
    parts = []
    if summary["added"]:
        parts.append(f"**🟢 +{summary['added']} added**")
    if summary["deleted"]:
        parts.append(f"**🔴 -{summary['deleted']} deleted**")
    if summary.get("bulk_moved"):
        parts.append(f"**🔵 {summary['bulk_moved']} moved**")
    if summary["changed"]:
        parts.append(f"**🟡 ~{summary['changed']} changed**")
    parts.append(f"{summary['unchanged']} unchanged")
    return ", ".join(parts)


def _detail_from_label(label: str, category: str) -> str:
    """Strip leading category from a scene label for the Detail column."""
    for prefix in [category + " ", "exterior " + category + " ",
                   "interior " + category + " "]:
        if label.lower().startswith(prefix):
            return label[len(prefix):]
    return label


# Matches " on <StoreyName>" at the end of a label
_STOREY_SUFFIX_RE = re.compile(r"\s+on\s+(.+)$")


def _name_breakdown(entities: list[dict], labels: dict, category: str) -> str:
    """Build a name/location sub-breakdown string for the Details column.

    Deduplicates a common storey suffix: instead of repeating "on Floor 0"
    on every item, appends it once at the end.
    """
    details: list[str | None] = []
    for e in entities:
        label = labels.get(e["guid"])
        if label:
            details.append(_detail_from_label(label, category))
        elif e.get("name"):
            details.append(clean_name(e["name"]) or e["name"])
        else:
            details.append(None)

    non_null = [d for d in details if d is not None]
    if not non_null:
        return ""

    # Extract storey suffixes and check if they're all the same
    common_storey = None
    storeys = []
    for d in non_null:
        m = _STOREY_SUFFIX_RE.search(d)
        storeys.append(m.group(1) if m else None)
    unique_storeys = set(storeys)
    if len(unique_storeys) == 1 and None not in unique_storeys:
        common_storey = unique_storeys.pop()

    # Strip the common storey suffix from each detail
    if common_storey:
        stripped = [_STOREY_SUFFIX_RE.sub("", d) for d in non_null]
    else:
        stripped = non_null

    counts = Counter(stripped)
    parts = []
    for detail, count in counts.most_common(3):
        if count > 1:
            parts.append(f"{count}× {detail}")
        else:
            parts.append(detail)

    n_extra = len(counts) - 3
    result = ", ".join(parts)
    if n_extra > 0:
        result += f" +{n_extra} more"
    if common_storey:
        result += f" ({common_storey})"
    return result


def _bulk_type_label(bm: dict) -> str:
    """Describe the type mix of a bulk movement for the Type column."""
    breakdown = bm["class_breakdown"]
    if len(breakdown) == 1:
        return human_class(next(iter(breakdown)))
    return ", ".join(human_class(cls) for cls in breakdown)


def _changes_table(result: dict, verbose: bool = False,
                   labels: dict | None = None) -> list[str]:
    """Generate a single unified change table for all change categories."""
    lines: list[str] = []

    has_any = (result.get("bulk_movements") or result["added"]
               or result["deleted"] or result["changed"])
    if not has_any:
        lines.append("*No changes*")
        return lines

    if verbose:
        return _changes_verbose(result, labels=labels)

    labels = labels or {}

    # Unified compact table
    rows: list[tuple[str, str, int, str]] = []  # (symbol, type, count, detail)

    # Bulk movements — one row per movement
    for bm in result.get("bulk_movements", []):
        groups = bm.get("groups", [])
        group_note = f" ({groups[0]})" if groups else ""
        name_detail = _name_breakdown(bm["entities"], labels, "")
        if name_detail:
            detail = f"{name_detail} — {bm['description']}{group_note}"
        else:
            detail = f"{bm['description']}{group_note}"
        rows.append(("🔵", _bulk_type_label(bm), bm["count"], detail))

    # Added — class breakdown with name/location details
    if result["added"]:
        entities_by_class: dict[str, list[dict]] = {}
        for e in result["added"]:
            cls = human_class(e["ifc_class"])
            entities_by_class.setdefault(cls, []).append(e)
        counts = Counter(human_class(e["ifc_class"]) for e in result["added"])
        for cls, count in counts.most_common():
            detail = _name_breakdown(entities_by_class[cls], labels, cls)
            rows.append(("🟢", cls, count, detail))

    # Deleted — class breakdown with name/location details
    if result["deleted"]:
        entities_by_class_del: dict[str, list[dict]] = {}
        for e in result["deleted"]:
            cls = human_class(e["ifc_class"])
            entities_by_class_del.setdefault(cls, []).append(e)
        counts = Counter(human_class(e["ifc_class"]) for e in result["deleted"])
        for cls, count in counts.most_common():
            detail = _name_breakdown(entities_by_class_del[cls], labels, cls)
            rows.append(("🔴", cls, count, detail))

    # Changed — class breakdown + deduplicated change descriptions
    if result["changed"]:
        counts = Counter(human_class(e["ifc_class"]) for e in result["changed"])
        # Collect unique change descriptions per class
        # Deduplicate by (property_name, old, new) — same change in multiple psets shows once
        details_by_class: dict[str, list[str]] = {}
        seen_by_class: dict[str, set[tuple]] = {}
        for e in result["changed"]:
            cls = human_class(e["ifc_class"])
            if cls not in details_by_class:
                details_by_class[cls] = []
                seen_by_class[cls] = set()
            for c in e["changes"]:
                if c["field"] == "placement":
                    desc = c.get("description", "placement changed")
                    short = re.sub(r'\s*\(from near .+\)$', '', desc)
                    key = ("placement", short)
                    if key not in seen_by_class[cls]:
                        seen_by_class[cls].add(key)
                        details_by_class[cls].append(short)
                else:
                    prop = _clean_field_name(c["field"])
                    old_v = _clean_value(c["old"])
                    new_v = _clean_value(c["new"])
                    key = (prop, old_v, new_v)
                    if key not in seen_by_class[cls]:
                        seen_by_class[cls].add(key)
                        details_by_class[cls].append(
                            f"{prop}: {old_v} → {new_v}"
                        )
        for cls, count in counts.most_common():
            details = details_by_class[cls][:3]
            n_extra = len(details_by_class[cls]) - 3
            suffix = f" +{n_extra} more" if n_extra > 0 else ""
            rows.append(("🟡", cls, count, "; ".join(details) + suffix))

    # Table-level storey dedup: if all rows that mention a storey share
    # the same one, strip it from all of them (rows without a storey
    # suffix, like property-change-only rows, are left as-is)
    if rows:
        storey_suffixes = set()
        for _, _, _, detail in rows:
            m = re.search(r"\(([^)]+)\)(?=\s*—|\s*$)", detail)
            if m:
                storey_suffixes.add(m.group(1))
        if len(storey_suffixes) == 1:
            common = storey_suffixes.pop()
            rows = [
                (s, t, c, re.sub(r"\s*\(" + re.escape(common) + r"\)", "", d).strip())
                for s, t, c, d in rows
            ]

        lines.append("| | Type | Count | Details |")
        lines.append("|---|------|------:|---------|")
        for symbol, typ, count, detail in rows:
            lines.append(f"| {symbol} | {typ} | {count} | {detail} |")

    return lines


def _changes_verbose(result: dict, labels: dict | None = None) -> list[str]:
    """Generate detailed per-entity change output."""
    labels = labels or {}
    lines: list[str] = []

    for bm in result.get("bulk_movements", []):
        groups = bm.get("groups", [])
        group_note = f" in *{groups[0]}*" if groups else ""
        count = bm["count"]
        lines.append(f"**Bulk movement** — {_plural(count, 'entity')}{group_note}")
        lines.append(f"  {bm['description']}")
        class_parts = ", ".join(
            f"{count} {human_class(cls)}" for cls, count in bm["class_breakdown"].items()
        )
        lines.append(f"  {class_parts}")
        lines.append("")

    if result["added"]:
        lines.append(f"**Added** ({_plural(len(result['added']), 'entity')})")
        lines.append("")
        lines.append("| Type | Name | Label | GlobalId |")
        lines.append("|------|------|-------|----------|")
        for e in result["added"]:
            name = e.get("name") or "*(unnamed)*"
            label = labels.get(e["guid"], "")
            lines.append(f"| {human_class(e['ifc_class'])} | {name} | {label} | `{e['guid']}` |")
        lines.append("")

    if result["deleted"]:
        lines.append(f"**Deleted** ({_plural(len(result['deleted']), 'entity')})")
        lines.append("")
        lines.append("| Type | Name | Label | GlobalId |")
        lines.append("|------|------|-------|----------|")
        for e in result["deleted"]:
            name = e.get("name") or "*(unnamed)*"
            label = labels.get(e["guid"], "")
            lines.append(f"| {human_class(e['ifc_class'])} | {name} | {label} | `{e['guid']}` |")
        lines.append("")

    if result["changed"]:
        lines.append(f"**Changed** ({_plural(len(result['changed']), 'entity')})")
        lines.append("")
        lines.append("| Type | Name | GlobalId | Fields |")
        lines.append("|------|------|----------|--------|")
        for e in result["changed"]:
            name = e.get("name") or "*(unnamed)*"
            field_descs = []
            for c in e["changes"]:
                if c["field"] == "placement":
                    field_descs.append(c.get("description", "placement changed"))
                else:
                    name = _clean_field_name(c["field"], qualified=True)
                    field_descs.append(
                        f"{name}: {_clean_value(c['old'])} → {_clean_value(c['new'])}"
                    )
            fields_str = "; ".join(field_descs)
            lines.append(
                f"| {human_class(e['ifc_class'])} | {name} | `{e['guid']}` | {fields_str} |"
            )
        lines.append("")

    return lines


def report_two_file(result: dict, old_path: str, new_path: str,
                    verbose: bool = False,
                    labels: dict | None = None) -> str:
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

    lines.append(_stats_line(result["summary"]))
    lines.append("")
    lines.extend(_changes_table(result, verbose=verbose, labels=labels))
    lines.append("")

    lines.append("---")
    lines.append("*Generated by [Athar](https://github.com/imantsliepins/athar)*")
    lines.append("")

    return "\n".join(lines)


def report_folder(groups_data: list[dict], verbose: bool = False) -> str:
    """Generate a Markdown report for folder mode.

    groups_data is a list of dicts, each with:
      - label: str (group name)
      - files: list[Path] (version files)
      - steps: list[dict] with {old_name, new_name, result, labels?}
      - cumulative: dict|None (cumulative diff result, if 3+ versions)
      - cumulative_labels: dict|None (guid→label for cumulative diff)
      - parsed_first_meta: dict (metadata of the first version, for ref time)
    """
    lines: list[str] = []
    lines.append("# IFC Version History")
    lines.append("")

    for gi, group in enumerate(groups_data):
        label = group["label"]
        files = group["files"]
        steps = group["steps"]
        cumulative = group.get("cumulative")
        first_meta = group.get("parsed_first_meta", {})
        first_ts = first_meta.get("timestamp")

        n_versions = len(files)
        app = first_meta.get("application", "?")
        date_str = _format_date(first_ts)

        if len(groups_data) > 1:
            lines.append(f"## {label}")
        else:
            lines.append(f"## {label}")
        lines.append("")
        lines.append(f"*{_plural(n_versions, 'version')} · {date_str} · {app}*")
        lines.append("")

        # Cumulative summary — compact, at top
        if cumulative:
            lines.append(f"### Overall (`{files[0].name}` → `{files[-1].name}`)")
            lines.append("")
            lines.append(_stats_line(cumulative["summary"]))
            lines.append("")
            cumulative_labels = group.get("cumulative_labels")
            lines.extend(_changes_table(cumulative, verbose=verbose,
                                        labels=cumulative_labels))
            lines.append("")

        # Step-by-step diffs
        for step in steps:
            result = step["result"]
            meta = result.get("metadata", {})
            old_meta = meta.get("old", {})
            new_meta = meta.get("new", {})
            old_ts = old_meta.get("timestamp")
            new_ts = new_meta.get("timestamp")

            old_dt = _parse_timestamp(old_ts)
            time_label = _format_date(new_ts, ref_dt=old_dt)

            lines.append(
                f"### {step['old_name']} → {step['new_name']}"
            )
            lines.append("")
            lines.append(f"*{time_label}*")
            lines.append("")
            lines.append(_stats_line(result["summary"]))
            lines.append("")
            step_labels = step.get("labels")
            lines.extend(_changes_table(result, verbose=verbose,
                                        labels=step_labels))
            lines.append("")

        if gi < len(groups_data) - 1:
            lines.append("---")
            lines.append("")

    lines.append("---")
    lines.append("*Generated by [Athar](https://github.com/imantsliepins/athar)*")
    lines.append("")

    return "\n".join(lines)
