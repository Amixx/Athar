"""CLI entry point for Athar IFC diff tool."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from .folder import scan_folder
from .report import report_two_file, report_folder
from .scene import build_scene, human_class
from .placement import enrich_diff


# ANSI color helpers — disabled when stdout is not a TTY
def _use_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    if not _use_color():
        return text
    return f"\033[{code}m{text}\033[0m"

def _green(t: str) -> str: return _c("32", t)
def _red(t: str) -> str: return _c("31", t)
def _yellow(t: str) -> str: return _c("33", t)
def _cyan(t: str) -> str: return _c("36", t)
def _magenta(t: str) -> str: return _c("35", t)
def _bold(t: str) -> str: return _c("1", t)
def _dim(t: str) -> str: return _c("2", t)


def _parse_timestamp(ts: str | None) -> datetime | None:
    """Try to parse an IFC header timestamp into a datetime."""
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _format_relative_time(delta_seconds: float) -> str:
    """Format a timedelta as a human-readable 'X later' string."""
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
    """Format a timestamp: full date if ref_dt is None, relative if ref_dt is set."""
    if not ts:
        return "?"
    dt = _parse_timestamp(ts)
    if dt is None:
        return ts  # fallback to raw string
    if ref_dt is None:
        return dt.strftime("%-d %b %Y, %H:%M")
    # Strip tzinfo from both to avoid naive/aware mismatch
    dt_naive = dt.replace(tzinfo=None)
    ref_naive = ref_dt.replace(tzinfo=None)
    delta = (dt_naive - ref_naive).total_seconds()
    return _format_relative_time(delta)


def main():
    raise SystemExit(
        "athar_layers CLI is temporarily disabled while it is rewired to the graph engine."
    )
    parser = argparse.ArgumentParser(
        prog="athar",
        description="Semantic IFC diff — compare BIM models at the entity/property level.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Two IFC files to diff, or a folder to auto-detect and diff versions",
    )
    parser.add_argument(
        "-o", "--output",
        help="Write JSON diff to file instead of stdout (two-file mode only)",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a human-readable summary instead of JSON",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-entity details in summary mode (default summary is condensed)",
    )
    parser.add_argument(
        "--report",
        metavar="FILE",
        help="Write a Markdown (.md) report to FILE",
    )

    args = parser.parse_args()
    # Summary defaults to condensed (short) view; --verbose expands it
    args.short = args.summary and not args.verbose

    # Folder mode: single directory argument — summary is the default
    if len(args.paths) == 1 and Path(args.paths[0]).is_dir():
        if not args.summary and not args.output and not args.report:
            args.summary = True
            args.short = not args.verbose
        _folder_mode(args.paths[0], args)
    # Two-file mode
    elif len(args.paths) == 2:
        if not args.summary and not args.output and not args.report:
            args.summary = True
            args.short = not args.verbose
        _two_file_mode(args.paths[0], args.paths[1], args)
    else:
        parser.error("Provide either two IFC files or a single folder")


def _build_labels(*parsed_models) -> dict:
    """Build guid → scene label lookup from parsed model versions."""
    labels = {}
    for parsed in parsed_models:
        if parsed:
            scene = build_scene(parsed)
            for guid, el in scene["elements"].items():
                labels[guid] = el["label"]
    return labels


def _two_file_mode(old_path: str, new_path: str, args):
    old_model = parse(old_path)
    new_model = parse(new_path)
    result = diff(old_model, new_model)
    enrich_diff(result, new_model["entities"])

    if args.report:
        labels = _build_labels(old_model, new_model)
        md = report_two_file(result, old_path, new_path, verbose=args.verbose,
                             labels=labels)
        with open(args.report, "w") as f:
            f.write(md)
        print(f"Report written to {args.report}", file=sys.stderr)
    elif args.summary:
        _print_summary(result, short=args.short)
    elif args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Diff written to {args.output}", file=sys.stderr)
    else:
        json.dump(result, sys.stdout, indent=2)
        print()


def _folder_mode(folder: str, args):
    print(f"Scanning {folder}/ for IFC versions...\n", file=sys.stderr)

    groups = scan_folder(folder)

    if not groups:
        print(f"No version groups found in {folder}/. "
              "Need at least 2 IFC files that are versions of the same model.",
              file=sys.stderr)
        sys.exit(1)

    # If --report, collect all data and generate markdown at the end
    report_groups = [] if args.report else None

    for gi, group in enumerate(groups):
        # Infer group name from common prefix of filenames
        group_label = _group_label(group)

        if not args.report:
            print(f"{'=' * 60}")
            print(f"GROUP: {group_label} ({len(group)} versions)")
            print(f"{'=' * 60}\n")

            for i, f in enumerate(group):
                print(f"  [{i}] {f.name}")
            print()

        # Parse all files in group
        parsed = []
        for f in group:
            print(f"  Parsing {f.name}...", file=sys.stderr)
            parsed.append(parse(str(f)))

        # Determine reference datetime from the first version
        first_ts = parsed[0]["metadata"].get("timestamp")
        ref_dt = _parse_timestamp(first_ts)

        # Collect step data for report
        step_data = []

        # Step-by-step diffs
        for i in range(len(group) - 1):
            result = diff(parsed[i], parsed[i + 1])
            enrich_diff(result, parsed[i+1]["entities"])
            step_entry = {
                "old_name": group[i].name,
                "new_name": group[i + 1].name,
                "result": result,
            }
            if args.report:
                step_entry["labels"] = _build_labels(parsed[i], parsed[i + 1])
            step_data.append(step_entry)

            if args.report:
                pass  # will write report at end
            elif args.summary:
                print(f"Step {i + 1}: {group[i].name}  →  {group[i + 1].name}")
                _print_step_summary(result, short=args.short, is_first=(i == 0))
                print()
            elif args.output:
                out = Path(args.output).stem + f"_{group_label}_{i + 1}.json"
                with open(out, "w") as f:
                    json.dump(result, f, indent=2)
                print(f"  Diff written to {out}", file=sys.stderr)
            else:
                if len(group) > 2 or len(groups) > 1:
                    print(f"// Step {i + 1}: {group[i].name} → {group[i + 1].name}")
                json.dump(result, sys.stdout, indent=2)
                print()

        # Cumulative diff if 3+ versions
        cumulative = None
        if len(group) > 2:
            cumulative = diff(parsed[0], parsed[-1])
            enrich_diff(cumulative, parsed[-1]["entities"])
            if args.report:
                pass  # will write report at end
            elif args.summary:
                print(f"Cumulative: {group[0].name}  →  {group[-1].name}")
                _print_step_summary(cumulative, short=args.short)
                print()  # cumulative shows full dates on both sides
            else:
                print(f"// Cumulative: {group[0].name} → {group[-1].name}")
                json.dump(cumulative, sys.stdout, indent=2)
                print()

        if report_groups is not None:
            cumulative_labels = None
            if cumulative:
                cumulative_labels = _build_labels(parsed[0], parsed[-1])
            report_groups.append({
                "label": group_label,
                "files": group,
                "steps": step_data,
                "cumulative": cumulative,
                "cumulative_labels": cumulative_labels,
                "parsed_first_meta": parsed[0]["metadata"],
            })

        if not args.report and gi < len(groups) - 1:
            print()

    if args.report and report_groups is not None:
        md = report_folder(report_groups, verbose=args.verbose)
        with open(args.report, "w") as f:
            f.write(md)
        print(f"Report written to {args.report}", file=sys.stderr)


def _group_label(group: list[Path]) -> str:
    """Derive a human label for a group of version files."""
    import re
    names = [p.stem for p in group]
    if len(names) == 1:
        return names[0]
    # Find longest common prefix
    prefix = names[0]
    for name in names[1:]:
        while not name.startswith(prefix) and prefix:
            prefix = prefix[:-1]
    # Strip version suffixes: _v3, -rev1, _version2, trailing digits, etc.
    label = re.sub(r'[-_ ]*(v|ver|version|rev|r)\d*$', '', prefix, flags=re.IGNORECASE)
    # Strip any remaining trailing separators or digits
    label = re.sub(r'[-_ ]+\d*$', '', label)
    label = label.rstrip("-_ ")
    return label if label else group[0].stem


def _print_summary(result: dict, short: bool = False):
    meta = result.get("metadata", {})
    old_meta = meta.get("old", {})
    new_meta = meta.get("new", {})

    old_ts = old_meta.get("timestamp")
    new_ts = new_meta.get("timestamp")
    old_dt = _parse_timestamp(old_ts)

    print("FILES:")
    print(f"  Old: {_format_date(old_ts)}  "
          f"via {old_meta.get('application', '?')}")
    print(f"  New: {_format_date(new_ts, ref_dt=old_dt)}  "
          f"via {new_meta.get('application', '?')}")
    print()

    _print_changes(result, short=short)


def _print_step_summary(result: dict, short: bool = False,
                        is_first: bool = False):
    meta = result.get("metadata", {})
    old_meta = meta.get("old", {})
    new_meta = meta.get("new", {})

    old_ts = old_meta.get("timestamp")
    new_ts = new_meta.get("timestamp")
    old_dt = _parse_timestamp(old_ts)
    new_label = _format_date(new_ts, ref_dt=old_dt)

    if is_first:
        print(f"  {_format_date(old_ts)} · {new_label}  "
              f"via {new_meta.get('application', '?')}")
    else:
        print(f"  {new_label}  "
              f"via {new_meta.get('application', '?')}")

    _print_changes(result, indent="  ", short=short)


def _print_changes(result: dict, indent: str = "", short: bool = False):
    s = result["summary"]
    parts = []
    if s["added"]:
        parts.append(_green(f"{s['added']} added"))
    if s["deleted"]:
        parts.append(_red(f"{s['deleted']} deleted"))
    if s.get("bulk_moved"):
        parts.append(_magenta(f"{s['bulk_moved']} bulk-moved"))
    if s["changed"]:
        parts.append(_yellow(f"{s['changed']} changed"))
    parts.append(_dim(f"{s['unchanged']} unchanged"))
    print(f"{indent}{', '.join(parts)}\n")

    if short:
        _print_class_breakdown(result, indent)
        return

    # Bulk movements — compact summary
    for bm in result.get("bulk_movements", []):
        class_parts = ", ".join(
            f"{count} {human_class(cls)}" for cls, count in bm["class_breakdown"].items()
        )
        groups = bm.get("groups", [])
        group_suffix = f" in '{groups[0]}'" if groups else ""
        print(_magenta(f"{indent}BULK MOVEMENT ({bm['count']} entities{group_suffix}): {bm['description']}"))
        print(f"{indent}  {class_parts}")
        print()

    if result["added"]:
        print(_bold(_green(f"{indent}ADDED:")))
        for e in result["added"]:
            print(_green(f"{indent}  + [{human_class(e['ifc_class'])}] {e['name'] or '(unnamed)'} ({e['guid']})"))
        print()

    if result["deleted"]:
        print(_bold(_red(f"{indent}DELETED:")))
        for e in result["deleted"]:
            print(_red(f"{indent}  - [{human_class(e['ifc_class'])}] {e['name'] or '(unnamed)'} ({e['guid']})"))
        print()

    if result["changed"]:
        print(_bold(_yellow(f"{indent}CHANGED:")))
        for e in result["changed"]:
            print(_yellow(f"{indent}  ~ [{human_class(e['ifc_class'])}] {e['name'] or '(unnamed)'} ({e['guid']})"))
            for c in e["changes"]:
                if c["field"] == "placement":
                    desc = c.get("description", "(matrix changed)")
                    print(f"{indent}      {c['field']}: {_cyan(desc)}")
                else:
                    print(f"{indent}      {c['field']}: {_red(repr(c['old']))} → {_green(repr(c['new']))}")
        print()


def _print_class_breakdown(result: dict, indent: str = ""):
    """Print a compact class-level breakdown for --short mode."""
    from collections import Counter

    # Bulk movements
    for bm in result.get("bulk_movements", []):
        class_parts = ", ".join(
            f"{count} {human_class(cls)}" for cls, count in bm["class_breakdown"].items()
        )
        groups = bm.get("groups", [])
        group_suffix = f" in '{groups[0]}'" if groups else ""
        print(_magenta(f"{indent}BULK MOVEMENT ({bm['count']} entities{group_suffix}): {bm['description']}"))
        print(f"{indent}  {class_parts}")

    color_for = {"added": _green, "deleted": _red, "changed": _yellow}
    for section, symbol in [("added", "+"), ("deleted", "-"), ("changed", "~")]:
        entities = result[section]
        if not entities:
            continue
        counts = Counter(human_class(e["ifc_class"]) for e in entities)
        parts = ", ".join(
            f"{count} {cls}" for cls, count in counts.most_common()
        )
        print(color_for[section](f"{indent}{section.upper()}: {parts}"))

    # For changed entities, also show which fields are commonly changing
    if result["changed"]:
        field_counts: Counter = Counter()
        for e in result["changed"]:
            for c in e["changes"]:
                field_counts[c["field"]] += 1
        top_fields = ", ".join(
            f"{field} ({count})" for field, count in field_counts.most_common(10)
        )
        print(f"{indent}  fields: {top_fields}")

    print()
