"""Per-aspect delta report assembly."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict

from athar.bottom._native import native
from athar.bottom.types import Coverage, OrphanPropertySet, PropertyEntry, SignatureBundle, SignatureVector
from athar.matcher.types import MatchedPair


def build_delta_report(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    matches: list[MatchedPair],
    unmatched_old: list[int],
    unmatched_new: list[int],
) -> dict:
    """Build a structured change report."""
    old_signatures = old_bundle.signatures
    new_signatures = new_bundle.signatures
    old_props = old_bundle.property_index
    new_props = new_bundle.property_index

    modified: list[dict] = []
    unchanged: list[dict] = []
    guid_changed: list[dict] = []
    dropped_matches = 0
    new_step_of = {pair.old_step: pair.new_step for pair in matches}
    matched_new = set(new_step_of.values())

    for pair in matches:
        old_sig = old_signatures.get(pair.old_step)
        new_sig = new_signatures.get(pair.new_step)
        if old_sig is None or new_sig is None:
            dropped_matches += 1
            continue
        aspects = _aspect_diff(old_sig, new_sig)
        relations, relations_inherited = _relation_changes(
            old_bundle, new_bundle, old_sig, new_sig, new_step_of, matched_new
        )
        aspects["relations"] = "changed" if relations else "unchanged"
        aspects["class"] = _changed(old_sig.canonical_class, new_sig.canonical_class)
        mover = None
        if aspects["placement"] == "changed":
            mover = _placement_mover(old_signatures, new_signatures, old_sig, new_sig, new_step_of)
        item = {
            "old": _entity_summary(old_sig),
            "new": _entity_summary(new_sig),
            "match": {"score": pair.score, "reason": pair.reason},
            "aspects": aspects,
            "data_hash": {
                "old": old_sig.vh_data,
                "new": new_sig.vh_data,
            },
            "change_scope": _change_scope(
                aspects, inherited_placement=mover is not None, inherited_relations=relations_inherited
            ),
        }
        if mover is not None:
            item["moved_with"] = _entity_summary(mover)
        if pair.reason == "geometry_hash" and old_sig.guid != new_sig.guid:
            item["identity"] = {"guid_changed": True, "old_guid": old_sig.guid, "new_guid": new_sig.guid}
            guid_changed.append({"old": item["old"], "new": item["new"]})
        if relations:
            item["relations"] = relations
        if aspects["class"] == "changed":
            item["class"] = {"old_class": old_sig.canonical_class, "new_class": new_sig.canonical_class}
        if aspects["data"] == "changed":
            item["data_delta"] = _data_delta(old_sig, new_sig)
            if old_props is not None and new_props is not None:
                deltas = _property_deltas(
                    old_props.get(pair.old_step, []),
                    new_props.get(pair.new_step, []),
                )
                if deltas:
                    item["property_deltas"] = deltas
        if any(value == "changed" for key, value in aspects.items() if key != "placement_delta_mm"):
            modified.append(item)
        else:
            unchanged.append(item)

    added = [_entity_summary(new_signatures[step]) for step in unmatched_new if step in new_signatures]
    deleted = [_entity_summary(old_signatures[step]) for step in unmatched_old if step in old_signatures]

    modified.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    unchanged.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    guid_changed.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    added.sort(key=lambda item: (item["class"], item["step_id"]))
    deleted.sort(key=lambda item: (item["class"], item["step_id"]))
    orphans_added, orphans_removed = _orphan_property_set_diff(
        old_bundle.orphan_property_sets, new_bundle.orphan_property_sets
    )

    return {
        "engine": "athar",
        "canon_version": old_bundle.canon_version,
        "schemas": {"old": old_bundle.schema, "new": new_bundle.schema},
        "stats": {
            "added": len(added),
            "deleted": len(deleted),
            "modified": len(modified),
            "unchanged": len(unchanged),
            "old_signatures": len(old_signatures),
            "new_signatures": len(new_signatures),
            "old_diagnostics": _diagnostic_summary(old_bundle),
            "new_diagnostics": _diagnostic_summary(new_bundle),
            "old_edge_stats": old_bundle.edge_stats,
            "new_edge_stats": new_bundle.edge_stats,
            "modified_change_scope": _scope_stats(modified),
            "placement_cohorts": _placement_cohorts(modified, old_signatures, new_signatures),
            "dropped_matches": dropped_matches,
            "guid_changed": len(guid_changed),
            "orphan_property_sets": {
                "old": len(old_bundle.orphan_property_sets),
                "new": len(new_bundle.orphan_property_sets),
                "added": len(orphans_added),
                "removed": len(orphans_removed),
            },
            "coverage": _coverage_stats(old_bundle.coverage, new_bundle.coverage),
        },
        "added": added,
        "deleted": deleted,
        "modified": modified,
        "unchanged": unchanged,
        "guid_changed": guid_changed,
        "orphan_property_sets": {"added": orphans_added, "removed": orphans_removed},
    }


def _coverage_stats(old: Coverage, new: Coverage) -> dict:
    """Per-file coverage blocks plus the classes whose unreached content differs."""

    def block(coverage: Coverage) -> dict:
        return {
            "unknown_relationships": dict(sorted(coverage.unknown_relationships.items())),
            "unreached": {
                name: {"count": count, "digest": digest}
                for name, (count, digest) in sorted(coverage.unreached.items())
            },
            "ignored_by_design": dict(sorted(coverage.ignored_by_design.items())),
        }

    return {
        "old": block(old),
        "new": block(new),
        "unexamined_changes": sorted(
            name
            for name in old.unreached.keys() | new.unreached.keys()
            if old.unreached.get(name) != new.unreached.get(name)
        ),
    }


def _orphan_property_set_diff(
    old_orphans: tuple[OrphanPropertySet, ...],
    new_orphans: tuple[OrphanPropertySet, ...],
) -> tuple[list[dict], list[dict]]:
    """Multiset difference of orphan property sets by content, GlobalId ignored.

    Surplus copies of one content are reported from the end of the file.
    """
    old_by_key: dict[str, list[OrphanPropertySet]] = defaultdict(list)
    new_by_key: dict[str, list[OrphanPropertySet]] = defaultdict(list)
    for orphan in old_orphans:
        old_by_key[orphan.content_key].append(orphan)
    for orphan in new_orphans:
        new_by_key[orphan.content_key].append(orphan)

    added: list[OrphanPropertySet] = []
    removed: list[OrphanPropertySet] = []
    for key in old_by_key.keys() | new_by_key.keys():
        old_items = sorted(old_by_key.get(key, ()), key=lambda item: item.step_id)
        new_items = sorted(new_by_key.get(key, ()), key=lambda item: item.step_id)
        surplus = len(new_items) - len(old_items)
        if surplus > 0:
            added.extend(new_items[-surplus:])
        elif surplus < 0:
            removed.extend(old_items[surplus:])

    def summary(orphan: OrphanPropertySet) -> dict:
        return {
            "step_id": orphan.step_id,
            "guid": orphan.guid,
            "name": orphan.name,
            "entity_type": orphan.entity_type,
        }

    return (
        [summary(item) for item in sorted(added, key=lambda item: item.step_id)],
        [summary(item) for item in sorted(removed, key=lambda item: item.step_id)],
    )


def _entity_summary(sig: SignatureVector) -> dict:
    summary = {
        "step_id": sig.step_id,
        "guid": sig.guid,
        "name": sig.name,
        "class": sig.canonical_class,
        "entity_type": sig.entity_type,
    }
    if sig.placement_unresolved is not None:
        summary["placement_unresolved"] = True
    return summary


def _relation_changes(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    old_sig: SignatureVector,
    new_sig: SignatureVector,
    new_step_of: dict[int, int],
    matched_new: set[int],
) -> tuple[list[dict], bool]:
    """Relations of a matched pair whose targets are not the same entities.

    A target with a signature is the same entity when it is matched to the
    new target. A target without one (a type object) is the same when the
    GlobalIds are equal or, like interchangeable entities in the matcher,
    when class and content are equal; a GlobalId several entities of its
    file carry identifies none of them. Each entry lists the targets only
    the old side names and the targets only the new side names.

    The flag is true when every listed target is an added or deleted entity:
    the pair then differs only because a neighbour appeared or disappeared.
    """
    if not old_sig.relation_targets and not new_sig.relation_targets:
        return [], False

    def by_relation(bundle: SignatureBundle, sig: SignatureVector, matched: dict[int, int] | None) -> dict:
        """Per (relation, role): target identity -> (summary, content, unmatched)."""
        out: dict[tuple[str, str], dict[tuple, tuple]] = defaultdict(dict)
        for target in sig.relation_targets:
            root = bundle.signatures.get(target.step_id)
            if root is not None:
                if matched is None:
                    identity = ("signature", target.step_id)
                    unmatched = target.step_id not in matched_new
                else:
                    step = matched.get(target.step_id)
                    identity = ("signature", step) if step is not None else ("unmatched", target.step_id)
                    unmatched = step is None
                out[(target.relation, target.role)][identity] = (_entity_summary(root), None, unmatched)
            else:
                related = bundle.related_entities[target.step_id]
                summary = {
                    "step_id": target.step_id,
                    "guid": related.guid,
                    "name": related.name,
                    "class": related.canonical_class,
                }
                content = (related.canonical_class, related.content)
                if related.guid is None or related.guid_shared:
                    identity = ("related", matched is None, target.step_id)
                else:
                    identity = ("guid", related.guid)
                out[(target.relation, target.role)][identity] = (summary, content, False)
        return out

    def order(target: tuple) -> tuple:
        summary = target[0]
        return (summary["class"], summary["guid"] or "", summary["step_id"])

    old_targets = by_relation(old_bundle, old_sig, new_step_of)
    new_targets = by_relation(new_bundle, new_sig, None)
    changes: list[dict] = []
    inherited = True
    for key in sorted(old_targets.keys() | new_targets.keys()):
        old = old_targets.get(key, {})
        new = new_targets.get(key, {})
        only_old = sorted((target for identity, target in old.items() if identity not in new), key=order)
        only_new = sorted((target for identity, target in new.items() if identity not in old), key=order)
        equal_content = Counter(content for _, content, _ in only_old if content is not None) & Counter(
            content for _, content, _ in only_new if content is not None
        )
        for side in (only_old, only_new):
            left = Counter(equal_content)
            kept = []
            for target in side:
                if left[target[1]] > 0:
                    left[target[1]] -= 1
                else:
                    kept.append(target)
            side[:] = kept
        if only_old or only_new:
            inherited = inherited and all(unmatched for _, _, unmatched in (*only_old, *only_new))
            changes.append(
                {
                    "relation": key[0],
                    "role": key[1],
                    "old": [summary for summary, _, _ in only_old],
                    "new": [summary for summary, _, _ in only_new],
                }
            )
    return changes, bool(changes) and inherited


def _aspect_diff(old_sig: SignatureVector, new_sig: SignatureVector) -> dict:
    placement_delta = _placement_delta_mm(old_sig.placement, new_sig.placement)
    return {
        "geometry": _changed(old_sig.vh_geometry, new_sig.vh_geometry),
        "data": _changed(old_sig.vh_data, new_sig.vh_data),
        "topology": _changed(old_sig.vh_topology, new_sig.vh_topology),
        "placement": _changed(
            (old_sig.placement, old_sig.placement_unresolved),
            (new_sig.placement, new_sig.placement_unresolved),
        ),
        "placement_delta_mm": placement_delta,
    }


def _changed(old, new) -> str:
    return "unchanged" if old == new else "changed"


def _data_delta(old_sig: SignatureVector, new_sig: SignatureVector) -> list[dict]:
    old_facts = dict(old_sig.data_facts)
    new_facts = dict(new_sig.data_facts)
    out: list[dict] = []
    if old_sig.canonical_class != new_sig.canonical_class:
        # The facts of the entity itself are compared under the new class.
        old_label = f"{old_sig.canonical_class}[GlobalId={old_sig.guid}]."
        new_label = f"{new_sig.canonical_class}[GlobalId={new_sig.guid}]."
        old_facts = {
            (new_label + path[len(old_label) :] if path.startswith(old_label) else path): value
            for path, value in old_facts.items()
        }
        out.append({"path": f"{new_label}(class)", "old": old_sig.canonical_class, "new": new_sig.canonical_class})
    for path in sorted(set(old_facts) | set(new_facts)):
        old_value = old_facts.get(path)
        new_value = new_facts.get(path)
        if old_value == new_value:
            continue
        out.append({"path": path, "old": old_value, "new": new_value})
    if not out:
        # The same values under the same paths, attached differently (an
        # unordered member moved between two owners of one class that have
        # no GlobalId).
        label = new_sig.canonical_class if new_sig.guid is None else f"{new_sig.canonical_class}[GlobalId={new_sig.guid}]"
        out.append({"path": f"{label}.(structure)", "old": old_sig.vh_data[:12], "new": new_sig.vh_data[:12]})
    return out


def _placement_delta_mm(old: tuple[int, ...] | None, new: tuple[int, ...] | None) -> tuple[float, float, float] | None:
    if old is None or new is None:
        return None
    if len(old) < 12 or len(new) < 12:
        return None
    # Translation entries in row-major 4x4 matrix.
    dx_m = (new[3] - old[3]) / 1_000_000.0
    dy_m = (new[7] - old[7]) / 1_000_000.0
    dz_m = (new[11] - old[11]) / 1_000_000.0
    return (round(dx_m * 1000.0, 3), round(dy_m * 1000.0, 3), round(dz_m * 1000.0, 3))


def _diagnostic_summary(bundle: SignatureBundle) -> dict:
    return {
        "dangling_refs": bundle.diagnostics.dangling_refs,
        "cycle_breaks": bundle.diagnostics.cycle_breaks,
        "warnings": list(bundle.diagnostics.warnings),
    }


def _placement_mover(
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
    old_sig: SignatureVector,
    new_sig: SignatureVector,
    new_step_of: dict[int, int],
) -> SignatureVector | None:
    """The new-side ancestor a pair whose world placement changed moved with.

    Walks the placement parents of both sides together and stops at the
    first pair whose own local placement differs or whose parents are not
    matched to each other. ``None`` when that pair is the given one, or when
    the chain ends without such a pair.
    """
    start = new_sig.step_id
    visited = {start}
    while old_sig.local_placement == new_sig.local_placement:
        old_parent, new_parent = old_sig.placement_parent, new_sig.placement_parent
        if old_parent is None and new_parent is None:
            return None
        if old_parent is None or new_parent is None or new_step_of.get(old_parent) != new_parent:
            break
        if new_parent in visited:
            return None
        visited.add(new_parent)
        old_sig, new_sig = old_signatures[old_parent], new_signatures[new_parent]
    return None if new_sig.step_id == start else new_sig


def _change_scope(aspects: dict, *, inherited_placement: bool, inherited_relations: bool) -> str:
    """Whether the pair itself changed, its surroundings did, or both.

    ``inherited_placement``: the placement changed only through an ancestor.
    ``inherited_relations``: the relations differ only in added or deleted
    targets.
    """
    intrinsic_changed = (
        any(aspects.get(name) == "changed" for name in ("geometry", "data", "class"))
        or (aspects.get("placement") == "changed" and not inherited_placement)
        or (aspects.get("relations") == "changed" and not inherited_relations)
    )
    transitive_changed = aspects.get("topology") == "changed" or inherited_placement or inherited_relations
    if intrinsic_changed and transitive_changed:
        return "mixed"
    if intrinsic_changed:
        return "intrinsic"
    if transitive_changed:
        return "transitive"
    return "none"


def _placement_cohorts(
    modified: list[dict],
    old_signatures: dict[int, SignatureVector],
    new_signatures: dict[int, SignatureVector],
) -> list[dict]:
    groups: dict[tuple[int, ...], list[dict]] = defaultdict(list)
    identity = (100_000, 0, 0, 0, 100_000, 0, 0, 0, 100_000)
    for item in modified:
        if item["aspects"]["placement"] != "changed":
            continue
        old = old_signatures[item["old"]["step_id"]]
        new = new_signatures[item["new"]["step_id"]]
        if old.placement is None or new.placement is None or old.placement_unresolved or new.placement_unresolved:
            continue
        motion = native().placement_motion(old.placement, new.placement)
        rotation = tuple(round(motion[4 * i + j] * 100_000) for i in range(3) for j in range(3))
        translation = tuple(round(motion[i] * 1_000) for i in (3, 7, 11))
        if rotation == identity and translation == (0, 0, 0):
            continue
        groups[rotation + translation].append(item)
    cohorts = []
    for key, items in groups.items():
        if len(items) < 2:
            continue
        items.sort(key=lambda item: item["new"]["step_id"])
        causes = [item.get("moved_with", item["new"]) for item in items]
        root = causes[0] if len({cause["step_id"] for cause in causes}) == 1 else None
        rotation = [value / 100_000 for value in key[:9]]
        angle = math.degrees(
            math.atan2(
                math.hypot(rotation[7] - rotation[5], rotation[2] - rotation[6], rotation[3] - rotation[1]) / 2,
                (rotation[0] + rotation[4] + rotation[8] - 1) / 2,
            )
        )
        delta = [float(value) for value in key[9:]]
        if root is not None:
            mover = next((item for item in modified if item["new"]["step_id"] == root["step_id"]), None)
            if mover is not None and mover["aspects"]["placement_delta_mm"] is not None:
                delta = [round(value, 1) + 0.0 for value in mover["aspects"]["placement_delta_mm"]]
        cohort = {
            "delta_mm": delta, "rotation_deg": round(angle, 4),
            "count": len(items), "members": [item["new"]["step_id"] for item in items],
        }
        if root is not None:
            cohort["root_cause"] = root
        cohorts.append(cohort)
    cohorts.sort(key=lambda cohort: (-cohort["count"], cohort["delta_mm"], cohort["rotation_deg"], cohort["members"]))
    return cohorts


def _scope_stats(modified: list[dict]) -> dict[str, int]:
    out = {"intrinsic": 0, "transitive": 0, "mixed": 0}
    for item in modified:
        scope = item.get("change_scope")
        if scope in out:
            out[scope] += 1
    return out


def _property_deltas(
    old_entries: list[PropertyEntry],
    new_entries: list[PropertyEntry],
) -> dict | None:
    """Compare old and new properties per (pset name, property name).

    A key may have several definitions: on the occurrence and on its type,
    or in two sets of one name. The effective value is the occurrence's when
    it defines the key, the type's otherwise; definitions of the effective
    origin that disagree make it the sorted list of their distinct values.
    Values compare in SI; ``old_written`` / ``new_written`` carry the values
    as the files write them where those differ.
    """

    def by_key(entries: list[PropertyEntry]) -> dict[tuple[str, str], list[PropertyEntry]]:
        out: dict[tuple[str, str], list[PropertyEntry]] = defaultdict(list)
        for entry in entries:
            out[(entry.pset, entry.name)].append(entry)
        return out

    def encoded(value) -> str:
        return json.dumps(value, sort_keys=True, default=str)

    def distinct(values: list) -> object:
        unique = {encoded(value): value for value in values}
        if len(unique) == 1:
            return next(iter(unique.values()))
        return [unique[key] for key in sorted(unique)]

    def value_of(definitions: list[PropertyEntry]):
        return distinct([entry.value for entry in definitions])

    def written_of(definitions: list[PropertyEntry]):
        return distinct([entry.value if entry.written is None else entry.written for entry in definitions])

    def deciding(definitions: list[PropertyEntry]) -> list[PropertyEntry]:
        return [entry for entry in definitions if not entry.inherited] or definitions

    def effective(definitions: list[PropertyEntry]) -> tuple[object, bool, str | None]:
        decided = deciding(definitions)
        return value_of(decided), decided[0].inherited, decided[0].unit

    def with_written(out: dict, side: str, definitions: list[PropertyEntry]) -> dict:
        written = written_of(definitions)
        if encoded(written) != encoded(out[f"{side}_value"]):
            out[f"{side}_written"] = written
        return out

    def origin(inherited: bool) -> str:
        return "type" if inherited else "occurrence"

    def listed(definitions: list[PropertyEntry]) -> list[dict]:
        rows = [
            {"origin": origin(entry.inherited), "pset_guid": entry.pset_guid, "value": entry.value}
            | ({} if entry.written is None else {"written": entry.written})
            for entry in definitions
        ]
        return sorted(rows, key=lambda row: (row["origin"], row["pset_guid"] or "", encoded(row["value"])))

    def entry(key: tuple[str, str], inherited: bool, unit: str | None = None, **values) -> dict:
        out = {"pset": key[0], "name": key[1], "inherited": inherited, **values}
        if unit is not None:
            out["unit"] = unit
        return out

    old_keys = by_key(old_entries)
    new_keys = by_key(new_entries)
    result: dict[str, list[dict]] = defaultdict(list)
    for key in old_keys.keys() | new_keys.keys():
        old_definitions = old_keys.get(key)
        new_definitions = new_keys.get(key)
        if old_definitions is None:
            value, inherited, unit = effective(new_definitions)
            added = with_written(entry(key, inherited, unit, new_value=value), "new", deciding(new_definitions))
            if len(new_definitions) > 1:
                added["new_definitions"] = listed(new_definitions)
            result["added"].append(added)
            continue
        if new_definitions is None:
            value, inherited, unit = effective(old_definitions)
            removed = with_written(entry(key, inherited, unit, old_value=value), "old", deciding(old_definitions))
            if len(old_definitions) > 1:
                removed["old_definitions"] = listed(old_definitions)
            result["removed"].append(removed)
            continue

        old_value, old_inherited, old_unit = effective(old_definitions)
        new_value, new_inherited, unit = effective(new_definitions)
        if old_value != new_value or old_unit != unit:
            changed = entry(key, new_inherited, unit, old_value=old_value, new_value=new_value)
            with_written(changed, "old", deciding(old_definitions))
            with_written(changed, "new", deciding(new_definitions))
            if old_unit != unit:
                changed["old_unit"], changed["unit"] = old_unit, unit
            if len(old_definitions) > 1 or len(new_definitions) > 1:
                changed["old_definitions"] = listed(old_definitions)
                changed["new_definitions"] = listed(new_definitions)
            result["changed"].append(changed)
        if old_inherited != new_inherited:
            result["origin_changed"].append(
                entry(key, new_inherited, old_origin=origin(old_inherited), new_origin=origin(new_inherited))
            )
        elif not new_inherited:
            old_type = [e for e in old_definitions if e.inherited]
            new_type = [e for e in new_definitions if e.inherited]
            if old_type and new_type and value_of(old_type) != value_of(new_type):
                overridden = entry(key, True, new_type[0].unit, old_value=value_of(old_type), new_value=value_of(new_type))
                with_written(overridden, "old", old_type)
                result["overridden_changed"].append(with_written(overridden, "new", new_type))
        for inherited in (False, True):
            old_count = sum(1 for e in old_definitions if e.inherited == inherited)
            new_count = sum(1 for e in new_definitions if e.inherited == inherited)
            if old_count != new_count:
                result["count_changed"].append(entry(key, inherited, old_count=old_count, new_count=new_count))

    if not result:
        return None

    def order(item: dict) -> tuple[str, str, bool]:
        return (item["name"], item["pset"], item["inherited"])

    return {
        kind: sorted(result[kind], key=order)
        for kind in ("changed", "added", "removed", "origin_changed", "overridden_changed", "count_changed")
        if kind in result
    }
