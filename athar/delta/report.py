"""Phase 1 per-aspect delta report assembly."""

from __future__ import annotations

from athar.bottom.types import SignatureBundle, SignatureVector
from athar.matcher.types import MatchedPair


def build_delta_report(
    old_bundle: SignatureBundle,
    new_bundle: SignatureBundle,
    matches: list[MatchedPair],
    unmatched_old: list[int],
    unmatched_new: list[int],
) -> dict:
    """Build a structured change report for Phase 1."""
    old_signatures = old_bundle.signatures
    new_signatures = new_bundle.signatures

    modified: list[dict] = []
    unchanged: list[dict] = []

    for pair in matches:
        old_sig = old_signatures.get(pair.old_step)
        new_sig = new_signatures.get(pair.new_step)
        if old_sig is None or new_sig is None:
            continue
        aspects = _aspect_diff(old_sig, new_sig)
        item = {
            "old": _entity_summary(old_sig),
            "new": _entity_summary(new_sig),
            "match": {"score": pair.score, "reason": pair.reason},
            "aspects": aspects,
        }
        if any(value == "changed" for key, value in aspects.items() if key != "placement_delta_mm"):
            modified.append(item)
        else:
            unchanged.append(item)

    added = [_entity_summary(new_signatures[step]) for step in unmatched_new if step in new_signatures]
    deleted = [_entity_summary(old_signatures[step]) for step in unmatched_old if step in old_signatures]

    modified.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    unchanged.sort(key=lambda item: (item["new"]["class"], item["new"]["step_id"]))
    added.sort(key=lambda item: (item["class"], item["step_id"]))
    deleted.sort(key=lambda item: (item["class"], item["step_id"]))

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
        },
        "added": added,
        "deleted": deleted,
        "modified": modified,
        "unchanged": unchanged,
    }


def _entity_summary(sig: SignatureVector) -> dict:
    return {
        "step_id": sig.step_id,
        "guid": sig.guid,
        "class": sig.canonical_class,
        "entity_type": sig.entity_type,
    }


def _aspect_diff(old_sig: SignatureVector, new_sig: SignatureVector) -> dict:
    placement_delta = _placement_delta_mm(old_sig.placement, new_sig.placement)
    return {
        "geometry": _changed(old_sig.vh_geometry, new_sig.vh_geometry),
        "data": _changed(old_sig.vh_data, new_sig.vh_data),
        "topology": _changed(old_sig.vh_topology, new_sig.vh_topology),
        "placement": _changed(old_sig.placement, new_sig.placement),
        "placement_delta_mm": placement_delta,
    }


def _changed(old, new) -> str:
    return "unchanged" if old == new else "changed"


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
