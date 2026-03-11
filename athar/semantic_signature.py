"""Soft signature generation for candidate blocking (S:)."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any


def semantic_signature(entity: dict) -> str:
    payload = semantic_payload(entity)
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def semantic_payload(entity: dict) -> dict:
    attributes = entity.get("attributes", {})
    shaped_attrs = {name: _shape(value) for name, value in attributes.items()}
    return {
        "entity_type": entity.get("entity_type"),
        "attributes": shaped_attrs,
        "edges": _edge_signature(entity.get("refs", [])),
    }


def _shape(value: Any) -> Any:
    if not isinstance(value, dict):
        return {"kind": "raw"}
    kind = value.get("kind")
    if kind in {"null", "bool", "int", "real", "string"}:
        return {"kind": kind}
    if kind == "simple":
        return {"kind": "simple", "type": value.get("type")}
    if kind == "select":
        return {
            "kind": "select",
            "type": value.get("type"),
            "value": _shape(value.get("value")),
        }
    if kind in {"list", "set", "bag"}:
        items = value.get("items", [])
        item_shapes = [_shape(item) for item in items]
        counts = Counter(json.dumps(s, sort_keys=True, separators=(",", ":")) for s in item_shapes)
        return {
            "kind": kind,
            "len": len(items),
            "item_kinds": [
                {"shape": shape, "count": count}
                for shape, count in sorted(counts.items())
            ],
        }
    if kind == "ref":
        return {"kind": "ref"}
    return {"kind": kind or "unknown"}


def _edge_signature(refs: list[dict]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str | None]] = Counter()
    for ref in refs:
        counts[(ref.get("path", ""), ref.get("target_type"))] += 1
    edges = [
        {"path": path, "target_type": target_type, "count": count}
        for (path, target_type), count in counts.items()
    ]
    edges.sort(key=lambda item: (item["path"], item["target_type"] or "", item["count"]))
    return edges
