"""STEP identity restoration, separate from semantic diff identity policy."""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from hashlib import sha256
from pathlib import Path
import os
import re
import tempfile

import ifcopenshell
from ifcopenshell import ifcopenshell_wrapper

from athar.bottom._native import native
from athar.bottom.signatures import build_signature_bundle
from athar.engine import SchemaMismatchError
from athar.matcher.core import match_signatures


class RekeyRefused(ValueError):
    def __init__(self, message, diagnostics):
        super().__init__(message)
        self.diagnostics = {"status": "refused", "message": message, **diagnostics}


# Mirrors ifcmerge's record reader, float normalisation and _dissemble tokenizer.
_STEP_LINE = re.compile(rb"^#([0-9]+)=(.*);")
_FLOAT = re.compile(rb"(-?\d*\.\d+(?:[eE][+-]?\d+)?|-?\d+\.(?:\d+)?(?:[eE][+-]?\d+)?)")
_CLASS_ATTRIBUTES = re.compile(rb"^([_0-9A-Za-z]+)\((.*)\)$")
_NAME = rb"[_0-9A-Za-z]+"
_COMMA = rb"[\s,]+"
_QUOTED = rb"'.*?'"
_SIMPLE = rb"[^',)(]+"
_PARAMVALUE = _NAME + rb"\(.*?\)"
_PARAMQUOTED = _NAME + rb"\('.*?'\)"
_BRACKETED = rb"\((?:" + rb"|".join((_COMMA, _PARAMQUOTED, _PARAMVALUE, _QUOTED, _SIMPLE)) + rb")*\)"
_TOKEN = re.compile(rb"(" + rb"|".join((_PARAMQUOTED, _PARAMVALUE, rb"^\(.*\)$", _BRACKETED, _QUOTED, _SIMPLE)) + rb")")


def _float(match):
    return b"%.10g" % float(match[1])


def _ifcmerge_records(path):
    with open(path, "rb") as stream:
        return {int(m[1]): m[2] for m in map(_STEP_LINE.match, stream) if m}


def _ifcmerge_splits(record, count):
    """Whether ifcmerge's attribute tokenizer recovers exactly COUNT attributes."""
    parts = _CLASS_ATTRIBUTES.match(record)
    if not parts:
        return False
    tokens = [re.sub(rb"\s*$", b"", re.sub(rb"^[\s,]+", b"", t)) for t in _TOKEN.findall(parts[2])]
    return len(tokens) == count and b",".join(tokens) == parts[2]


def _value(value, reference):
    if isinstance(value, ifcopenshell.entity_instance):
        if value.id():
            return reference(value.id())
        return (value.is_a(), tuple(_value(v, reference) for v in value))
    if isinstance(value, tuple):
        return tuple(_value(v, reference) for v in value)
    return value


class _Graph:
    def __init__(self, model):
        self.entities = {e.id(): e for e in sorted(model, key=lambda e: e.id())}
        self.values = {i: tuple(e) for i, e in self.entities.items()}
        self.guids = defaultdict(list)
        for i, e in self.entities.items():
            guid = getattr(e, "GlobalId", None)
            if guid:
                self.guids[guid].append(i)
        self.hashes = {}
        visiting = set()

        def digest(i):
            if i in self.hashes:
                return self.hashes[i]
            if i in visiting:
                raise ValueError("cyclic resource graph")
            e = self.entities[i]
            guid = getattr(e, "GlobalId", None)
            if guid and len(self.guids[guid]) == 1:
                payload = (e.is_a(), "guid", guid)
            else:
                visiting.add(i)
                try:
                    payload = (e.is_a(), _value(self.values[i], digest))
                finally:
                    visiting.remove(i)
            result = sha256(repr(payload).encode("utf-8")).digest()
            self.hashes[i] = result
            return result

        self.cycles = 0
        for i in self.entities:
            try:
                digest(i)
            except ValueError:
                self.cycles += 1
        self.incoming = defaultdict(lambda: defaultdict(list))
        for i, e in self.entities.items():
            if e.is_a("IfcRelationship"):
                for index, value in enumerate(self.values[i]):
                    for target in self.refs(value):
                        self.incoming[target][(e.is_a(), index)].append(i)

    def role(self, i):
        e = self.entities[i]
        names = []
        for index, value in enumerate(self.values[i]):
            for target in self.refs(value):
                child = self.entities[target]
                name = getattr(child, "Name", None)
                if name:
                    names.append((index, child.is_a(), name))
        return (e.is_a(), getattr(e, "Name", None), tuple(sorted(names)))

    def refs(self, value):
        result = []

        def collect(i):
            result.append(i)

        _value(value, collect)
        return result


def rekey_files(base_path: str, new_path: str, output_path: str) -> dict:
    """Write NEW with BASE ids where recoverable; return matching diagnostics."""
    output = Path(output_path)
    if output.resolve() in {Path(base_path).resolve(), Path(new_path).resolve()}:
        raise ValueError("output must not overwrite either input")
    base_model = ifcopenshell.open(base_path)
    new_model = ifcopenshell.open(new_path)
    if base_model.schema != new_model.schema:
        raise SchemaMismatchError(base_model.schema, new_model.schema)
    pairs, _, _, _ = match_signatures(
        build_signature_bundle(base_path).signatures,
        build_signature_bundle(new_path).signatures,
    )
    old, new = _Graph(base_model), _Graph(new_model)
    mapping, reverse, reasons = {}, {}, {}
    queue = deque()

    def match(a, b, reason):
        if a in reverse or b in mapping or old.entities[a].is_a() != new.entities[b].is_a():
            return
        mapping[b], reverse[a], reasons[b] = a, b, reason
        queue.append((a, b))

    for guid, ids in sorted(old.guids.items()):
        other = new.guids.get(guid, ())
        if len(ids) == len(other) == 1:
            match(ids[0], other[0], "guid")
    for pair in pairs:
        match(pair.old_step, pair.new_step, "signature")

    schema = ifcopenshell_wrapper.schema_by_name(new_model.schema)
    attributes = {}
    conflicts = 0

    def child_pairs(aa, bb, ordered):
        left, right = set(aa), set(bb)
        result = []
        for a in sorted(left):
            b = reverse.get(a)
            if b in right:
                left.remove(a)
                right.remove(b)

        def bucket(ids, graph, key):
            buckets = defaultdict(list)
            for i in sorted(ids):
                k = key(graph, i)
                if k is not None:
                    buckets[k].append(i)
            return buckets

        for key in (
            lambda g, i: g.hashes.get(i),
            lambda g, i: g.role(i),
        ):
            la, lb = bucket(left, old, key), bucket(right, new, key)
            for k in la.keys() & lb.keys():
                # Equal content is interchangeable; role-only matches must be unique.
                if len(la[k]) == len(lb[k]) == 1 or isinstance(k, bytes):
                    for a, b in zip(la[k], lb[k]):
                        result.append((a, b))
                        left.remove(a)
                        right.remove(b)
        if ordered and len(aa) == len(bb):
            result.extend((a, b) for a, b in zip(aa, bb) if a in left and b in right)
        return result

    def propagate():
        nonlocal conflicts
        while queue:
            proposals = set()
            while queue:
                a, b = queue.popleft()
                cls = new.entities[b].is_a()
                for role in old.incoming[a].keys() & new.incoming[b].keys():
                    proposals.update(child_pairs(old.incoming[a][role], new.incoming[b][role], False))
                if cls not in attributes:
                    attributes[cls] = schema.declaration_by_name(cls).all_attributes()
                for index, (av, bv) in enumerate(zip(old.values[a], new.values[b])):
                    aa, bb = old.refs(av), new.refs(bv)
                    if not aa or not bb:
                        continue
                    agg = attributes[cls][index].type_of_attribute().as_aggregation_type()
                    ordered = agg is None or agg.type_of_aggregation_string() in {"list", "array"}
                    for x, y in child_pairs(aa, bb, ordered):
                        if old.entities[x].is_a() == new.entities[y].is_a():
                            # Products cannot inherit the identity of a different role occupant.
                            gx = getattr(old.entities[x], "GlobalId", None)
                            gy = getattr(new.entities[y], "GlobalId", None)
                            if gx != gy and not (
                                old.entities[x].is_a("IfcPropertyDefinition")
                                or old.entities[x].is_a("IfcRelationship")
                            ):
                                continue
                            proposals.add((x, y))
            counts_a, counts_b = Counter(a for a, _ in proposals), Counter(b for _, b in proposals)
            for a, b in sorted(proposals):
                if a in reverse or b in mapping or counts_a[a] != 1 or counts_b[b] != 1:
                    conflicts += 1
                    continue
                match(a, b, "role")

    propagate()
    buckets = defaultdict(list)
    for a in old.entities:
        if a not in reverse and a in old.hashes:
            buckets[old.hashes[a]].append(a)
    new_buckets = defaultdict(list)
    for b in new.entities:
        if b not in mapping and b in new.hashes:
            new_buckets[new.hashes[b]].append(b)
    for key in sorted(buckets.keys() & new_buckets.keys()):
        for a, b in zip(buckets[key], new_buckets[key]):
            match(a, b, "content")
    # Root GUID is intentionally excluded from exact resource content, but
    # retained in the serialized NEW entity. This recovers resources whose
    # exporter regenerated a GUID without treating that GUID as identity.
    by_role = defaultdict(list)
    for a in old.entities:
        if a not in reverse:
            by_role[old.role(a)].append(a)
    for b in sorted(new.entities):
        if b in mapping:
            continue
        candidates = by_role.get(new.role(b), ())
        if len(candidates) == 1 and candidates[0] not in reverse:
            match(candidates[0], b, "role")
    propagate()

    matched = dict(mapping)
    next_id = max(old.entities, default=0) + 1
    for b in new.entities:
        if b not in mapping:
            mapping[b] = next_id
            next_id += 1

    classes = defaultdict(Counter)
    for b, e in new.entities.items():
        classes[e.is_a()]["total"] += 1
        classes[e.is_a()][reasons.get(b, "fresh")] += 1
    stats = {
        "entities": len(new.entities), "matched": len(matched),
        "fresh": len(new.entities) - len(matched), "base_max_id": max(old.entities, default=0),
        "matched_by": dict(Counter(reasons.values())), "role_conflicts": conflicts,
        "cyclic_content_unavailable": {"base": old.cycles, "new": new.cycles},
        "classes": {k: dict(v) for k, v in sorted(classes.items())},
    }
    rooted_additions = [b for b, e in new.entities.items() if b not in matched and e.is_a("IfcRoot")]
    if rooted_additions:
        # Independent two-way rekeys cannot deduplicate roots added by both branches.
        raise RekeyRefused("unmatched rooted additions are unsafe for ifcmerge", {
            **stats, "unsafe_rooted_additions": rooted_additions,
        })

    fd, temporary = tempfile.mkstemp(suffix=".ifc", dir=output.parent)
    os.close(fd)
    try:
        native().rekey_step_file(str(new_path), temporary, mapping)
        before, after = _ifcmerge_records(base_path), _ifcmerge_records(temporary)
        unmergeable = []
        for b, a in sorted(matched.items()):
            x, y = before.get(a), after.get(a)
            if x is None or y is None:
                unmergeable.append(b)
                continue
            if x == y or _FLOAT.sub(_float, x) == _FLOAT.sub(_float, y):
                continue
            count = len(old.values[a])
            if not (_ifcmerge_splits(x, count) and _ifcmerge_splits(y, count)):
                unmergeable.append(b)
        if unmergeable:
            # ifcmerge merges a record changed on both sides attribute by attribute.
            raise RekeyRefused("changed records that ifcmerge cannot split into attributes", {
                **stats, "unmergeable_changes": unmergeable,
            })
        reopened = ifcopenshell.open(temporary)
        if len(list(reopened)) != len(new.entities):
            raise ValueError("rekey changed the entity count")
        for b, e in new.entities.items():
            restored = reopened.by_id(mapping[b])
            expected = _value(new.values[b], lambda i: ("ref", mapping[i]))
            actual = _value(tuple(restored), lambda i: ("ref", i))
            if restored.is_a() != e.is_a() or expected != actual:
                raise ValueError(f"rekey changed attributes or references of #{b}")
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

    return stats
