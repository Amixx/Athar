"""End-to-end metamorphic invariants for the signature pipeline.

Signatures must depend only on semantic content, not on incidental STEP
encoding. Two transformations that preserve meaning must produce identical
signature vectors:

- **renumbering** every ``#id`` (entity ids carry no identity; they are
  excluded from every hash), and
- **reordering** the DATA-section records (the pipeline sorts everything it
  hashes, so record order is irrelevant).

These drive the real native pipeline through ``build_signature_bundle`` on a
small corpus fixture, replacing the toy-``ParseResult`` metamorphic tests that
exercised the former pure-Python stages.
"""

from __future__ import annotations

import random

from athar.bottom.signatures import build_signature_bundle

from .corpus import corpus_path

_FIXTURE_KEY = "bl_v1"


def _renumber(text: str, offset: int) -> str:
    """Shift every ``#id`` (definitions and references) by ``offset``.

    Quote-aware: a ``#`` inside a string literal (e.g. an element Name or Tag)
    is left untouched so the transform preserves semantic content.
    """
    out: list[str] = []
    i = 0
    n = len(text)
    in_string = False
    while i < n:
        ch = text[i]
        if ch == "'":
            out.append(ch)
            if in_string and i + 1 < n and text[i + 1] == "'":
                out.append(text[i + 1])  # escaped quote, stay in string
                i += 2
                continue
            in_string = not in_string
            i += 1
            continue
        if ch == "#" and not in_string:
            j = i + 1
            while j < n and text[j].isdigit():
                j += 1
            if j > i + 1:
                out.append("#")
                out.append(str(int(text[i + 1 : j]) + offset))
                i = j
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_records(body: str) -> tuple[list[str], str]:
    """Split a DATA body into records on top-level ``;`` (quote-aware)."""
    records: list[str] = []
    current: list[str] = []
    in_string = False
    i = 0
    while i < len(body):
        ch = body[i]
        current.append(ch)
        if ch == "'":
            if in_string and i + 1 < len(body) and body[i + 1] == "'":
                current.append(body[i + 1])
                i += 2
                continue
            in_string = not in_string
        elif ch == ";" and not in_string:
            records.append("".join(current))
            current = []
        i += 1
    return records, "".join(current)


def _reorder(text: str, seed: int) -> str:
    """Shuffle the DATA-section records, preserving everything else."""
    pre, sep, rest = text.partition("DATA;")
    assert sep, "fixture has no DATA; section"
    body, sep2, post = rest.partition("ENDSEC;")
    assert sep2, "fixture has no ENDSEC; after DATA;"
    records, trailing = _split_records(body)
    random.Random(seed).shuffle(records)
    return pre + "DATA;" + "".join(records) + trailing + "ENDSEC;" + post


def _signature_fingerprint(bundle) -> list[tuple]:
    """Order-independent, id-independent fingerprint keyed by identity fields."""
    return sorted(
        (
            sig.guid,
            sig.canonical_class,
            sig.vh_geometry,
            sig.vh_data,
            sig.vh_topology,
            sig.placement,
            sig.centroid,
            sig.aabb,
        )
        for sig in bundle.signatures.values()
        if sig.guid
    )


def _bundle_for(text: str, tmp_path, name: str):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return build_signature_bundle(str(path))


def test_step_id_renumbering_is_signature_invariant(tmp_path) -> None:
    source = open(corpus_path(_FIXTURE_KEY), encoding="utf-8", errors="surrogateescape").read()

    base = _bundle_for(source, tmp_path, "base.ifc")
    renumbered = _bundle_for(_renumber(source, 1_000_000), tmp_path, "renumbered.ifc")

    assert _signature_fingerprint(base)
    assert _signature_fingerprint(renumbered) == _signature_fingerprint(base)


def test_record_reordering_is_signature_invariant(tmp_path) -> None:
    source = open(corpus_path(_FIXTURE_KEY), encoding="utf-8", errors="surrogateescape").read()

    base = _bundle_for(source, tmp_path, "base.ifc")
    reordered = _bundle_for(_reorder(source, seed=7), tmp_path, "reordered.ifc")

    assert _signature_fingerprint(base)
    assert _signature_fingerprint(reordered) == _signature_fingerprint(base)
