# Design: native Stage B — Rust signature pipeline

Goal: kill both the parse-time ceiling (~72% of bundle build) and the peak-RSS
ceiling (~2 GB on a 50 MB model, ~3.3 GB on the 77 MB faceted-BRep model) by
running the whole bottom layer natively and never materializing the ~1M+
`ParsedEntity` objects in Python. Only the ~thousands of product/spatial
`SignatureVector`s cross back to Python.

Back-compat is **not** required (maintainer decision, 2026-06-26): the native
pipeline may define its own canonical form. `athar_store` is content-addressed
and re-diffs live, so there are no persisted hashes to invalidate; no test
hardcodes golden hashes. Acceptance is **structural diff correctness** (the
existing invariant + metamorphic suite) and run-to-run determinism, not
byte-identity with the old Python output. Native becomes the canonical
implementation under its own `CANON_VERSION`.

## Why not FFI into ifcopenshell's C++ (measured 2026-06-26)

On `uni-project-house-50mb.ifc` (1,051,850 entities):

| step | time |
|---|---:|
| `ifcopenshell.open()` (C++ STEP parse) | 2.29s |
| iterate all entities (SWIG) | 2.45s |
| **full `parse_ifc()`** | **41.42s** |
| → athar's Python `_extract_entity` canonicalization | **~39s** |
| peak RSS | ~1.97 GB |

The C++ parser is **not** the bottleneck — it is 2.3s. The cost is athar's own
Python canonicalization (tagged-dict attributes + 1M Python objects). So:

- Rewriting ifcopenshell in Rust: **no.**
- FFI into ifcopenshell's C++ IfcParse: **no** — it would add a heavy
  boost/CMake build to reuse a 2.3s step we can replace with a plain Rust
  byte-tokenizer at similar cost, and it would not remove the 39s (that work is
  athar-specific, not ifcopenshell's).
- **Chosen: pure-Rust hybrid.** Rust tokenizes the STEP bytes; ifcopenshell is
  called once, file-independently, only for *schema descriptors*. No C++ FFI,
  no new system deps, keeps the clean-wheel story.

## Architecture

```
Python (once per parse, file-independent):
  ifcopenshell.ifcopenshell_wrapper.schema_by_name(schema)
    -> per-class descriptor map (only classes present in the file):
       { "IFCWALL": { class, product, spatial, guid_idx, name_idx,
                      attrs:[ {name, shape} ... ] }, ... }
    -> unit_factors (LENGTHUNIT/AREAUNIT/... -> float) from IfcUnitAssignment

Rust (athar/_native):
  step.rs      mmap + tokenize DATA section -> Vec<Record{id, keyword, attrs}>   [DONE]
  descriptor.rs deserialize the schema descriptor map (serde_json)               [next]
  canon.rs     apply descriptor to tokens -> canonical attr parts + collect refs [next]
  edges.rs     port edge_policy classification (direct refs + IfcRel* rules)     [next]
  merkle       reuse Stage A compute (geometry/data domains)                     [DONE - reuse]
  wl           reuse Stage A topology gossip                                     [DONE - reuse]
  spatial.rs   port placement-chain resolve + centroid/aabb quantization         [next]
  lib.rs       orchestrate -> return {step_id: SignatureVector-tuple}            [next]
```

### Descriptor shape (per attribute)

Recursive `Shape`, precomputed in Python from the EXPRESS type:

- `Leaf { measure }` — scalar position. `measure ∈ {Length, Area, Volume,
  Angle, Direction, Default}` folds the schema measure type *and* the
  `"DIRECTION" in attr_name` heuristic, picking the quantization scale
  (length/area/vol/angle → `*unit*1e6`, direction → `*1e5`, default → `*1e6`,
  banker's rounding). Typed tokens (`IFCLENGTHMEASURE(x)`) override the measure
  from their own keyword.
- `Agg { sorted, elem }` — list/array (`sorted=false`) or set/bag
  (`sorted=true`, items sorted by canonical key).
- `Select` — value self-describes via the STEP token's type tag (a `#ref` →
  branch label = target keyword, contributes as an edge; a `KEYWORD(inner)` →
  measure/scalar from the keyword).

Refs are excluded from attribute parts (they become Merkle edges); a ref inside
an aggregate contributes a constant placeholder so element count/structure is
still captured — matching the discrimination level of the Python pipeline
(child edges are sorted by hash, so order within a same-label aggregate is not
significant in either implementation).

## Status (2026-06-27) — COMPLETE

Stage B is **landed and green**. The native pipeline is the canonical
implementation; Python remains as an automatic fallback (`ATHAR_NO_NATIVE=1`).

- Stage A (Merkle + WL hashing in Rust): landed, perf-neutral by design — see
  `FINDINGS_native_stage_a_2026-06-26.md`.
- `step.rs` tokenizer, `descriptor.rs`, `canon.rs` (incl. `data_facts`),
  `edges.rs`, `spatial.rs`, `lib.rs` orchestration: **all built + unit-tested**
  (`cargo test --no-default-features`).
- Python `native_schema.py` descriptor builder + `signatures.py` native
  dispatch (threading `data_facts` back to `SignatureVector`): **done**.
- Full suite green through the native path: **154 passed, 31 skipped, 1
  xfailed**. The two structural failures that surfaced during the port are
  fixed: corrupted/oversized refs (lenient `i64` parse + drop of dangling refs
  in `edges.rs`) and the rename-is-a-data-change case (native `data_facts`).

### Measured (uni-project-house-50mb.ifc, IFC2X3, 6,385 signatures)

| path | bundle time | peak RSS | signatures |
|---|---:|---:|---:|
| Python (`ATHAR_NO_NATIVE=1`) | 61.9s | 2431 MB | 6385 |
| **native** | **14.0s** | **1987 MB** | 6385 |

≈4.4× faster, ~450 MB lower peak, identical signature count. The residual peak
is dominated by the single file-independent `ifcopenshell.open()` (schema +
unit factors, ~2.3s C++) which is released (`del ifc`) before the Rust parse;
the ~1M mesh-primitive entities never become Python objects.

## Acceptance for the native pipeline

1. `assert_report_invariants` + metamorphic stability (renumber/reorder) green
   on the corpus pairs through the native path.
2. Run-to-run determinism (`test_engine_output_is_byte_identical`, same build).
3. Measured: discipline-scale pair in a few seconds, peak RSS well under the
   current ~2–3.3 GB. Unhandled STEP/schema constructs raise a hard error (never
   silently miscompute); Python can fall back to the ifcopenshell parse path.
