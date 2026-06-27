# Findings: native pipeline memory profile (180 MB IFC2X3)

Date: 2026-06-27

## Context

Follow-up to `FINDINGS_competitor_benchmark_large_files_2026-06-27.md`, which
flagged peak RSS as Athar's one consistent weakness and *projected* a real
two-file 180 MB diff at **~11 GB** by doubling the same-file figure. That
projection was untested: `engine.diff_files` aliases `new_bundle = old_bundle`
for identical paths (`athar/engine.py:51-54`), so the same-file benchmark builds
one bundle and could not show per-phase cost or the true two-file ceiling.

This note measures both, with a new reproducible tool.

## Tool

`scripts/explore/profile_memory.py` — dependency-free (RSS sampled by shelling to
`ps` from a background thread; cross-checked against `getrusage` true peak). It
mirrors the phase boundaries of `_build_signature_bundle_native`, reusing the
real `athar.bottom` helpers, so per-phase RSS is visible without changing engine
code. Modes: `single` (one build, phase breakdown), `double` (two bundles of the
same file held simultaneously), `diff` (full end-to-end on two paths).

    python scripts/explore/profile_memory.py single real-world-test/real-world-spanish-180mb.ifc --out docs/perf/mem_single_spanish_180mb.json
    python scripts/explore/profile_memory.py double real-world-test/real-world-spanish-180mb.ifc --out docs/perf/mem_double_spanish_180mb.json

Machine: 16 GB RAM, macOS arm64, Python 3.12, commit `b3d95d5`, `--release`
native. File: `real-world-spanish-180mb.ifc` (181.8 MB, IFC2X3, 44,389
signatures).

## Headline: the ~11 GB projection is wrong — the ceiling is ~`T`, not `2T`

`double` mode builds a second bundle of the same file while holding the first:

| measurement | value |
|---|---|
| peak RSS, one build (`single`) | 3,890–4,663 MB (run-to-run, see variance note) |
| peak RSS, **two bundles held** (`double`) | 4,320 MB |
| marginal peak added by the 2nd build | **~+430 MB** |
| resident holding one bundle | 4,054 MB |
| resident holding two bundles | 3,665 MB (*lower* — allocator returned pages) |

The first build's transient arena is released/reused before the second build
starts, so a second same-size bundle adds only a small resident delta, not
another full transient. **A real two-file 180 MB diff's build phase peaks ~4–4.5
GB, not ~11 GB.** The same-file aliasing was not hiding a 2× memory cliff.

Caveat: this measures the *build* phase. Two genuinely different files would add
matcher + report memory, which scales with the number of changes (report rows),
not file size; signatures themselves are small. A true two-file large pair is
still worth measuring for the match-phase tail (`diff` mode supports it).

## Where the RAM actually goes (single, per phase)

Representative run (peak 4,663 MB true):

| phase | sec | resident after | Δrss | reading |
|---|---:|---:|---:|---|
| `ifc_open` | 12.8 | 1,624 MB | +1,533 | ifcopenshell loads the whole C++ model just to open |
| `extract_properties` | 11.7 | 1,761 MB | +137 | pset/relationship traversal |
| `ifc_release` (`del ifc` + gc) | 0.7 | 1,570 MB | −189 | **only ~190 MB of the 1.5 GB C++ model returns to OS** |
| `native_build` (Rust) | 69.6 | 4,570 MB | **+2,999** | dominant peak driver: tokenize → canon → merkle → WL → spatial |
| `materialize_signatures` | 1.3 | 4,586 MB | +12 | Python `SignatureVector` objects are cheap |

`resident_after_build` (after `del sigs` + gc) ≈ peak: freed memory is **not
returned to the OS**, so RSS is monotonic across phases within a run.

## Findings

1. **`native_build` is the lever** — ~3.0 GB of the ~4.6 GB peak. ~72% of the
   time and the bulk of the memory live in the Rust pipeline, not Python. This
   is where to profile next (per-stage RSS *inside* Rust: tokenize vs the parsed
   record `Vec` vs merkle/WL maps), free intermediate buffers eagerly, and shrink
   per-record footprint.
2. **ifcopenshell's open cost (~1.5 GB) lingers.** `del ifc` returns only
   ~190 MB. The C++ model is opened only for schema name, unit factors, and
   property traversal, then dropped — but its pages stay resident and stack on
   top of the Rust arena. Two angles: (a) free it harder / earlier (it overlaps
   the Rust peak today), or (b) longer term, source properties + units from the
   Rust tokenizer and drop ifcopenshell from the build path entirely.
3. **Peak RSS is noisy (~20% run-to-run): 3,890 vs 4,663 MB on identical input.**
   Driven by allocator page-return timing and system memory pressure, not by the
   work. Report peak as approximate; prefer the *structural* deltas (which phase,
   how much it grows) over a single absolute peak. Take the median of ≥3 runs for
   any before/after optimization comparison.
4. **Resident never reclaims within a run** — pages freed by `del`/gc are not
   returned to the OS. This means "resident after build" overstates *live* memory;
   the true live set (one bundle = 44k signatures) is far smaller. An allocator
   that returns pages (e.g. configuring malloc trim / a different global
   allocator) could cut steady-state RSS independently of any pipeline change.

## Rust-internal stage breakdown (`native_build`, instrumented)

`native_build` is one opaque call to Python, so it was instrumented from the
inside with a **tracking global allocator** (`athar/_native/src/lib.rs`): two
atomics count live Rust heap bytes + a high-water mark, printed per internal
stage when `ATHAR_NATIVE_PROFILE=1`. This measures the pipeline's *own* heap
precisely and deterministically — no `ps`, no RSS noise, and Python's heap (its
own allocator) is excluded.

Run:

    ATHAR_NATIVE_PROFILE=1 python scripts/explore/profile_memory.py single real-world-test/real-world-spanish-180mb.ifc

180 MB file (full table in `mem_stage_breakdown_spanish_180mb.txt`):

| stage | sec | live MB after | Δlive MB | what it allocates |
|---|---:|---:|---:|---|
| read_bytes | 0.0 | 174 | +173 | file bytes |
| tokenize | 1.7 | 1,520 | **+1,346** | `records` (owned STEP tokens) |
| canonicalize | 9.2 | 3,494 | **+1,974** | `parsed.entities` (~1M) — biggest |
| build_edges | 2.9 | 3,854 | +360 | `edge_list` |
| merkle_inputs | 6.3 | 4,754 | **+900** | clones of `parsed` data + adjacency maps |
| merkle | 30.6 | 5,275 | +521 | geom/data hash caches |
| wl_topology | 12.5 | 5,829 | +554 | seeds + context/spatial maps |
| spatial | 1.3 | 5,910 | +81 | placement/centroid/aabb |
| assemble_signatures | 5.6 | 6,431 | +521 | output `SigTuple` vec |

Two structural facts jump out:

1. **Nothing is freed mid-pipeline — live heap only grows.** Peak = the *sum* of
   every intermediate held simultaneously. `bytes` (174 MB, needed only through
   tokenize) and `records` (1,346 MB, needed only through canonicalize) are still
   alive at `assemble_signatures`, through the 43 s merkle+WL tail.
2. **Reserved heap (6.4 GB) is ~2.6× the resident growth attributed to
   `native_build` (~2.4 GB).** The gap is `Vec`/`HashMap` capacity slack +
   lazy paging — a lot of requested-but-untouched memory.

## Next optimizations (ranked, now evidence-backed)

1. **Drop intermediates as soon as they're dead.** `drop(bytes)` after tokenize
   (−174 MB) and `drop(records)` after canonicalize (−1,346 MB) remove ~1.5 GB
   from the working set across the entire merkle+WL+assemble tail (the slowest
   43 s, where peak is reached). Pure win, no algorithm change.
2. **Stop cloning in `merkle_inputs` (+900 MB).** `geom_parts`/`data_parts` are
   `.clone()`d out of `parsed.entities`, duplicating strings that already exist.
   Borrow them, or consume `parsed` (move) once canonicalization is done.
3. **Tighten container capacity (the 2.6× reserve-vs-touch slack).** Pre-size or
   `shrink_to_fit` the hot maps (merkle caches, adjacency) so reserved heap
   tracks live data.
4. **ifcopenshell open (~1.5 GB, OS view) lingers** — `del ifc` returns only
   ~190 MB and it coexists with the Rust peak. Longer term, source props/units
   from the Rust tokenizer and drop ifcopenshell from the build path.

Items 1–2 are small, local edits in `build_bundle` and together target ~2.4 GB.

## Results: optimizations #1 + #2 applied

Implemented in `build_bundle`: `drop(bytes)` after tokenize, `drop(records)`
after canonicalize, and `std::mem::take` (move, not clone) for
`geom_parts`/`data_parts` into the merkle maps. Signatures unchanged (44,389) —
correctness preserved.

Rust live-heap peak (deterministic, tracking allocator):

| stage | live MB before | live MB after |
|---|---:|---:|
| tokenize | 1,520 | **1,347** (−174, `bytes` freed) |
| canonicalize | 3,494 | **1,975** (−1,346, `records` freed) |
| merkle_inputs (Δlive) | +900 | **+595** (move saved ~305 dup strings) |
| **final peak** | **6,439** | **4,615** |

**−1,824 MB (−28%) off the Rust working set.**

Honest caveat on the OS view: peak **RSS** moved only ~250 MB (4,114 → 3,858),
which is *within* the ~20% run-to-run RSS noise. Most of the 1.8 GB freed was
`Vec`/`String` reserved capacity that was never fully resident, and freed pages
aren't promptly returned to the OS. The deterministic win is the working-set
reduction — it's what compounds in a real two-file diff (two pipelines) and
under memory pressure. (Wall time is dominated by environmental variance here:
the `merkle` stage alone swung 30→41 s between runs; `drop`/`mem::take` add no
time.)

Then the merkle input maps + `edge_list` were also dropped right after they go
dead (after `merkle_compute` / the adjacency loop). This freed memory *during*
the 56 s merkle stage, so the allocator reused it for WL+assemble instead of
growing RSS — a **real** ~600 MB peak-RSS drop (4,114 → 3,497) and live-heap
final 3,291 ≈ RSS (the reserve-vs-touch slack collapsed).

## Better proxies than peak RSS

Peak RSS is a noisy, one-dimensional snapshot: three runs of *identical* code on
this file gave 4,114 / 3,497 / 2,396 MB — ~1.7 GB of pure run-to-run variance
(allocator page-return timing + OS pressure). Optimizing against it is chasing
noise. Two better metrics were added:

- **Allocation churn** (Rust tracking allocator: total bytes + alloc-event count,
  vs peak live). **Deterministic** — a function of the input, not the OS — so it
  only moves when *we* change the work. The best "are we doing the right thing"
  signal.
- **GB·seconds** (`profile_memory.py`: trapezoidal integral of RSS over time).
  Fuses memory × time into the unit cloud platforms bill. The "cost" KPI.

Measured on the 180 MB file (post-drops):

| stage | sec | churn MB | allocs |
|---|---:|---:|---:|
| tokenize | 1.9 | 1,346 | 15.1 M |
| canonicalize | 9.1 | 2,312 | **79.6 M** |
| merkle | 56.6 | 2,919 | **48.0 M** |
| wl_topology | 12.7 | 1,677 | 31.5 M |
| assemble | 4.9 | 1,576 | 29.8 M |
| **TOTAL** | | **11,542** | **221.3 M** |

`churn_factor = 11,542 / 3,665 = 3.1×`. `memory-time = 168 GB·s`.

**Headline: 221 million allocations to diff one file (~200 per entity).**
`canonicalize` (80 M) and `merkle` (48 M, 56 s) dominate — the stages that build
`Vec<String>` payloads via `format!`, NFC-normalize, then `sha256_hex(join(...))`
(a fresh 64-char hex `String` per entity per domain). The 56 s merkle *time* and
the memory churn share one root cause: transient string building.

### Refactor #1 (done): stream payloads into the hasher (merkle / WL / seed)

`hash_entity`, `topology_compute`, and the WL seed were rewritten to feed each
payload part straight into `Sha256` with a `0x1F` separator byte instead of
building a `Vec<String>` and `join`-ing it. **Byte-identical** output (verified:
signature-hash checksums on BasicHouse + AdvancedProject match the pre-rewrite
checkpoint exactly), so zero correctness risk.

| metric | before | after | Δ |
|---|---:|---:|---:|
| merkle allocs | 48.0 M | 12.2 M | −75% |
| merkle time | 56.6 s | 34.7 s | −39% |
| wl_topology allocs | 31.5 M | 14.8 M | −53% |
| total allocs | 221.3 M | 168.8 M | −24% |
| native_build time (Σ) | ~92 s | ~67 s | −27% |
| churn factor | 3.1× | 2.6× | leaner |

Both axes moved together by deleting transient strings — fewer `malloc` calls
*is* the time win.

Caveat learned: **GB·s rose (168 → 192) despite shorter wall time**, because it
integrates *RSS* and RSS was noisy-high that run (3,587 vs 2,396 MB). GB·s
inherits RSS noise; to be a real KPI it should integrate live heap (deterministic),
not RSS. The trustworthy proxies remain **allocation count** and stage time.

### Refactor #2 (done): canonicalize encode-into-buffer + deterministic heap·s

Two changes:

- **`canon.rs` encode path** rewritten to append into one `&mut String` buffer
  (`encode_*_into`) instead of returning a `String` per node and `join`-ing
  aggregates; STEP-logical detection uses `eq_ignore_ascii_case` instead of
  `to_uppercase()` (allocation-free). Byte-identical (verified vs checkpoint;
  21/21 Rust unit tests pass).
- **Deterministic heap·s** added to the native report: live heap (exact) ×
  stage time, trapezoidal. Unlike RSS-based GB·s it doesn't inherit allocator
  noise. (`heap=150.4 GB·s` on the 180 MB file.)

| metric | before | after | Δ |
|---|---:|---:|---:|
| canonicalize allocs | 79.6 M | 67.6 M | −15% |
| canonicalize time | 9.1 s | 7.9 s | −13% |

Modest vs merkle's −75% because canon's allocations are spread across several
sources; the encode path was only one. **Remaining canon levers** (untouched):
`collect_refs` does `attr_name.to_string()` per reference (millions);
`human_value` rebuilds a report string per data attribute (all `format!`);
`nfc_strip` allocates an NFC `String` per string value (largely inherent).

### Cumulative (both refactors, byte-identical throughout)

| metric | baseline | now | Δ |
|---|---:|---:|---:|
| total allocs | 221.3 M | 156.7 M | −29% |
| merkle time | 56.6 s | 36.0 s | −36% |
| native_build (Σ stages) | ~92 s | ~67 s | −27% |
| churn factor | 3.1× | 2.5× | leaner |

Correctness proven by signature-hash checksum (BasicHouse + AdvancedProject)
matching the pre-refactor checkpoint, plus the Rust unit suite.

### Refactor #3 (batch): attr-name interning + WL tuples + dedupe-consume

Three churn sites in one pass (all byte-identical, now including `data_facts`
in the checksum; 21/21 Rust tests pass):

| change | stage allocs | Δ |
|---|---|---:|
| dedupe consume (`dedupe_fact_paths` moves instead of cloning into `out`) | assemble 29.8M → 21.0M | −8.8M |
| attr-name interning (`RefOut.attr_name: Rc<str>`) | canon 67.6M → 63.9M | −3.7M |
| WL neighbour tuples (`(prefix, seed)` not `format!`) | wl 14.8M → 14.5M | −0.3M |
| **total** | 156.7M → **143.9M** | **−12.8M (−8%)** |

**Two estimates were wrong, and the misses are the useful finding:**

- attr interning expected tens of millions of refs; there are only a few million
  — IFC's geometry *mass* is coordinate **lists** (`IfcCartesianPoint` reals),
  not references, so the ceiling was lower.
- WL tuples barely moved because WL churn is **not** the token strings (most mesh
  entities have no neighbours). It is `neighbours_within_k` allocating a fresh
  `HashSet` + `VecDeque` **per node across ~1M nodes**. That per-node scratch
  allocation — reuse one cleared buffer across the loop — is the real WL lever
  (~10M+ allocs and a chunk of WL's ~12 s), still untouched.

### Refactor #4: WL BFS scratch reuse (the corrected lever)

Acting on #3's finding: `neighbors_within_k` allocated a fresh `HashSet` +
`VecDeque` per call, twice per node, across ~1M nodes. Replaced with
`neighbors_within_k_into` writing into one cleared scratch pair (plus a reused
`tokens` buffer) hoisted out of the loop. Byte-identical (tokens are sorted, so
set iteration order is irrelevant); 21/21 tests pass.

| metric | before | after | Δ |
|---|---:|---:|---:|
| wl_topology allocs | 14.5 M | 4.84 M | **−67%** |
| total allocs | 143.9 M | 134.3 M | −7% |

One contained edit ≈ the whole previous 3-change batch. WL *time* was too noisy
to read in one run (11.7↔14.2 s, while merkle swung 35↔39 s); the deterministic
alloc count is the trustworthy signal that the work dropped.

### Cumulative (all refactors, byte-identical throughout)

Total allocs **221.3M → 134.3M (−39%)**; merkle 56.6s → ~35s; churn factor
3.1× → 2.3×. Metric lesson reinforced: model *where* allocations are before
optimizing (`ATHAR_NATIVE_PROFILE=1` per-stage allocs), and trust the
deterministic alloc count over noisy time/RSS for whether a change helped.

### Remaining largest sources (for next time)

`canonicalize` 63.9 M (inherent part `String`s + `nfc_strip` + `human_value`
report strings), `assemble` 21.0 M, `tokenize` 15.1 M (keyword interning is the
obvious lever there). Diminishing returns from here — much of canon/tokenize is
the actual signature/token data, not transient waste.

Artifacts: `mem_single_spanish_180mb.json` (before),
`mem_single_spanish_180mb_optimized.json` (after).

The `~11 GB two-file ceiling` follow-up from the competitor note is **closed**:
measured ~4–4.5 GB for the build phase; a real two-file diff adds only
change-proportional matcher/report memory on top.

## Artifacts

- `docs/perf/mem_single_spanish_180mb.json`
- `docs/perf/mem_double_spanish_180mb.json`
- `docs/perf/mem_stage_breakdown_spanish_180mb.txt`
