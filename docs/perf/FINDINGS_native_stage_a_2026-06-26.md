# Findings: native Stage A (Rust Merkle + WL gossip)

Context: Tier-2 Rust port of the bottom signature pipeline. Stage A ports the
two pure-data stages — `compute_merkle_hashes` (`athar/bottom/merkle.py`) and
`compute_topology_hashes` (`athar/bottom/wl_gossip.py`) — to a PyO3 crate
(`athar/_native`), with a pure-Python fallback and **byte-identical** output
(`tests/test_native_parity.py`).

## What shipped

- `athar/_native` — PyO3/maturin crate, `abi3-py310` (one wheel per platform
  for CPython 3.10+), `sha2`+`hex`, release LTO. Imported as `athar_native`.
- `athar.bottom.merkle` / `athar.bottom.wl_gossip` dispatch to the native
  module when present and fall back to pure Python when it is absent or
  `ATHAR_NO_NATIVE=1`. The "pure Python, only ifcopenshell" global contract
  still holds for pip users — native is an opt-in accelerator.
- `make native-build` / `make native-clean`; `pip install -e .[native]` for the
  build tool.
- Parity is exact: native and pure-Python produce identical Merkle hashes,
  topology hashes, and full signature vectors on `bl_v1`. Full fast suite green
  both with native on (124 passed) and off (`ATHAR_NO_NATIVE=1`, 121 passed +
  parity skips).

## Measured perf (uni-project-house-50mb.ifc, 1,051,850 entities)

Single parse, then each stage timed native vs pure-Python:

| stage     | pure python | native | speedup |
|-----------|-------------|--------|---------|
| merkle    | 11.27s      | 11.67s | 0.97x   |
| topology  |  4.42s      |  4.22s | 1.05x   |
| Stage A   | 15.69s      | 15.89s | 0.99x   |

**Stage A is perf-neutral at this boundary.** Splitting the native merkle call:

- Python-side precompute of the JSON-encoded attribute *parts*
  (`_attribute_parts` → `json.dumps`): **6.21s** (51%)
- FFI marshalling of the per-entity maps + Rust sha256 recursion: **6.01s** (49%)

## Interpretation

The Stage-A hypothesis ("~20% time, sha256-bound, trivially parallel") does not
hold at the natural FFI boundary. `hashlib.sha256` is already a C extension, so
moving only the hashing to Rust saves little. The real cost is (a) the Python
attribute canonicalization (`json.dumps` per attribute), which **byte-identical
output forces to stay in Python** — it is the one place that reproduces the
exact `json.dumps(..., sort_keys=True, separators=(",",":"), ensure_ascii=True)`
bytes — and (b) marshalling ~1M-entry maps across the FFI boundary. Both paths
pay (a); the native path adds (b). Net wash.

Note: the parser already quantizes reals to integers
(`{"kind":"real_q","value":<int>}`), so attribute values are only
ints/strings/bools/null/lists/dicts — there are **no raw floats**, which means a
Rust-side encoder *could* reproduce the bytes without float-formatting risk.

## What this redirects to

1. **Toolchain is proven** (the explicit Stage-A goal): PyO3 build, abi3 wheel,
   maturin, graceful fallback, exact parity, green suite. The wheel/CI story is
   de-risked for whatever comes next.
2. A bigger Stage-A win requires moving the attribute *encoding* into Rust, not
   just the hashing — which is only worthwhile if entity data stops
   round-tripping through Python objects, i.e. **Stage B** (native STEP parse;
   never materialize `ParsedEntity`). That is where both the parse-time win
   (40s) and the memory win (3.3 GB peak) actually live.
3. **Byte-identical-with-Python is the gating constraint.** It pins encoding to
   Python today and is the single largest source of work/risk for a native
   parser. Relaxing it (a native canon version, native as the one
   implementation, one-time re-baseline) is the lever that makes Stage B
   tractable — a strategic decision, deferred to the maintainer.
