# Findings: competitor benchmark + large-file memory (post-Rust)

Date: 2026-06-27

## Context

- Current engine (`athar/bottom/` native Rust signature pipeline +
  `athar/matcher/` + `athar/delta/`), native built `--release`.
- Harness: `scripts/explore/benchmark_competitors.py` (median of 3), competitors
  `ifcdiff` 0.8.5 (two modes) and `ifcfast` 0.4.
- Machine: 16 GB RAM, macOS (Apple silicon, 16 KB pages).
- Added two opt-in (`--large`) same-file parse-stress pairs:
  - `large_same_file_gni_p5_arc` — `model_5_arc.ifc` (76 MB, IFC4).
  - `large_same_file_spanish_180mb` — `real-world-spanish-180mb.ifc` (180 MB,
    IFC2X3).
- Large-file numbers below were taken with focused one-off runners (Athar-only,
  then ifcdiff both modes) guarded by a disk + available-RAM watchdog, because
  the full `--large` suite would peak too high to run safely alongside other
  apps. Watchdogs never fired; free disk held at 8.3 GB, available RAM ≥ 5 GB.

## Numbers

Small default pairs (≤ ~1 MB, gni_190 family): Athar 0.10–0.20 s vs
`ifcdiff` 0.7–1.2 s vs `ifcfast` ~0.017 s. Athar 4–6× faster than ifcdiff and
correct; `ifcfast` far faster but shallow GUID/product-table only.

Mid/large pairs (full diff):

| Pair | Athar | ifcdiff_default | ifcdiff_relationships |
|---|---|---|---|
| discipline (gni p0 arc/str) | 2.19 s / 578 MB | 1.17 s | 1.22 s |
| large_unrelated gni_77/68 (45 MB×2, IFC4) | 6.92 s / 1631 MB | 2.71 s / 504 MB | 2.56 s / 503 MB |

Same-file parse-bound stress (all three tools, identical inputs):

| File | Athar | ifcdiff_default | ifcdiff_relationships |
|---|---|---|---|
| 76 MB IFC4 (gni_p5_arc) | 17.7 s / 3.13 GB ✅ | 75.1 s / 3.05 GB ✅ | 8.7 s / 1.14 GB — **3045 false "modified"** ❌ |
| 180 MB IFC2X3 (spanish) | 51.4 s / 5.69 GB ✅ | 60.2 s / 3.13 GB ✅ | 81.1 s / 2.38 GB — **44 296 false "modified"** ❌ |

Athar same-file memory scaling: 76 MB → 3.13 GB (41× file size), 180 MB →
5.69 GB (32×). Sub-linear — ratio drops with size.

## Findings

- **Speed vs ifcdiff is content/schema dependent, not a clean loss.** ifcdiff
  loses on small files, beats Athar ~2–2.7× on the mid-size IFC4 gni pairs, and
  loses again on the 180 MB file. ifcdiff's own `default` mode took 75 s on the
  76 MB IFC4 file but only 60 s on the larger 180 MB IFC2X3 file — bigger file,
  less time — so its cost tracks content/relationship shape, not byte size.
- **On the largest file Athar is the fastest correct tool** (51.4 s vs 60.2 s vs
  81.1 s).
- **Memory is Athar's sole consistent weakness:** ~1.8–2.4× ifcdiff's peak RSS
  at every size.
- **Correctness:** Athar reports zero changes on identical input at all sizes.
  ifcdiff's faster `relationships` mode fabricates phantom modifications
  (3045 at 76 MB, 44 296 at 180 MB).
- **Same-file undercounts memory ~2×:** `engine.diff_files` aliases
  `new_bundle = old_bundle` for identical paths (`athar/engine.py:51-54`), so a
  same-file run builds one `SignatureBundle`. A real two-file 180 MB diff holds
  two → projected ~11 GB. Peak RSS is during the Rust build pipeline, not the
  final signatures.

## Follow-ups

- Reduce peak RAM (the one consistent loss). Profile per-stage RSS in the Rust
  pipeline, free intermediate buffers eagerly, shrink per-entity signatures.
  Tracked outside the repo (dfc task, project `github-com-amixx-athar`).
- These large pairs are same-file (parse-bound); they isolate parse, not full
  matching. A true two-file large pair is still needed for end-to-end large-diff
  numbers and the real memory ceiling.
