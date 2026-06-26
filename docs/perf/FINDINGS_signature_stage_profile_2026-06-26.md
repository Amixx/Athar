# Findings: bottom-layer signature stage profile (2026-06-26)

## Context

Added a minimal exploratory profiler for the current engine's bundle build path:
`scripts/explore/profile_signature_bundle.py`. It mirrors
`athar.bottom.signatures.build_signature_bundle()` and times each major stage
separately so Rust/native work can be ported and checked one function at a
time.

This profiler is worth keeping after the first Rust port lands: it is small,
has no engine coupling, and gives a direct before/after view of which remaining
stage is worth porting next. Delete it only if the bottom-layer pipeline is
fully replaced and the script no longer describes a live code path.

## Commands

```bash
.venv/bin/python \
  scripts/explore/profile_signature_bundle.py \
  tests/fixtures/tiny_no_products.ifc \
  real-world-test/Building-Landscaping-v1.ifc \
  real-world-test/Duplex-Architecture.ifc \
  --out docs/perf/signature_stage_profile_2026-06-26.json

.venv/bin/python \
  scripts/explore/profile_signature_bundle.py \
  real-world-test/AdvancedProject.ifc \
  --out docs/perf/signature_stage_profile_advanced_project_2026-06-26.json
```

Single run per file, no warmup, using a repo-local `.venv`.

## Results

| File | MB | Entities | Signatures | Total | Parse | Merkle | WL/topology | Edge set | Spatial | Assemble |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `tiny_no_products.ifc` | 0.001 | 8 | 0 | 0.020s | 0.019s | 0.000s | 0.000s | 0.001s | 0.000s | 0.000s |
| `Building-Landscaping-v1.ifc` | 1.204 | 150 | 8 | 0.518s | 0.448s | 0.068s | 0.000s | 0.000s | 0.000s | 0.000s |
| `Duplex-Architecture.ifc` | 2.381 | 38,898 | 295 | 2.731s | 2.101s | 0.400s | 0.104s | 0.097s | 0.022s | 0.007s |
| `AdvancedProject.ifc` | 44.337 | 770,172 | 2,365 | 56.898s | 40.885s | 8.990s | 2.721s | 1.976s | 1.942s | 0.383s |

AdvancedProject stage share:

- `parse_ifc`: ~72%
- `compute_merkle_hashes`: ~16%
- `compute_topology_hashes`: ~5%
- `build_edge_set`: ~3.5%
- `build_spatial_features`: ~3.4%
- `assemble_signatures`: <1%

## Takeaways for Rust sequencing

1. **Parser replacement is the biggest cold-run ceiling.** On the 44MB model,
   `parse_ifc()` alone is ~41s of ~57s. This supports the native parser
   strategy, but it is also the highest correctness risk.
2. **Merkle hashing is the best bounded native-port target after/alongside
   parser work.** It is pure computation over already-canonicalized entities
   and costs ~9s on the 44MB model.
3. **WL/topology is third.** It is measurable but smaller in the current engine
   than it was in the deleted old graph engine.
4. **Edge set and spatial are secondary.** Both are around 2s on this medium
   model; worth revisiting only after parse/Merkle/topology improve.

## Caveats

- These are wall-clock timings from one machine and one run per file.
- The profiler mirrors the bundle builder but does not exercise `diff_files()`
  cache behavior or report/matcher work.
- Large 50MB+/180MB acceptance models should be profiled before committing to a
  final native-port order.
