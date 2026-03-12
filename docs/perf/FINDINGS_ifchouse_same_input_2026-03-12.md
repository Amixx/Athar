# IfcHouse Same-Input Perf Findings (2026-03-12)

Source run:
`python -m scripts.explore.benchmark_diff_engine --case ifchouse:data/BasicHouse.ifc:data/BasicHouse.ifc --warmup 0 --iterations 1 --engine-timings --heartbeat-s 15 --out /tmp/ifchouse-baseline.json`

Concrete findings:

- Input size: `1,026,311` entities, `1,729,603` refs, `3,443` entities with GlobalId.
- Parse time: `67.8s` total (`old 29.9s`, `new 37.9s`).
- `diff_graphs`: `1316.9s` (~21m57s), peak memory `~2.68 GB`.
- `stream_ndjson`: `1429.5s`, `stream_chunked_json`: `1632.2s`.
- Output was empty (`base_change_count=0`, `derived_marker_count=0`), so runtime is compute-bound.

Stage hotspots (`diff_graphs` timings):

- `prepare_context`: `1078.8s` (`82.2%`)
- `context.assign_old_ids`: `480.3s` (`36.6%`)
- `context.assign_new_ids`: `492.0s` (`37.5%`)
- `emit_base_changes`: `231.5s` (`17.6%`)
- `context.secondary_match`: `54.1s` (`4.1%`)

Actions that followed immediately:

- Reuse parse result when both input paths resolve to the same file (`diff_files`, `stream_diff_files`).
- Add direct same-graph short-circuit in `diff_graphs` / `stream_diff_graphs` (same object => immediate empty diff/stream with valid headers/stats, while still enforcing schema/profile/GUID/matcher policy validation).
- Avoid per-bucket sorting in base-change and matched-by-method paths by sorting once during identity indexing.
- Fast-skip deep equality checks for paired `H:` + `exact_hash` entities.
- Rewrite WL round payload construction to avoid per-node JSON/dict allocations in the hot loop.

Future perf work queue (ordered):

- Add partition-stability convergence stop in WL (not only digest-change heuristic).
