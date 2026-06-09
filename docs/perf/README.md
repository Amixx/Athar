# Performance Notes

Dated findings notes and benchmark artifacts. `STATUS.md` states what is still
current; everything else is a historical record tied to the engine that existed
on its date.

Notes dated before 2026-06-09 measured the old graph engine (`athar/diff/`,
`athar/graph/`, `athar/_native/`), which was deleted on 2026-06-09 along with
its benchmark harnesses (`scripts/explore/benchmark_*`, `run_perf_suite`,
`render_perf_summary`, `watch_progress`). Those notes and JSON artifacts are
kept as the record of that work; the commands they reference no longer exist.

To crib from a deleted harness (progress/sidecar patterns, artifact schema,
scenario methodology), recover it from git, e.g.:

```bash
git show b0fb66a:scripts/explore/benchmark_diff_engine.py
```

Conventions for new notes:

- One file per investigation: `FINDINGS_<topic>_<YYYY-MM-DD>.md`.
- Facts only: command/context, key numbers, hotspots, chosen follow-ups.
- Update `STATUS.md` when a note changes what is considered current.
