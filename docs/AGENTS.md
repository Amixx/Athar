# Documentation Agent Notes

Use `docs/` for durable project evidence and historical findings, not active
implementation instructions.

## Corpus Notes

- Corpus measurements, file inventory, pair shapes, tier distributions, and
  survey conclusions live under `docs/corpus/`.
- `docs/corpus/REMOTE_CORPUS_MANIFEST.md` describes the future design for
  large files that are too big to store in the repo. No download
  infrastructure exists yet.
- Keep corpus notes factual and dated. Do not duplicate long corpus detail in
  root agent instructions.

## Performance Notes

- Save concrete performance investigations under `docs/perf/`.
- A perf note should include command/context, key numbers, hotspots, and chosen
  follow-up actions. Avoid speculation that was not measured.
- `docs/perf/STATUS.md` states what is still current; dated notes are
  historical records.
