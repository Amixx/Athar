# QA Presentation Agent Notes

`athar_qa/` renders the QA wedge ("CI for BIM") for humans: it turns the
structured verdict from `athar.check.evaluate_report` and the event log from
`athar_store` into readable pass/fail reports and review timelines.

It is a presentation package. It may import engine/integration *output* (plain
dicts), never the reverse: core engine modules under `athar/` must not import
`athar_qa`. It carries no policy logic — gate decisions and exit codes stay in
`athar.check`; this layer only formats them.

## Surface

- `render_check_report(result, *, title=...)` — formats one `evaluate_report`
  verdict. Reads `ok`, `summary`, and each `violations[]` entry. Violation
  detail is rendered per `code`: `property_value_changed` shows
  `Name old -> new`, `property_removed` lists dropped property names,
  `schema_changed` shows `old -> new`, `placement_delta_limit` shows the delta
  vs limit. `affected` is capped upstream (20); the renderer prints the
  `count`-vs-shown remainder as `... and N more`.
- `render_history(events, *, title=...)` — formats a `BaselineStore.history`
  list as a chronological timeline with a pass/fail tally. Event lines are keyed
  off `event` (`baseline_set` / `review` / `approval` / `rejection`).

## Wiring

- `athar check --format text` calls `render_check_report`.
- `athar store history --format text` calls `render_history`.

Both CLIs default to `json` so existing scripted consumers are unaffected; text
is opt-in. Keep new violation `code`s renderable here when they are added to
`athar.check`, but degrade gracefully (fall back to the violation `message`)
for unknown codes.
