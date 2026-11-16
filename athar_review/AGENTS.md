# Review UI Agent Notes

`athar_review/` is the AECO-facing product surface over `athar_store`: a
locally-served web app (`athar review`) where a reviewer browses model
lineages, reads semantic change reports, sees conflicts, uploads candidates,
and approves or rejects — with Git, shas, and delta storage invisible.

Integration package: depends on `athar_store` (and `athar_view.server.find_dist`
for locating the built SPA). Nothing in `athar/` may depend on it.

## Server (`server.py`)

Stdlib `ThreadingHTTPServer` on 127.0.0.1, same pattern as `athar_view`:

- `GET /api/overview` — projects, model lineages, baseline pointers, pending
  counts.
- `GET /api/projects/{p}/events[?model_key=]` — the event log.
- `GET /api/projects/{p}/reviews/{id}` — the persisted delta report
  (404 for reviews predating report persistence; the UI degrades gracefully).
- `GET /api/projects/{p}/artifacts/{sha}.ifc` — exact artifact bytes,
  materialized from the delta pack per request.
- `POST /api/projects/{p}/models/{m}/review|baseline?actor=&filename=[&policy=]`
  — raw IFC bytes as the body; streams to a temp file, runs the store action,
  returns the event. Synchronous: the response *is* the finished review.
- `POST /api/projects/{p}/models/{m}/approve|reject` — JSON
  `{review_id, actor, note}`.
- `GET /viewer/{p}/{review_id}/manifest.json` — a v1 viewer manifest whose
  old/new point at the artifact endpoints and whose report is the persisted
  review report, so the 3D viewer (`/index.html?src=…`) opens any recorded
  review with zero recomputation.
- Static: serves the built SPA; `/` and unknown extensionless paths fall back
  to `review.html` (hash routing), path-traversal guarded.

Identifiers are validated against the store's `[A-Za-z0-9._-]` rule and
artifact names against a sha256 shape before touching the filesystem.

## Frontend (`viewer/src/review/`)

Second Vite entry (`viewer/review.html`) in the existing Svelte 5 SPA package —
JS stays confined to `viewer/`. Design: drawing-register theme (paper, graphite
rules, checking-stamp blue; ACCEPTED/REJECTED rendered as tilted control
stamps; revisions lettered A, B, C on accepted events), deliberately the paper
counterpart to the viewer's dark model space; the four diff hues match the
viewer. `lib/format.ts` and `lib/api.ts` are pure and covered by
`viewer/tests/review-format.test.ts` (bun). Pending-ness in the UI mirrors
`BaselineStore.pending_reviews`: unresolved reviews whose baseline is still
current.

## Testing

`tests/test_review_ui.py` runs the real server on an ephemeral port against a
seeded store: overview/events/report endpoints, byte-exact artifact download,
upload→review→approve flipping the baseline, conflict surfacing, the viewer
manifest bridge, traversal/id guards. Frontend logic tests run with
`bun test tests` in `viewer/`.
