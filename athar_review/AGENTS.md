# Review UI Agent Notes

`athar_review/` is the AECO-facing product surface over `athar_store`: a
locally-served web app (`athar review`) where a reviewer browses model
lineages, reads semantic change reports, sees conflicts, uploads candidates,
and approves or rejects — with Git, shas, and delta storage invisible.

Integration package: depends on `athar_store` (and `athar_view.server.find_dist`
for locating the built SPA). Nothing in `athar/` may depend on it.

## Server (`server.py`)

Stdlib `ThreadingHTTPServer` bound to 127.0.0.1, same pattern as `athar_view`.

Request guard, applied before routing:

- `Host` must be `127.0.0.1:<port>` or `localhost:<port>` (403
  `host_not_allowed`), which defeats DNS rebinding.
- A POST whose `Origin` is present and is not one of those origins is 403
  `origin_not_allowed`.
- JSON POSTs require `Content-Type: application/json`; uploads require
  `application/x-step` or `application/octet-stream`. None of these is
  CORS-safelisted, so a cross-site page cannot send them without a preflight,
  and the server never sends CORS headers. `multipart/form-data` is
  safelisted and is refused like every other type: 415.
- Bodies need an ASCII-digit `Content-Length` (411 when absent, 400 when malformed).
- Refusals before body consumption drain the declared bytes up to the upload limit plus 1 MB before sending the JSON error. Larger or unframed bodies close without draining; draining has a five-second socket timeout.

Errors are `{"error": {"code", "message", ...}}`. Store exceptions map as:
`ReviewStateError` 409 `review_approved|review_rejected|review_stale` (plus
`state`, `event_id`, `review_id`), `StaleBaselineError` 409
`baseline_changed`, other `StoreConflictError` (baseline exists without
`replace`) 409 `baseline_exists`, `UnknownBaselineError` 409 `no_baseline`,
`UnknownReviewError` 404 `review_not_found`, `EngineError` 422
`engine_refused`. The server itself answers 404 `project_not_found` for an
unknown project (except an accepted baseline upload, which creates it), 400
`actor_required` for a missing or blank actor before reading the body, 400
`unknown_policy`, and 413 `upload_too_large` above `max_upload_bytes`
(`--max-upload-mb`, also reported by `/api/overview`). Any other `StoreError`
is 500 `store_error` with the store's message; any other exception is logged
with its traceback and answered 500 `internal_error` with a generic message.

Endpoints:

- `GET /api/overview` — projects, model lineages, baseline pointers, pending
  counts, and `max_upload_bytes`.
- `GET /api/projects/{p}/events[?model_key=]` — the event log.
- `GET /api/projects/{p}/reviews/{id}` — the persisted delta report
  (404 for reviews predating report persistence, where the UI degrades
  gracefully, and for ids that name no review event).
- `GET /api/projects/{p}/artifacts/{sha}.ifc` — exact artifact bytes,
  materialized from the delta pack per request.
- `POST /api/projects/{p}/models/{m}/review|baseline?actor=&filename=[&policy=]`
  — raw IFC bytes as the body; streams to a fixed temporary file name, runs
  the store action, returns the event. Synchronous: the response *is* the
  finished review. `filename` is the delivery name the event records, never
  a path; a missing or blank one is a 400.
- `POST /api/projects/{p}/models/{m}/approve|reject` — JSON
  `{review_id, actor, note}`.
- `GET /viewer/{p}/{review_id}/manifest.json` — a v1 viewer manifest whose
  old/new point at the artifact endpoints and whose report is the persisted
  review report, so the 3D viewer (`/index.html?src=…`) opens any recorded
  review with zero recomputation.
- Static: serves the built SPA; `/` and unknown extensionless paths fall back
  to `review.html` (hash routing), path-traversal guarded.

Identifiers must fully match the store's `[A-Za-z0-9._-]+` rule, excluding `.` and `..`; invalid identifiers return 400. Artifact names must match a sha256 shape before touching the filesystem.

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
