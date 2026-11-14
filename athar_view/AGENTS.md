# Diff Viewer Agent Notes (`viewer/` + `athar_view/`)

Visual presentation layer. It may depend on the core engine and the `athar_git`
bundle cache; nothing in `athar/` may depend on it.

Onshape-style visual overlay diff: both models tessellated in the browser and
rendered in one 3D scene, colored by the engine's report (green added, red
deleted, blue modified, gray unchanged, magenta `extra` for meshes the report
has no record of), with an old↔new crossfade
slider, amber displacement arrows for placement moves (each engine
`placement_cohort` — a group move — collapses to one averaged arrow, and the
summary panel lists cohorts as "N moved together, Δ x m"), a summary panel,
and a click inspector (class, GUID, aspects, placement delta, match
reason/score).
Report `step_id`s are the express ids the browser parser emits, so no id
mapping layer exists anywhere.

## `viewer/`

Standalone SPA: Svelte 5 + Vite + TypeScript, managed with bun. The only
JavaScript in the repo; everything outside `viewer/` stays Python. Tessellation
is `@ifc-lite/geometry` (WASM, streaming). Each file comes back in its own
local frame (the tessellator subtracts a per-file RTC offset in IFC Z-up);
`src/lib/frame.ts` maps that offset to three.js Y-up and the scene translates
the new model by the origin difference so both land in the old model's frame.
Rendering is three.js:
`@ifc-lite/renderer` was evaluated and rejected because per-entity color
overrides are not public API there (they take internal GPU handles) and its
federation registry assumes one expressID namespace, while a diff holds two
models with colliding STEP ids. `src/lib/ifc.ts` is the only module that
touches ifc-lite — the contract stays "tessellate file → meshes per STEP id",
so parser and renderer can be swapped independently.

- `src/lib/report.ts` + `renderState.ts` — pure, unit-tested core: report
  validation, `DiffIndex` (side × STEP id → entity record), and the bucket
  model. Every entity lands in exactly one visual bucket
  (`deleted/added/modifiedOld/modifiedNew/modifiedStatic/unchanged/extra`);
  modified pairs whose geometry+placement are identical render once
  (`modifiedStatic`) to avoid z-fighting; unchanged and extra render new-side
  only, and `unreportedEntities` lists old-side meshes without a report record
  for the legend.
  Per-bucket appearance (visible/opacity/color) is a pure function of slider t
  and toggles.
- `src/lib/scene.ts` — three.js scene: one merged mesh per bucket, so slider
  drags are O(buckets) material updates regardless of entity count; per-entity
  index ranges drive raycast picking and a zero-copy highlight overlay.
- `src/lib/sources.ts` — both input paths: drag-drop (two IFC files +
  report.json; `assignRoles` takes old/new from the report's
  `audit.inputs.*.sha256`, and an unverifiable pair goes to an explicit
  confirm step, never name order) and `?src=<origin>` manifest handoff.
  `dataUrl` resolves every fetched url under the src path and carries the
  src's query, plus the page's when the page is served from the src origin,
  so a launch token in either reaches the server and never another origin.
  `parseManifest` is a pure,
  unit-tested function that normalizes v1 single-pair and v2 chain manifests
  into one `steps[]` shape (a v1 manifest is one step); each `StepSource`
  carries display names, per-file cache keys, and lazy byte/report resolvers so
  App.svelte fetches and tessellates a step only when it becomes active.
  App.svelte keeps a per-url mesh cache across the whole chain, so switching
  to an adjacent step re-tessellates only the one new revision. Files are
  dropped only onto the empty view, so dropped meshes and reports are keyed by
  role; Start over clears both caches and disposes the scene.
- `src/lib/debug.ts` — `window.__athar_viewer` state mirror (phase, bucket
  counts, report coverage per legend row, appearances, selection) so headless
  tests assert
  state, not pixels; the app stays usable without WebGL (panels work, canvas
  degrades).
- `viewer/dist/` is generated and ignored. Use `make viewer-build` for local
  offline testing; release builds can copy it into ignored `athar_view/static`
  with `make viewer-package-static` before building the Python distribution.

## `athar_view/`

Thin Python launcher. Depends on the core engine and the `athar_git` bundle
cache; nothing depends on it.

- `server.py` — Builds the report through cached signature bundles, then serves
  the manifest contract on `127.0.0.1` only. Data endpoints sit under a
  per-launch `secrets.token_urlsafe` path prefix (`server.data_url`, the
  viewer's `src`): `<token>/manifest.json` (`athar_viewer_manifest: 1` plus
  `schema_version` for the viewer handshake) and
  `<token>/files/old.ifc|new.ifc|report.json`; manifest file urls are relative
  so the viewer resolves them under the token. The static SPA (offline only)
  is served at `/` without the token and never redirects to it. Every request
  must carry the server's own `Host: 127.0.0.1:<port>` (421 otherwise, the
  DNS-rebinding guard); a request with an `Origin` must come from the server's
  own origin or the one configured `allowed_origin` (403 otherwise). Only that
  configured origin (hosted hand-off) gets `Access-Control-Allow-Origin`; there
  is no wildcard or private-network allowance.
  `Cross-Origin-Resource-Policy: same-origin` and a path-traversal guard apply
  everywhere.
- Manifest schema v2 (chain) — `MANIFEST_SCHEMA_VERSION = 2`. A chain manifest
  drops the top-level `old/new/report` and carries `steps: [{label, old, new,
  report}, ...]` plus `active_step`. Each shared revision is served once under a
  stable per-file url (`files/model/{i}.ifc`); adjacent steps reuse it, so a
  file that is the new-side of one step and the old-side of the next has one url
  (the viewer keys its mesh cache on that url). Step reports are built lazily on
  first `GET <token>/files/report/{step}.json` and cached, so an N-file chain never
  pays for diffs the user does not open. A v1 single-pair manifest is
  equivalent to a one-step chain; the viewer treats "no `steps` field" as the
  legacy single-pair form, and both remain served and drag-drop loadable.
- `cli.py` — `athar view a.ifc b.ifc [c.ifc …] [--offline] [--no-open]
  [--port N] [--viewer-url URL] [--dev-origin URL] [--cache-dir DIR]`. Two files is a single diff
  (v1 manifest); three or more builds a chain of
  consecutive pair steps (`r2→r3, r3→r4, …`) served as a v2 manifest. Hosted
  mode opens
  `<viewer-url>?src=<local data url>` Perfetto-style (URL from `--viewer-url`
  or `ATHAR_VIEWER_URL`) and requires `--dev-origin` equal to that URL's
  origin (both normalised to what a browser sends: lowercase scheme and host,
  no default port), the one CORS origin the server grants; a missing or different
  `--dev-origin` is refused before the reachability check, and the error
  names `--dev-origin <origin>` and `--offline`. Nothing is uploaded. It
  falls back to offline if the hosted origin is unreachable. A Vite dev server uses
  `--viewer-url http://localhost:5173/ --dev-origin http://localhost:5173`. The
  `Viewer:` URL is printed and flushed before serving, so `--no-open` works
  through a pipe.
  Offline mode — the default while no hosted deployment exists — serves
  `ATHAR_VIEWER_DIST`, release-packaged `athar_view/static`, or local
  `viewer/dist`, and grants `--dev-origin` the same CORS access when given
  (including after a hosted fallback). Inputs over 50MB warn but proceed.
- `athar git install --difftool` additionally sets
  `difftool.athar.cmd = <athar> view "$LOCAL" "$REMOTE"` (the same absolute
  `<athar>` prefix as the diff driver, see `athar_git/AGENTS.md`), so
  `git difftool -t athar -- model.ifc` opens the visual diff for any committed
  revision pair.

## Testing

Viewer tests live in `viewer/`. `bun test tests` unit-tests the pure
report→render-state mapping (parsing, bucket assignment, crossfade appearances)
and manifest parsing (`sources.test.ts`: v1 accepted, v2 chain parsed,
`active_step` honored, out-of-range `active_step` and malformed chain
rejected). `bunx playwright test` runs a headless smoke against the committed
fixture pair in `viewer/e2e/fixtures/` (regenerate with
`scripts/make_viewer_fixture.py`), asserting bucket counts and report coverage
through the `window.__athar_viewer` debug state rather than pixels — it
covers the `?src=` handoff path, the file-input path, slider snaps/toggles, and
click selection. `chain.spec.ts` synthesizes a 2-step v2 manifest over the same
fixtures and asserts prev/next + `[`/`]` navigation swaps the step
(`activeStep`/`stepLabel` in the debug state) with bucket counts stable across
the switch. `frame.spec.ts` moves the fixture site to a large world coordinate
(so each file gets a different RTC offset) and asserts an element's old and new
bounds coincide through the `entityBounds` debug probe.
`viewer/e2e/corpus.spec.ts` is opt-in acceptance against a live launcher:
`athar view OLD NEW --offline --no-open --port 4799 --dev-origin http://127.0.0.1:4173`
(Playwright serves the app from port 4173, so its fetches to 4799 are
cross-origin), then
`ATHAR_E2E_SRC=<the src from the printed Viewer URL> bunx playwright test corpus` (verified on
Building-Landscaping v0→v3 and a GNI 2026 arc/structure pair). On the Python
side, `tests/test_athar_view.py` covers report bytes + cache population, server
endpoints over real HTTP (token, foreign `Host`, foreign `Origin`, configured
origin CORS/PNA preflight, traversal guard), the piped `--no-open` URL line, offline/hosted/fallback modes,
`--difftool` git config, and `python -m athar view` dispatch.
