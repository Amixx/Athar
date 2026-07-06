# Diff Viewer Agent Notes (`viewer/` + `athar_view/`)

Visual presentation layer. It may depend on the core engine and the `athar_git`
bundle cache; nothing in `athar/` may depend on it.

Onshape-style visual overlay diff: both models tessellated in the browser and
rendered in one 3D scene, colored by the engine's report (green added, red
deleted, blue modified, gray ghosted unchanged), with an old↔new crossfade
slider, amber displacement lines for placement moves, a summary panel, and a
click inspector (class, GUID, aspects, placement delta, match reason/score).
Report `step_id`s are the express ids the browser parser emits, so no id
mapping layer exists anywhere.

## `viewer/`

Standalone SPA: Svelte 5 + Vite + TypeScript, managed with bun. The only
JavaScript in the repo; everything outside `viewer/` stays Python. Tessellation
is `@ifc-lite/geometry` (WASM, streaming; model 1's RTC offset is reused for
model 2 so both land in one coordinate frame). Rendering is three.js:
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
  (`modifiedStatic`) to avoid z-fighting; unchanged renders new-side only.
  Per-bucket appearance (visible/opacity/color) is a pure function of slider t
  and toggles.
- `src/lib/scene.ts` — three.js scene: one merged mesh per bucket, so slider
  drags are O(buckets) material updates regardless of entity count; per-entity
  index ranges drive raycast picking and a zero-copy highlight overlay.
- `src/lib/sources.ts` — both input paths: drag-drop (old.ifc + new.ifc +
  report.json) and `?src=<origin>` manifest handoff. `parseManifest` is a pure,
  unit-tested function that normalizes v1 single-pair and v2 chain manifests
  into one `steps[]` shape (a v1 manifest is one step); each `StepSource`
  carries display names, per-file cache keys, and lazy byte/report resolvers so
  App.svelte fetches and tessellates a step only when it becomes active.
  App.svelte keeps a per-url mesh cache and one shared RTC offset (captured from
  the first file tessellated) across the whole chain, so switching to an
  adjacent step re-tessellates only the one new revision.
- `src/lib/debug.ts` — `window.__athar_viewer` state mirror (phase, bucket
  counts, meshless counts, appearances, selection) so headless tests assert
  state, not pixels; the app stays usable without WebGL (panels work, canvas
  degrades).
- `viewer/dist/` is generated and ignored. Use `make viewer-build` for local
  offline testing; release builds can copy it into ignored `athar_view/static`
  with `make viewer-package-static` before building the Python distribution.

## `athar_view/`

Thin Python launcher. Depends on the core engine and the `athar_git` bundle
cache; nothing depends on it.

- `server.py` — Builds the report through cached signature bundles, then serves
  the manifest contract on `127.0.0.1`: `/manifest.json`
  (`athar_viewer_manifest: 1` plus `schema_version` for the viewer handshake),
  `/files/old.ifc|new.ifc|report.json`, and the static SPA when offline. CORS
  supports hosted handoff: origin-restricted `Access-Control-Allow-Origin`,
  Chrome Private Network Access preflight, `Cross-Origin-Resource-Policy:
  cross-origin`, path-traversal guard.
- Manifest schema v2 (chain) — `MANIFEST_SCHEMA_VERSION = 2`. A chain manifest
  drops the top-level `old/new/report` and carries `steps: [{label, old, new,
  report}, ...]` plus `active_step`. Each shared revision is served once under a
  stable per-file url (`/files/model/{i}.ifc`); adjacent steps reuse it, so a
  file that is the new-side of one step and the old-side of the next has one url
  (the viewer keys its mesh cache on that url). Step reports are built lazily on
  first `GET /files/report/{step}.json` and cached, so an N-file chain never
  pays for diffs the user does not open. A v1 single-pair manifest is
  equivalent to a one-step chain; the viewer treats "no `steps` field" as the
  legacy single-pair form, and both remain served and drag-drop loadable.
- `cli.py` — `athar view a.ifc b.ifc [c.ifc …] [--offline] [--no-open]
  [--port N] [--viewer-url URL] [--cache-dir DIR]`. Two files is a single diff
  (v1 manifest, byte-identical to before); three or more builds a chain of
  consecutive pair steps (`r2→r3, r3→r4, …`) served as a v2 manifest. Hosted
  mode opens
  `<viewer-url>?src=<local origin>` Perfetto-style (URL from `--viewer-url` or
  `ATHAR_VIEWER_URL`; nothing is uploaded — the hosted page fetches from
  localhost) and falls back to offline if the hosted origin is unreachable.
  Offline mode — the default while no hosted deployment exists — serves
  `ATHAR_VIEWER_DIST`, release-packaged `athar_view/static`, or local
  `viewer/dist`. Inputs over 50MB warn but proceed.
- `athar git install --difftool` additionally sets
  `difftool.athar.cmd = athar view "$LOCAL" "$REMOTE"`, so
  `git difftool -t athar -- model.ifc` opens the visual diff for any committed
  revision pair.

## Testing

Viewer tests live in `viewer/`. `bun test tests` unit-tests the pure
report→render-state mapping (parsing, bucket assignment, crossfade appearances)
and manifest parsing (`sources.test.ts`: v1 accepted, v2 chain parsed,
`active_step` honored, malformed chain rejected). `bunx playwright test` runs a
headless smoke against the committed fixture pair in `viewer/e2e/fixtures/`
(regenerate with `scripts/make_viewer_fixture.py`), asserting bucket/meshless
counts through the `window.__athar_viewer` debug state rather than pixels — it
covers the `?src=` handoff path, the file-input path, slider snaps/toggles, and
click selection. `chain.spec.ts` synthesizes a 2-step v2 manifest over the same
fixtures and asserts prev/next + `[`/`]` navigation swaps the step
(`activeStep`/`stepLabel` in the debug state) with bucket counts stable across
the switch.
`viewer/e2e/corpus.spec.ts` is opt-in acceptance against a live launcher:
`athar view OLD NEW --offline --no-open --port 4799`, then
`ATHAR_E2E_SRC=http://127.0.0.1:4799 bunx playwright test corpus` (verified on
Building-Landscaping v0→v3 and a GNI 2026 arc/structure pair). On the Python
side, `tests/test_athar_view.py` covers report bytes + cache population, server
endpoints/CORS/PNA preflight/traversal guard, offline/hosted/fallback modes,
`--difftool` git config, and `python -m athar view` dispatch.
