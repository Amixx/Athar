# Athar Viewer

Standalone SPA for visual IFC diffing: both models tessellated in the browser
and overlaid in one 3D scene, colored by an Athar delta report. This directory
is the only JavaScript in the repository; everything else stays Python.

## Stack

- Svelte 5 (runes) + Vite + TypeScript, managed with [bun](https://bun.sh).
- `@ifc-lite/geometry` for in-browser WASM tessellation (streaming; model 1's
  RTC offset is reused for model 2 so both share one coordinate frame).
- three.js for rendering. `@ifc-lite/renderer` was evaluated and rejected:
  per-entity color overrides are not public API there (they require internal
  GPU device/pipeline handles), and its federation registry assumes a single
  expressID namespace while a diff holds two models with colliding STEP ids.
  `src/lib/ifc.ts` is the only module that imports ifc-lite; the contract is
  "tessellate file → meshes per STEP id", so the parser or renderer can be
  swapped without touching the rest of the app.

## Layout

| Module | Role |
| --- | --- |
| `src/lib/types.ts` | TS mirror of the engine report shape (`athar/delta/report.py`) |
| `src/lib/report.ts` | Report validation + `DiffIndex` (side × STEP id → entity record) |
| `src/lib/renderState.ts` | Pure core: bucket assignment, crossfade appearances, count helpers |
| `src/lib/ifc.ts` | ifc-lite tessellation (the only ifc-lite import) |
| `src/lib/scene.ts` | three.js scene: merged per-bucket meshes, picking, highlight, displacement lines, camera fits |
| `src/lib/sources.ts` | Input paths: `?src=` manifest handoff and drag-drop classification |
| `src/lib/debug.ts` | `window.__athar_viewer` state mirror for headless tests |
| `src/App.svelte` | UI shell: loading flow, slider, summary, inspector, toggles |

Every report entity lands in exactly one visual bucket — `deleted`, `added`,
`modifiedOld`, `modifiedNew`, `modifiedStatic` (modified pairs with identical
geometry+placement, rendered once to avoid z-fighting), `unchanged` (new side
only), or `extra` (meshes the report doesn't mention). Each bucket is one
merged `THREE.Mesh`, so slider drags update O(buckets) materials regardless of
entity count; per-entity index ranges drive raycast picking and a zero-copy
highlight overlay.

## Inputs

1. **Drag-drop / file pickers**: `old.ifc`, `new.ifc`, and the report JSON
   (`python -m athar old.ifc new.ifc > report.json`).
2. **`?src=<origin>`**: fetches `<origin>/manifest.json`
   (`athar_viewer_manifest: 1`, `schema_version` handshake) and the files it
   points at. This is what `athar view` (in `athar_view/`) serves.

## Commands

```bash
bun install
bun run dev          # vite dev server
bun run check        # svelte-check + tsc
bun test tests       # unit tests (pure report→render-state core)
bun run test:e2e     # playwright headless smoke (builds + previews on :4173)
bun run build        # production bundle into dist/
```

`viewer/dist` is committed so `athar view` works from a plain pip install.
After changing viewer sources, rebuild and commit it (`make viewer-build`
from the repo root).

## Testing

- `tests/renderState.test.ts` — bun unit tests for parsing, bucket
  assignment, and crossfade appearances.
- `e2e/smoke.spec.ts` — headless playwright against the committed fixture
  pair in `e2e/fixtures/` (regenerate with
  `python scripts/make_viewer_fixture.py` from the repo root). Asserts bucket
  and meshless counts through `window.__athar_viewer` instead of pixels, and
  covers both input paths, slider snaps, toggles, and click selection.
- `e2e/corpus.spec.ts` — opt-in acceptance against a live launcher:

  ```bash
  athar view old.ifc new.ifc --offline --no-open --port 4799 &
  ATHAR_E2E_SRC=http://127.0.0.1:4799 bunx playwright test corpus
  ```

The app publishes its state to `window.__athar_viewer` (phase, bucket entity
counts, meshless counts per section, appearances, slider position, selection,
`rendererReady`). WebGL failure degrades gracefully — panels and counts still
work with `rendererReady: false` — which keeps every assertion runnable in
headless environments.
