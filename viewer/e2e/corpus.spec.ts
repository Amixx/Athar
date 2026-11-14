/**
 * Acceptance run against a live `athar view` server on a real corpus pair.
 * Opt-in: skipped unless ATHAR_E2E_SRC is set. Launch the server with the
 * Playwright preview origin granted, then set ATHAR_E2E_SRC to the `src`
 * value of the viewer url it prints (the per-launch data url):
 *
 *   athar view old.ifc new.ifc --offline --no-open --dev-origin http://127.0.0.1:4173
 *   ATHAR_E2E_SRC=<printed src url> bunx playwright test corpus
 *
 * Verifies the served manifest/report against what the app actually
 * bucketed: every report entity, whatever its change scope, is either drawn
 * or listed as undrawn under its legend row. Saves a screenshot to test-results/corpus.png.
 */

import { expect, test } from '@playwright/test'
import { dataUrl, parseManifest } from '../src/lib/sources'

const SRC = process.env.ATHAR_E2E_SRC

test('athar view serves a corpus pair the app can bucket completely', async ({ page }) => {
  test.skip(!SRC, 'set ATHAR_E2E_SRC to a running athar view origin')
  test.setTimeout(300_000)

  const manifest = parseManifest(await (await fetch(dataUrl(SRC!, SRC!, 'manifest.json'))).json())
  const step = manifest.steps[manifest.activeStep]
  const report = await (await fetch(dataUrl(SRC!, SRC!, step.report.url))).json()

  await page.goto(`/?src=${encodeURIComponent(SRC!)}`)
  await page.waitForFunction(
    () => {
      const state = (window as any).__athar_viewer
      return state?.phase === 'ready' || state?.phase === 'error'
    },
    undefined,
    { timeout: 240_000 },
  )
  const state = await page.evaluate(() => (window as any).__athar_viewer)
  expect(state.error ?? '').toBe('')
  expect(state.phase).toBe('ready')

  const buckets = state.bucketEntityCounts as Record<string, number>
  const coverage = state.reportCoverage as {
    inReport: Record<string, number>
    drawn: Record<string, number>
    undrawn: Array<{ key: string }>
  }

  // Every change scope the report has lands in exactly one legend row.
  const scopes = report.modified.reduce((counts: Record<string, number>, item: any) => {
    counts[item.change_scope] = (counts[item.change_scope] ?? 0) + 1
    return counts
  }, {})
  const { transitive = 0, ...direct } = scopes
  expect(coverage.inReport.transitive).toBe(transitive)
  expect(coverage.inReport.modified).toBe(Object.values(direct).reduce((a: number, b) => a + (b as number), 0))
  expect(coverage.inReport.modified + coverage.inReport.transitive).toBe(report.stats.modified)
  expect(coverage.inReport.added).toBe(report.stats.added)
  expect(coverage.inReport.deleted).toBe(report.stats.deleted)
  expect(coverage.inReport.unchanged).toBe(report.stats.unchanged)

  // Drawn plus listed-undrawn covers every report entity, per legend row.
  for (const key of Object.keys(coverage.inReport)) {
    const undrawn = coverage.undrawn.filter((entity) => entity.key === key).length
    expect(coverage.drawn[key] + undrawn).toBe(coverage.inReport[key])
  }
  for (const key of ['added', 'deleted', 'transitive', 'unchanged']) {
    expect(coverage.drawn[key]).toBe(buckets[key] ?? 0)
  }
  // A direct change crossfades when its shape or placement changed, and is
  // drawn once on the new side otherwise; each side is drawn where it has meshes.
  const directPairs = report.modified.filter((item: any) => item.change_scope !== 'transitive')
  const crossfades = (item: any) => item.aspects.placement === 'changed' || item.aspects.geometry === 'changed'
  const expected = await page.evaluate(
    ({ crossfade, still }) => {
      const state = (window as any).__athar_viewer
      const meshed = (side: string, stepId: number) => state.entityBounds(side, stepId) !== null
      return {
        modifiedOld: crossfade.filter(([oldId]) => meshed('old', oldId)).length,
        modifiedNew: crossfade.filter(([, newId]) => meshed('new', newId)).length,
        modifiedStatic: still.filter((newId) => meshed('new', newId)).length,
      }
    },
    {
      crossfade: directPairs.filter(crossfades).map((item: any) => [item.old.step_id, item.new.step_id] as [number, number]),
      still: directPairs.filter((item: any) => !crossfades(item)).map((item: any) => item.new.step_id as number),
    },
  )
  expect({
    modifiedOld: buckets.modifiedOld ?? 0,
    modifiedNew: buckets.modifiedNew ?? 0,
    modifiedStatic: buckets.modifiedStatic ?? 0,
  }).toEqual(expected)

  await page.waitForTimeout(800)
  await page.screenshot({ path: 'test-results/corpus.png' })
})
