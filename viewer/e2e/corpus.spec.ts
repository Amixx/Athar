/**
 * Acceptance run against a live `athar view` server on a real corpus pair.
 * Opt-in: skipped unless ATHAR_E2E_SRC points at a running server origin —
 *
 *   athar view old.ifc new.ifc --offline --no-open --port 4799
 *   ATHAR_E2E_SRC=http://127.0.0.1:4799 bunx playwright test corpus
 *
 * Verifies the served manifest/report against what the app actually
 * bucketed: every report entity, whatever its change scope, is either drawn
 * or listed as undrawn under its legend row. Saves a screenshot to test-results/corpus.png.
 */

import { expect, test } from '@playwright/test'

const SRC = process.env.ATHAR_E2E_SRC

test('athar view serves a corpus pair the app can bucket completely', async ({ page }) => {
  test.skip(!SRC, 'set ATHAR_E2E_SRC to a running athar view origin')
  test.setTimeout(300_000)

  const report = await (await fetch(`${SRC}/files/report.json`)).json()

  await page.goto(`/?src=${SRC}`)
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
  // Crossfade pairs are symmetric.
  expect(buckets.modifiedOld ?? 0).toBe(buckets.modifiedNew ?? 0)

  await page.waitForTimeout(800)
  await page.screenshot({ path: 'test-results/corpus.png' })
})
