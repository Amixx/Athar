/**
 * Acceptance run against a live `athar view` server on a real corpus pair.
 * Opt-in: skipped unless ATHAR_E2E_SRC points at a running server origin —
 *
 *   athar view old.ifc new.ifc --offline --no-open --port 4799
 *   ATHAR_E2E_SRC=http://127.0.0.1:4799 bunx playwright test corpus
 *
 * Verifies the served manifest/report against what the app actually
 * bucketed: every report entity is either bucketed or counted meshless,
 * per section. Saves a screenshot to test-results/corpus.png.
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
  const meshless = state.meshlessBySection as Record<string, number>

  // Section accounting: bucketed + meshless covers every report entity.
  expect((buckets.added ?? 0) + meshless.added).toBe(report.stats.added)
  expect((buckets.deleted ?? 0) + meshless.deleted).toBe(report.stats.deleted)
  expect(
    (buckets.modifiedNew ?? 0) + (buckets.modifiedStatic ?? 0) + meshless.modified,
  ).toBe(report.stats.modified)
  expect((buckets.unchanged ?? 0) + meshless.unchanged).toBe(report.stats.unchanged)
  // Crossfade pairs are symmetric.
  expect(buckets.modifiedOld ?? 0).toBe(buckets.modifiedNew ?? 0)

  await page.waitForTimeout(800)
  await page.screenshot({ path: 'test-results/corpus.png' })
})
