/**
 * Headless smoke test over the committed tiny fixture pair
 * (viewer/e2e/fixtures — regenerate with scripts/make_viewer_fixture.py).
 *
 * Assertions read the `window.__athar_viewer` state mirror instead of pixels,
 * so the test stays deterministic even where WebGL is unavailable; the WASM
 * tessellation + report mapping always runs.
 *
 * Expected counts are facts of the committed fixture (walls carry meshes,
 * spatial containers do not):
 *   deleted=WallC, added=WallE, modifiedOld/New=WallB (moved +1 m),
 *   modifiedStatic=WallD (data-only), transitive=WallA,
 *   not drawn: transitive=Building+Storey, unchanged=Site; 1 displacement line.
 */

import { createServer, type Server } from 'node:http'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { AddressInfo } from 'node:net'
import { expect, test, type Page } from '@playwright/test'

const FIXTURES = fileURLToPath(new URL('./fixtures', import.meta.url))

const EXPECTED_BUCKETS = {
  deleted: 1,
  added: 1,
  modifiedOld: 1,
  modifiedNew: 1,
  modifiedStatic: 1,
  transitive: 1,
}
const EXPECTED_IN_REPORT = { added: 1, deleted: 1, modified: 2, transitive: 3, unchanged: 1 }
const EXPECTED_DRAWN = { added: 1, deleted: 1, modified: 2, transitive: 1, unchanged: 0 }

let server: Server
let srcOrigin: string

test.beforeAll(async () => {
  server = createServer(async (req, res) => {
    const path = (req.url ?? '/').split('?')[0]
    const cors = {
      'access-control-allow-origin': '*',
      'cross-origin-resource-policy': 'cross-origin',
      'cache-control': 'no-store',
    }
    try {
      const file = path === '/' ? '/manifest.json' : path
      const data = await readFile(join(FIXTURES, file))
      res.writeHead(200, {
        ...cors,
        'content-type': file.endsWith('.json') ? 'application/json' : 'application/octet-stream',
      })
      res.end(data)
    } catch {
      res.writeHead(404, cors)
      res.end()
    }
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  srcOrigin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
})

test.afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()))
})

async function waitReady(page: Page): Promise<Record<string, any>> {
  await page.waitForFunction(
    () => {
      const state = (window as any).__athar_viewer
      return state?.phase === 'ready' || state?.phase === 'error'
    },
    undefined,
    { timeout: 60_000 },
  )
  const state = await page.evaluate(() => (window as any).__athar_viewer)
  expect(state.error ?? '').toBe('')
  expect(state.phase).toBe('ready')
  return state
}

for (const width of [375, 800, 1280, 1920]) {
  test(`panels do not overlap at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 })
    const step = {
      label: 'Revision comparison',
      old: { name: 'ammerforum-v1-1237.ifc', url: '/old.ifc' },
      new: { name: 'ammerforum-v2-1347.ifc', url: '/new.ifc' },
      report: { url: '/report.json' },
    }
    await page.route(`${srcOrigin}/manifest.json`, route => route.fulfill({ json: {
      athar_viewer_manifest: 1, schema_version: 999, steps: [step, step], active_step: 0,
    } }))
    await page.goto(`/?src=${srcOrigin}`)
    await waitReady(page)
    await page.getByTestId('undrawn').getByRole('button').filter({ hasText: 'Storey' }).click()
    const layout = await page.evaluate(() => {
      const panels = Array.from(document.querySelectorAll('.app > .panel, .brand-stack > .panel'))
        .map(el => ({ name: el.className, rect: el.getBoundingClientRect() }))
        .filter(({ rect }) => rect.width > 0 && rect.height > 0)
      const overlaps: string[] = []
      for (let i = 0; i < panels.length; i++) {
        for (const b of panels.slice(i + 1)) {
          const a = panels[i]
          if (Math.min(a.rect.right, b.rect.right) > Math.max(a.rect.left, b.rect.left) + 1 &&
              Math.min(a.rect.bottom, b.rect.bottom) > Math.max(a.rect.top, b.rect.top) + 1) {
            overlaps.push(`${a.name} / ${b.name}`)
          }
        }
      }
      return { overlaps, width: document.documentElement.scrollWidth, panels: panels.map(({ rect }) => ({ left: rect.left, right: rect.right })) }
    })
    expect(layout.overlaps).toEqual([])
    expect(layout.width).toBe(width)
    for (const rect of layout.panels) {
      expect(rect.left).toBeGreaterThanOrEqual(0)
      expect(rect.right).toBeLessThanOrEqual(width)
    }
    for (const button of await page.locator('.toolbar button, .viewbtns button, .inspector .close').all()) {
      await button.scrollIntoViewIfNeeded()
      await button.click({ trial: true })
    }
  })
}

test('loads a pair via ?src= hand-off and buckets entities', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  const state = await waitReady(page)

  expect(state.bucketEntityCounts).toEqual(EXPECTED_BUCKETS)
  expect(state.reportCoverage.inReport).toEqual(EXPECTED_IN_REPORT)
  expect(state.reportCoverage.drawn).toEqual(EXPECTED_DRAWN)
  expect(state.displacementLines).toBe(1)
  expect(state.reportStats.added).toBe(1)
  expect(state.reportStats.deleted).toBe(1)
  expect(state.reportStats.modified).toBe(5)
  expect(state.reportStats.unchanged).toBe(1)
  expect(typeof state.rendererReady).toBe('boolean')

  // Frame for humans/debugging — assertions above never read pixels.
  await page.waitForTimeout(500)
  await page.screenshot({ path: 'test-results/smoke-both.png' })
})

test('the legend shows report and drawn counts and lists undrawn entities for inspection', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  await waitReady(page)

  await expect(page.getByTestId('legend-transitive')).toContainText('3 in report, 1 drawn')
  await expect(page.getByTestId('legend-unchanged')).toContainText('1 in report, 0 drawn')
  const undrawn = page.getByTestId('undrawn').getByRole('button')
  await expect(undrawn).toHaveText([/Building/, /Storey/, /Site/])

  await undrawn.filter({ hasText: 'Storey' }).click()
  await expect(page.locator('.inspector .title')).toHaveText('Storey')
  await expect(page.locator('.inspector .badge')).toHaveText('modified')
})

test('every legend row keeps its label visibly apart from its count', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  await waitReady(page)

  const rows = page.locator('.legendrow')
  expect(await rows.count()).toBeGreaterThan(1)
  for (const row of await rows.all()) {
    const gap = await row.evaluate((el) => {
      const [label, count] = Array.from(el.children).map((child) => child.getBoundingClientRect())
      return count.left - label.right
    })
    expect(gap, await row.innerText()).toBeGreaterThanOrEqual(8)
  }
})

test('the legend says drawing is unavailable instead of drawn counts without WebGL', async ({ page }) => {
  await page.addInitScript(() => {
    const getContext = HTMLCanvasElement.prototype.getContext
    HTMLCanvasElement.prototype.getContext = function (this: HTMLCanvasElement, kind: string, ...rest: unknown[]) {
      return kind.startsWith('webgl') ? null : (getContext as any).call(this, kind, ...rest)
    } as typeof getContext
  })
  await page.goto(`/?src=${srcOrigin}`)
  const state = await waitReady(page)
  expect(state.rendererReady).toBe(false)

  await expect(page.getByTestId('legend-transitive')).toHaveText(/Indirect\s*3 in report$/)
  await expect(page.getByTestId('legend-extra')).toHaveText(/Extra\s*0 old, 0 new$/)
  await expect(page.getByTestId('legend-drawing')).toHaveText('3D drawing unavailable: nothing is drawn.')
})

test('slider snaps and section toggles drive bucket appearances', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  await waitReady(page)
  const debug = () => page.evaluate(() => (window as any).__athar_viewer)

  // Default Both state: crossfade midpoint.
  let state = await debug()
  expect(state.sliderT).toBe(0.5)
  expect(state.appearances.deleted.opacity).toBe(0.5)
  expect(state.appearances.added.opacity).toBe(0.5)

  // Old snap: deleted fully visible, added gone.
  await page.getByRole('button', { name: 'Old', exact: true }).click()
  await page.waitForFunction(() => (window as any).__athar_viewer?.sliderT === 0)
  state = await debug()
  expect(state.appearances.deleted.visible).toBe(true)
  expect(state.appearances.deleted.opacity).toBe(1)
  expect(state.appearances.added.visible).toBe(false)
  expect(state.appearances.modifiedOld.visible).toBe(true)
  expect(state.appearances.modifiedNew.visible).toBe(false)
  expect(state.appearances.modifiedStatic.visible).toBe(true)

  await page.waitForTimeout(400)
  await page.screenshot({ path: 'test-results/smoke-old.png' })

  // New snap: mirrored.
  await page.getByRole('button', { name: 'New', exact: true }).click()
  await page.waitForFunction(() => (window as any).__athar_viewer?.sliderT === 1)
  state = await debug()
  expect(state.appearances.deleted.visible).toBe(false)
  expect(state.appearances.added.visible).toBe(true)
  expect(state.appearances.modifiedNew.visible).toBe(true)

  // Section toggle hides every direct-modified bucket; indirect has its own.
  await page.getByRole('button', { name: 'Direct', exact: true }).click()
  await page.waitForFunction(
    () => (window as any).__athar_viewer?.appearances.modifiedStatic.visible === false,
  )
  state = await debug()
  expect(state.appearances.modifiedNew.visible).toBe(false)
  expect(state.appearances.modifiedOld.visible).toBe(false)
  expect(state.appearances.transitive.visible).toBe(true)
  await page.getByRole('button', { name: 'Indirect', exact: true }).click()
  await page.waitForFunction(
    () => (window as any).__athar_viewer?.appearances.transitive.visible === false,
  )

  // Unchanged ghosting toggle.
  expect(state.appearances.unchanged.opacity).toBe(1)
  await page.getByRole('button', { name: 'Ghost' }).click()
  await page.waitForFunction(
    () => (window as any).__athar_viewer?.appearances.unchanged.opacity < 0.5,
  )
  await page.getByRole('button', { name: 'Ghost' }).click()
  await page.waitForFunction(
    () => (window as any).__athar_viewer?.appearances.unchanged.opacity === 1,
  )
})

test('picking excludes ghosted and toggled-off geometry', async ({ page }) => {
  const report = JSON.parse(await readFile(join(FIXTURES, 'report.json'), 'utf8'))
  report.unchanged.push(...report.modified)
  report.modified = []
  report.added = []
  report.deleted = []
  await page.route(`${srcOrigin}/report.json`, route => route.fulfill({ json: report }))
  await page.goto(`/?src=${srcOrigin}`)
  const state = await waitReady(page)
  expect(state.rendererReady).toBe(true)
  const viewport = page.viewportSize()!
  await page.mouse.click(viewport.width / 2, viewport.height / 2)
  await expect(page.locator('.inspector')).toBeVisible()
  await page.keyboard.press('Escape')
  await page.getByRole('button', { name: 'Ghost', exact: true }).click()
  await page.mouse.click(viewport.width / 2, viewport.height / 2)
  await expect(page.locator('.inspector')).toHaveCount(0)
  await page.getByRole('button', { name: 'Ghost', exact: true }).click()
  await page.getByRole('button', { name: 'Unchanged', exact: true }).click()
  await page.mouse.click(viewport.width / 2, viewport.height / 2)
  await expect(page.locator('.inspector')).toHaveCount(0)
})

test('clicking an entity opens the inspector; Escape clears it', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  const state = await waitReady(page)
  test.skip(!state.rendererReady, 'WebGL unavailable in this environment')

  // After fit-to-model the wall cluster covers the viewport center.
  const viewport = page.viewportSize()!
  await page.mouse.click(viewport.width / 2, viewport.height / 2)
  await page.waitForFunction(() => Boolean((window as any).__athar_viewer?.selection))

  const selection = await page.evaluate(() => (window as any).__athar_viewer.selection)
  expect(['old', 'new']).toContain(selection.side)
  expect(selection.stepId).toBeGreaterThan(0)
  await expect(page.locator('.inspector')).toBeVisible()
  await expect(page.locator('.inspector .title')).not.toBeEmpty()
  const firstHighlight = await page.evaluate(() => (window as any).__athar_viewer.highlightObjects)
  expect(firstHighlight).not.toBeNull()

  for (let i = 0; i < 3; i += 1) {
    await page.keyboard.press('Escape')
    await page.mouse.click(viewport.width / 2, viewport.height / 2)
    await page.waitForFunction(() => Boolean((window as any).__athar_viewer?.selection))
  }
  const repeatedHighlight = await page.evaluate(() => (window as any).__athar_viewer.highlightObjects)
  expect(repeatedHighlight).toEqual(firstHighlight)

  await page.keyboard.press('Escape')
  await page.waitForFunction(() => (window as any).__athar_viewer?.selection === null)
  await expect(page.locator('.inspector')).toHaveCount(0)
})
