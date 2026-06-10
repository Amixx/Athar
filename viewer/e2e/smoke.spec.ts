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
 *   modifiedStatic=WallA (transitive) + WallD (data-only),
 *   meshless: modified=Building+Storey, unchanged=Site; 1 displacement line.
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
  modifiedStatic: 2,
}
const EXPECTED_MESHLESS = { added: 0, deleted: 0, modified: 2, unchanged: 1 }

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

test('loads a pair via ?src= hand-off and buckets entities', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  const state = await waitReady(page)

  expect(state.bucketEntityCounts).toEqual(EXPECTED_BUCKETS)
  expect(state.meshlessBySection).toEqual(EXPECTED_MESHLESS)
  expect(state.displacementLines).toBe(1)
  expect(state.reportStats.added).toBe(1)
  expect(state.reportStats.deleted).toBe(1)
  expect(state.reportStats.modified).toBe(5)
  expect(state.reportStats.unchanged).toBe(1)
  expect(typeof state.rendererReady).toBe('boolean')
})

test('loads a pair via the three file inputs', async ({ page }) => {
  await page.goto('/')
  await page.setInputFiles('[data-testid=input-old]', join(FIXTURES, 'old.ifc'))
  await page.setInputFiles('[data-testid=input-new]', join(FIXTURES, 'new.ifc'))
  await page.setInputFiles('[data-testid=input-report]', join(FIXTURES, 'report.json'))
  const state = await waitReady(page)
  expect(state.bucketEntityCounts).toEqual(EXPECTED_BUCKETS)
})
