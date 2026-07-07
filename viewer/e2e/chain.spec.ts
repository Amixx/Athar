/**
 * Headless chain-navigation smoke: a 2-step manifest (schema 2) built inline
 * from the committed fixture pair, served with per-step urls. Both steps reuse
 * the same two model urls, so a step switch is a pure mesh-cache hit — the test
 * asserts navigation + debug state through `window.__athar_viewer`, not pixels.
 *
 * No fixtures are regenerated; the manifest is synthesized by this server.
 */

import { createServer, type Server } from 'node:http'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { AddressInfo } from 'node:net'
import { expect, test, type Page } from '@playwright/test'

const FIXTURES = fileURLToPath(new URL('./fixtures', import.meta.url))

const CHAIN_MANIFEST = {
  athar_viewer_manifest: 1,
  schema_version: 2,
  generator: 'chain.spec.ts',
  steps: [
    {
      label: 'r1 → r2',
      old: { name: 'r1.ifc', url: '/files/model/0.ifc' },
      new: { name: 'r2.ifc', url: '/files/model/1.ifc' },
      report: { url: '/files/report/0.json' },
    },
    {
      label: 'r2 → r3',
      old: { name: 'r2.ifc', url: '/files/model/0.ifc' },
      new: { name: 'r3.ifc', url: '/files/model/1.ifc' },
      report: { url: '/files/report/1.json' },
    },
  ],
  active_step: 0,
}

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
      if (path === '/manifest.json' || path === '/') {
        res.writeHead(200, { ...cors, 'content-type': 'application/json' })
        res.end(JSON.stringify(CHAIN_MANIFEST))
        return
      }
      if (path.startsWith('/files/model/')) {
        const idx = path.endsWith('/1.ifc') ? 'new.ifc' : 'old.ifc'
        res.writeHead(200, { ...cors, 'content-type': 'application/octet-stream' })
        res.end(await readFile(join(FIXTURES, idx)))
        return
      }
      if (path.startsWith('/files/report/')) {
        res.writeHead(200, { ...cors, 'content-type': 'application/json' })
        res.end(await readFile(join(FIXTURES, 'report.json')))
        return
      }
      res.writeHead(404, cors)
      res.end()
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

async function waitStep(page: Page, step: number): Promise<Record<string, any>> {
  await page.waitForFunction(
    (s) => {
      const state = (window as any).__athar_viewer
      return state?.phase === 'ready' && state?.activeStep === s
    },
    step,
    { timeout: 60_000 },
  )
  return page.evaluate(() => (window as any).__athar_viewer)
}

test('steps through a 2-step chain via buttons and keyboard', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)

  let state = await waitStep(page, 0)
  expect(state.stepCount).toBe(2)
  expect(state.stepLabel).toBe('r1 → r2')
  expect(state.bucketEntityCounts.deleted).toBe(1)
  expect(state.bucketEntityCounts.added).toBe(1)
  const bucketsStep0 = state.bucketEntityCounts
  await expect(page.getByTestId('step-indicator')).toContainText('1/2: r1 → r2')

  // Next via button.
  await page.getByRole('button', { name: 'Next step' }).click()
  state = await waitStep(page, 1)
  expect(state.stepLabel).toBe('r2 → r3')
  // Shared model urls → mesh cache hit; buckets stay identical across the switch.
  expect(state.bucketEntityCounts).toEqual(bucketsStep0)
  await expect(page.getByTestId('step-indicator')).toContainText('2/2: r2 → r3')

  // Previous via the [ key.
  await page.keyboard.press('[')
  state = await waitStep(page, 0)
  expect(state.stepLabel).toBe('r1 → r2')
})
