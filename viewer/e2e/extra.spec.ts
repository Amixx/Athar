/**
 * A mesh with no report record: the fixture pair served with a report that
 * omits WallA, so WallA's new-side mesh lands in the `extra` bucket.
 */

import { createServer, type Server } from 'node:http'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { AddressInfo } from 'node:net'
import { expect, test } from '@playwright/test'

const FIXTURES = fileURLToPath(new URL('./fixtures', import.meta.url))
const WALL_A = 37

async function reportWithoutWallA(): Promise<string> {
  const report = JSON.parse(await readFile(join(FIXTURES, 'report.json'), 'utf8'))
  const before = report.modified.length
  report.modified = report.modified.filter((item: any) => item.new.step_id !== WALL_A)
  if (report.modified.length !== before - 1) throw new Error(`fixture report has no modified #${WALL_A}`)
  report.stats.modified = report.modified.length
  report.stats.modified_change_scope.transitive -= 1
  return JSON.stringify(report)
}

let server: Server
let srcOrigin: string

test.beforeAll(async () => {
  const files: Record<string, string | Buffer> = {
    '/manifest.json': JSON.stringify({
      athar_viewer_manifest: 1,
      schema_version: 2,
      old: { name: 'old.ifc', url: '/old.ifc' },
      new: { name: 'new.ifc', url: '/new.ifc' },
      report: { url: '/report.json' },
    }),
    '/old.ifc': await readFile(join(FIXTURES, 'old.ifc')),
    '/new.ifc': await readFile(join(FIXTURES, 'new.ifc')),
    '/report.json': await reportWithoutWallA(),
  }
  server = createServer((req, res) => {
    const body = files[(req.url ?? '/').split('?')[0]]
    res.writeHead(body === undefined ? 404 : 200, {
      'access-control-allow-origin': '*',
      'cache-control': 'no-store',
    })
    res.end(body)
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  srcOrigin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
})

test.afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()))
})

test('an entity missing from the report is drawn, counted and toggled as extra', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  await page.waitForFunction(() => {
    const phase = (window as any).__athar_viewer?.phase
    return phase === 'ready' || phase === 'error'
  })
  const state = await page.evaluate(() => (window as any).__athar_viewer)
  expect(state.error ?? '').toBe('')
  expect(state.bucketEntityCounts.extra).toBe(1)
  expect(state.bucketEntityCounts.transitive).toBeUndefined()

  await expect(page.getByTestId('legend-extra')).toContainText('1 drawn')
  await page.waitForTimeout(500)
  await page.screenshot({ path: 'test-results/extra.png' })

  const extraVisible = () => page.evaluate(() => (window as any).__athar_viewer.appearances.extra.visible)
  expect(await extraVisible()).toBe(true)
  await page.getByRole('button', { name: 'Extra' }).click()
  expect(await extraVisible()).toBe(false)
  expect(await page.evaluate(() => (window as any).__athar_viewer.appearances.unchanged.visible)).toBe(true)
})
