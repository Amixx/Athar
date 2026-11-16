/**
 * Georeferenced overlay: the fixture pair with its site placed at a large
 * world coordinate. The tessellator subtracts a different RTC offset from each
 * file (the two models have different extents), so the viewer must translate
 * one model into the other's frame. Bounds come from the
 * `window.__athar_viewer.entityBounds` and `drawnBounds` probes, so no pixels
 * are read.
 */

import { createServer, type Server } from 'node:http'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { AddressInfo } from 'node:net'
import { expect, test } from '@playwright/test'

const FIXTURES = fileURLToPath(new URL('./fixtures', import.meta.url))
const SITE_POINT = '#10=IFCCARTESIANPOINT((0.,0.,0.));'
const GEOREFERENCED_SITE_POINT = '#10=IFCCARTESIANPOINT((500000.,6000000.,100.));'

const WALL_A = 37
const WALL_B = 53

async function georeferenced(name: string): Promise<string> {
  const text = await readFile(join(FIXTURES, name), 'utf8')
  if (!text.includes(SITE_POINT)) throw new Error(`${name} has no site point ${SITE_POINT}`)
  return text.replace(SITE_POINT, GEOREFERENCED_SITE_POINT)
}

let server: Server
let srcOrigin: string

test.beforeAll(async () => {
  const files: Record<string, string> = {
    '/manifest.json': JSON.stringify({
      athar_viewer_manifest: 1,
      schema_version: 2,
      old: { name: 'old-geo.ifc', url: '/old.ifc' },
      new: { name: 'new-geo.ifc', url: '/new.ifc' },
      report: { url: '/report.json' },
    }),
    '/old.ifc': await georeferenced('old.ifc'),
    '/new.ifc': await georeferenced('new.ifc'),
    '/report.json': await readFile(join(FIXTURES, 'report.json'), 'utf8'),
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

test('models with different RTC offsets share one frame', async ({ page }) => {
  await page.goto(`/?src=${srcOrigin}`)
  await page.waitForFunction(() => {
    const phase = (window as any).__athar_viewer?.phase
    return phase === 'ready' || phase === 'error'
  })
  await page.waitForTimeout(500)
  await page.screenshot({ path: 'test-results/frame-offset.png' })

  const probe = await page.evaluate(
    ([wallA, wallB]) => {
      const state = (window as any).__athar_viewer
      return {
        error: state.error ?? '',
        translations: state.frameTranslations,
        wallA: { old: state.entityBounds('old', wallA), new: state.entityBounds('new', wallA) },
        wallB: { old: state.entityBounds('old', wallB), new: state.entityBounds('new', wallB) },
        drawn: {
          wallA: state.drawnBounds('new', wallA),
          wallB: { old: state.drawnBounds('old', wallB), new: state.drawnBounds('new', wallB) },
        },
      }
    },
    [WALL_A, WALL_B],
  )
  expect(probe.error).toBe('')
  expect(Math.hypot(probe.translations.new.x, probe.translations.new.y, probe.translations.new.z)).toBeGreaterThan(1)

  for (const corner of ['min', 'max'] as const) {
    for (let axis = 0; axis < 3; axis++) {
      expect(probe.wallA.new[corner][axis]).toBeCloseTo(probe.wallA.old[corner][axis], 3)
    }
  }
  // WallB moved +1 m along IFC x, which is scene x.
  expect(probe.wallB.new.min[0] - probe.wallB.old.min[0]).toBeCloseTo(1, 3)
  expect(probe.wallB.new.min[2]).toBeCloseTo(probe.wallB.old.min[2], 3)

  // The scene draws each mesh where the frame probe puts it.
  const drawnPairs = [
    [probe.drawn.wallA, probe.wallA.new],
    [probe.drawn.wallB.old, probe.wallB.old],
    [probe.drawn.wallB.new, probe.wallB.new],
  ]
  for (const [drawn, expected] of drawnPairs) {
    for (const corner of ['min', 'max'] as const) {
      for (let axis = 0; axis < 3; axis++) expect(drawn[corner][axis]).toBeCloseTo(expected[corner][axis], 3)
    }
  }
})
