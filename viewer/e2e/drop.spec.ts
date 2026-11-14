/**
 * Dropped files: old and new come from the report's `audit.inputs` hashes,
 * never from file names. The committed fixture report has no audit block, so
 * the verified case adds one computed from the fixture bytes.
 */

import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
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

let oldBytes: Buffer
let newBytes: Buffer
let reportJson: Record<string, unknown>

test.beforeAll(async () => {
  oldBytes = await readFile(join(FIXTURES, 'old.ifc'))
  newBytes = await readFile(join(FIXTURES, 'new.ifc'))
  reportJson = JSON.parse(await readFile(join(FIXTURES, 'report.json'), 'utf8'))
})

const sha256 = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex')
const ifc = (name: string, buffer: Buffer) => ({ name, mimeType: 'application/octet-stream', buffer })
const json = (name: string, value: unknown) => ({
  name,
  mimeType: 'application/json',
  buffer: Buffer.from(JSON.stringify(value)),
})

async function waitPhase(page: Page, phase: string): Promise<Record<string, any>> {
  await page.waitForFunction(
    (wanted) => {
      const current = (window as any).__athar_viewer?.phase
      return current === wanted || current === 'error'
    },
    phase,
    { timeout: 60_000 },
  )
  const state = await page.evaluate(() => (window as any).__athar_viewer)
  expect(state.error ?? '').toBe('')
  expect(state.phase).toBe(phase)
  return state
}

test('assigns old and new by the report hashes, not by name', async ({ page }) => {
  const audited = {
    ...reportJson,
    audit: { inputs: { old: { sha256: sha256(oldBytes) }, new: { sha256: sha256(newBytes) } } },
  }
  await page.goto('/')
  // "draft" sorts before "final", but final.ifc holds the old revision.
  await page.setInputFiles('[data-testid=input-ifc]', [ifc('draft.ifc', newBytes), ifc('final.ifc', oldBytes)])
  await page.setInputFiles('[data-testid=input-report]', json('report.json', audited))

  const state = await waitPhase(page, 'ready')
  expect(state.bucketEntityCounts).toEqual(EXPECTED_BUCKETS)
  await expect(page.locator('.filechip')).toContainText(/old\s*final\.ifc\s*new\s*draft\.ifc/)
})

test('start over loads changed contents under the same file names', async ({ page }) => {
  const audited = {
    ...reportJson,
    audit: { inputs: { old: { sha256: sha256(oldBytes) }, new: { sha256: sha256(newBytes) } } },
  }
  await page.goto('/')
  await page.setInputFiles('[data-testid=input-ifc]', [ifc('old.ifc', oldBytes), ifc('new.ifc', newBytes)])
  await page.setInputFiles('[data-testid=input-report]', json('report.json', audited))
  await waitPhase(page, 'ready')
  const before = await page.evaluate(() => (window as any).__athar_viewer.entityBounds('new', 53))
  await page.getByRole('button', { name: 'Start over', exact: true }).click()
  await expect(page.locator('h1')).toContainText('semantic IFC diff viewer')
  const changed = Buffer.from(newBytes.toString().replace('#10=IFCCARTESIANPOINT((0.,0.,0.));', '#10=IFCCARTESIANPOINT((10.,0.,0.));'))
  const updated = structuredClone(audited) as any
  updated.audit.inputs.new.sha256 = sha256(changed)
  updated.unchanged[0].new.name = 'Site v2'
  await page.setInputFiles('[data-testid=input-ifc]', [ifc('old.ifc', oldBytes), ifc('new.ifc', changed)])
  await page.setInputFiles('[data-testid=input-report]', json('report.json', updated))
  await waitPhase(page, 'ready')
  const after = await page.evaluate(() => (window as any).__athar_viewer.entityBounds('new', 53))
  expect(after.min[0] - before.min[0]).toBeCloseTo(10, 3)
  await expect(page.getByTestId('undrawn')).toContainText('Site v2')
})

async function canvasShot(page: Page): Promise<Buffer> {
  await page.evaluate(() => document.querySelectorAll<HTMLElement>('.center-stage').forEach((el) => (el.style.visibility = 'hidden')))
  const shot = await page.locator('canvas').screenshot()
  await page.evaluate(() => document.querySelectorAll<HTMLElement>('.center-stage').forEach((el) => (el.style.visibility = '')))
  return shot
}

test('start over leaves no model pixels on the canvas', async ({ page }) => {
  const audited = {
    ...reportJson,
    audit: { inputs: { old: { sha256: sha256(oldBytes) }, new: { sha256: sha256(newBytes) } } },
  }
  await page.goto('/')
  const blank = await canvasShot(page)
  await page.setInputFiles('[data-testid=input-ifc]', [ifc('old.ifc', oldBytes), ifc('new.ifc', newBytes)])
  await page.setInputFiles('[data-testid=input-report]', json('report.json', audited))
  const state = await waitPhase(page, 'ready')
  expect(state.rendererReady).toBe(true)
  await page.waitForTimeout(300)
  expect((await page.locator('canvas').screenshot()).equals(blank)).toBe(false)

  await page.getByRole('button', { name: 'Start over', exact: true }).click()
  await expect(page.locator('h1')).toContainText('semantic IFC diff viewer')
  await page.waitForTimeout(300)
  expect((await canvasShot(page)).equals(blank)).toBe(true)
})

test('asks for the roles when the report cannot verify the files', async ({ page }) => {
  await page.goto('/')
  await page.setInputFiles('[data-testid=input-ifc]', [ifc('b.ifc', oldBytes), ifc('a.ifc', newBytes)])
  await page.setInputFiles('[data-testid=input-report]', json('report.json', reportJson))

  await waitPhase(page, 'confirm')
  await expect(page.getByTestId('unverified-warning')).toContainText('could not be verified')
  const start = page.getByRole('button', { name: 'Start diff' })
  await expect(start).toBeDisabled()

  await page.getByRole('radio', { name: 'b.ifc is old' }).check()
  await expect(page.getByRole('radio', { name: 'a.ifc is new' })).toBeChecked()
  await start.click()

  const state = await waitPhase(page, 'ready')
  expect(state.bucketEntityCounts).toEqual(EXPECTED_BUCKETS)
  await expect(page.locator('.filechip')).toContainText(/old\s*b\.ifc\s*new\s*a\.ifc/)
})
