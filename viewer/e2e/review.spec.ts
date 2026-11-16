/**
 * Review UI end to end: each test starts the real `athar review` server on a
 * fresh store and drives the built review.html in a browser.
 *
 * ATHAR_REVIEW_EVIDENCE=<dir> records the main flow as a video, slows actions
 * down so it can be followed, and saves screenshots into <dir>.
 */

import { execFileSync, spawn, type ChildProcess } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { expect, test, type Page } from '@playwright/test'

const REPO = fileURLToPath(new URL('../..', import.meta.url))
const PYTHON = join(REPO, '.venv', 'bin', 'python')
const DIST = join(REPO, 'viewer', 'dist')
const EVIDENCE = process.env.ATHAR_REVIEW_EVIDENCE ?? null
const REVIEWER = 'Dana Fischer'

interface Running {
  url: string
  dir: string
  process: ChildProcess
  oldIfc: string
  newIfc: string
}

let fixtures: { dir: string; oldIfc: string; newIfc: string }

test.beforeAll(() => {
  if (!existsSync(PYTHON)) throw new Error(`no repo virtualenv at ${PYTHON}; run the setup in the root README`)
  if (!existsSync(join(DIST, 'review.html'))) throw new Error(`no built UI at ${DIST}; run bun run build`)
  const dir = mkdtempSync(join(tmpdir(), 'athar-review-e2e-'))
  const oldIfc = join(dir, 'riverside-arch-rev-A.ifc')
  const newIfc = join(dir, 'riverside-arch-rev-B.ifc')
  execFileSync(
    PYTHON,
    [
      '-c',
      'import sys; from tests.test_review_ui import write_review_demo_pair; write_review_demo_pair(*sys.argv[1:])',
      oldIfc,
      newIfc,
    ],
    { cwd: REPO, stdio: 'inherit' },
  )
  fixtures = { dir, oldIfc, newIfc }
})

test.afterAll(() => rmSync(fixtures.dir, { recursive: true, force: true }))

async function startServer(): Promise<Running> {
  const dir = mkdtempSync(join(tmpdir(), 'athar-review-store-'))
  const child = spawn(PYTHON, ['-m', 'athar', 'review', '--root', dir, '--no-open', '--dist', DIST], {
    cwd: REPO,
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    let buffered = ''
    const timer = setTimeout(() => reject(new Error('athar review did not print its URL')), 30_000)
    child.stdout!.on('data', (chunk: Buffer) => {
      buffered += chunk.toString()
      const match = /Athar Review: (\S+)/.exec(buffered)
      if (match) {
        clearTimeout(timer)
        resolve(match[1].replace(/\/$/, ''))
      }
    })
    child.on('exit', (code) => reject(new Error(`athar review exited with ${code}`)))
  })
  return { url, dir, process: child, oldIfc: fixtures.oldIfc, newIfc: fixtures.newIfc }
}

function stopServer(server: Running): void {
  server.process.kill()
  rmSync(server.dir, { recursive: true, force: true })
}

async function seedUpload(server: Running, kind: 'baseline' | 'review', path: string, actor: string, name: string) {
  const response = await fetch(
    `${server.url}/api/projects/riverside/models/architecture/${kind}?actor=${encodeURIComponent(actor)}&filename=${encodeURIComponent(name)}`,
    { method: 'POST', body: readFileSync(path), headers: { 'Content-Type': 'application/x-step' } },
  )
  expect(response.status).toBe(200)
  return (await response.json()) as { id: string }
}

async function seedDecision(server: Running, action: 'approve' | 'reject', reviewId: string, actor: string) {
  const response = await fetch(`${server.url}/api/projects/riverside/models/architecture/${action}`, {
    method: 'POST',
    body: JSON.stringify({ review_id: reviewId, actor }),
    headers: { 'Content-Type': 'application/json' },
  })
  expect(response.status).toBe(200)
}

async function nameReviewer(page: Page): Promise<void> {
  await page.addInitScript((name) => sessionStorage.setItem('athar-review-actor', name), REVIEWER)
}

async function snap(page: Page, name: string): Promise<void> {
  if (EVIDENCE) await page.screenshot({ path: join(EVIDENCE, name), fullPage: true })
}

function reviewUrl(server: Running, reviewId: string): string {
  return `${server.url}/#/p/riverside/m/architecture/r/${reviewId}`
}

let server: Running

test.beforeEach(async () => {
  server = await startServer()
})

test.afterEach(() => stopServer(server))

test.use({
  viewport: { width: 1280, height: 860 },
  video: EVIDENCE ? { mode: 'on', size: { width: 1280, height: 860 } } : 'off',
  launchOptions: { slowMo: EVIDENCE ? 120 : 0 },
})

test.describe('main flow', () => {
  test('set a baseline, review a delivery, read the changes, approve', async ({ page }) => {
    if (EVIDENCE) mkdirSync(EVIDENCE, { recursive: true })
    await page.goto(server.url)

    const nameDialog = page.getByRole('dialog', { name: 'Who is reviewing?' })
    await expect(nameDialog).toBeVisible()
    await nameDialog.getByLabel('Reviewer name').fill(REVIEWER)
    await nameDialog.getByRole('button', { name: 'Continue' }).click()
    await expect(page.getByTestId('reviewer-name')).toHaveText(REVIEWER)

    await page.getByRole('link', { name: 'Set a first baseline' }).click()
    await page.getByRole('textbox', { name: 'Project', exact: true }).fill('riverside')
    await page.getByRole('textbox', { name: 'Model', exact: true }).fill('architecture')
    await page.locator('input[type=file]').setInputFiles(server.oldIfc)
    await expect(page.getByText('Current baseline of riverside, architecture')).toBeVisible()

    await page.locator('input[type=file]').setInputFiles(server.newIfc)
    const title = page.getByRole('heading', { level: 2, name: 'riverside-arch-rev-B.ifc' })
    await expect(title).toBeVisible({ timeout: 60_000 })

    const report = (await (
      await page.request.get(`${server.url}/api/projects/riverside/reviews/evt-000002`)
    ).json()) as { stats: { added: number; deleted: number; modified: number } }
    const chips = page.locator('header .chips')
    await expect(chips.getByTitle('deleted elements')).toHaveText(`−${report.stats.deleted}`)
    await expect(chips.getByTitle('added elements')).toHaveText(`+${report.stats.added}`)
    await expect(chips.getByTitle('modified elements')).toHaveText(`~${report.stats.modified}`)

    const provenance = page.locator('dl.provenance')
    await expect(provenance).toContainText(REVIEWER)
    await expect(provenance).toContainText(/Athar \d+\.\d+\.\d+, canon \S+/)
    await expect(provenance).toContainText(/Delivery sha256\s*[0-9a-f]{12}\b/)
    await expect(provenance.getByRole('button', { name: 'Copy the full sha256 of the delivery' })).toBeVisible()

    const groups = page.locator('section.group h3')
    await expect(groups.nth(1)).toContainText(/IfcDoor\s*1 deleted/)
    await expect(groups.nth(2)).toContainText(/IfcWall\s*1 added/)
    await expect(groups.first()).toContainText('Bulk moves')

    const headHeight = page.getByRole('row', { name: /Pset_DoorCommon, HeadHeight/ })
    await expect(headHeight).toContainText('2.1')
    await expect(headHeight).toContainText('2.4')
    await expect(page.getByRole('row', { name: /Pset_DoorCommon, AcousticRating/ })).toContainText('Rw 32 dB')
    for (const path of await page.getByText('IfcPropertySingleValue', { exact: false }).all()) {
      await expect(path).toBeHidden()
    }
    await headHeight.scrollIntoViewIfNeeded()
    await snap(page, 'review-detail-property-changes.png')

    const cohort = page.getByRole('button', { name: '40 elements moved by 1.2, -0.35, 0.0 m' })
    await expect(cohort).toHaveAttribute('aria-expanded', 'false')
    await cohort.click()
    await expect(cohort).toHaveAttribute('aria-expanded', 'true')
    await expect(page.locator('ul.members li.member')).toHaveCount(40)
    await cohort.scrollIntoViewIfNeeded()
    await snap(page, 'bulk-move-cohort.png')

    expect(await page.locator('body').innerText()).not.toMatch(/[\u00B7\u2219\u22C5]/)

    await page.getByRole('link', { name: /Back to the register/ }).click()
    await expect(page.getByRole('table', { name: 'Review register' })).toContainText('awaiting decision')
    await snap(page, 'register.png')
    await page.getByRole('link', { name: 'Delivery reviewed' }).click()

    await page.getByRole('button', { name: 'Accept as new baseline' }).click()
    const confirm = page.getByRole('dialog', { name: 'Accept this delivery as the new baseline?' })
    await expect(confirm).toContainText('riverside-arch-rev-B.ifc')
    await expect(confirm.getByRole('button', { name: 'Cancel' })).toBeFocused()
    await confirm.getByRole('button', { name: 'Accept riverside-arch-rev-B.ifc' }).click()
    await expect(page.locator('.banner.decided')).toContainText(`Accepted as the baseline by ${REVIEWER}`)
    await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toHaveCount(0)
    if (EVIDENCE) await page.waitForTimeout(1500)

    const video = page.video()
    await page.close()
    if (EVIDENCE && video) copyFileSync(await video.path(), join(EVIDENCE, 'review-main-flow.webm'))
  })
})

test('property rows retain every raw data change in a collapsed block', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await nameReviewer(page)
  await page.route(`**/api/projects/riverside/reviews/${review.id}`, async (route) => {
    const response = await route.fetch()
    const report = await response.json()
    report.stats.placement_cohorts = []
    report.added = []
    report.deleted = []
    report.modified = [{
      old: { class: 'IfcDoor', step_id: 1, name: 'D-101' },
      new: { class: 'IfcDoor', step_id: 2, name: 'D-101' },
      aspects: { data: 'changed' },
      property_deltas: { changed: [{ pset: 'Dimensions', name: 'Height', old_value: 2.1, new_value: 2.4 }] },
      data_delta: [
        { path: 'IfcPropertySingleValue.NominalValue[1]', old: 'IfcLengthMeasure: 12', new: 'IfcReal: 12' },
        { path: 'IfcDoor.OverallHeight', old: '2100', new: '2400' },
      ],
    }]
    await route.fulfill({ response, json: report })
  })
  await page.goto(reviewUrl(server, review.id))
  await expect(page.getByRole('row', { name: 'Dimensions, Height 2.1 to 2.4' })).toBeVisible()
  const raw = page.locator('details.raw')
  await expect(raw.locator('summary')).toHaveText('Data changes (2)')
  await expect(raw).not.toHaveAttribute('open')
  await raw.locator('summary').click()
  await expect(raw.getByRole('row')).toHaveCount(2)
  await expect(raw).toContainText('IfcPropertySingleValue.NominalValue[1]')
  await expect(raw).toContainText('IfcLengthMeasure: 12')
  await expect(raw).toContainText('IfcReal: 12')
  await snap(page, 'review-raw-data-expanded.png')
})

test('decisions wait for a delayed change report', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await nameReviewer(page)
  let release!: () => void
  const held = new Promise<void>((resolve) => { release = resolve })
  await page.route(`**/api/projects/riverside/reviews/${review.id}`, async (route) => {
    await held
    await route.continue()
  })
  try {
    await page.goto(reviewUrl(server, review.id))
    await expect(page.getByRole('status')).toHaveText('Loading the change report…')
    await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeDisabled()
    await expect(page.getByRole('button', { name: 'Reject', exact: true })).toBeDisabled()
  } finally {
    release()
  }
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()
  await expect(page.getByRole('button', { name: 'Reject', exact: true })).toBeEnabled()
})

for (const code of ['internal_error', 'report_not_stored']) {
  test(`decision availability after report error ${code}`, async ({ page }) => {
    await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
    const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
    await nameReviewer(page)
    await page.route(`**/api/projects/riverside/reviews/${review.id}`, (route) => route.fulfill({
      status: code === 'report_not_stored' ? 404 : 500,
      json: { error: { code, message: 'Report unavailable' } },
    }))
    await page.goto(reviewUrl(server, review.id))
    if (code === 'report_not_stored') {
      await expect(page.locator('.report-missing')).toContainText('only the summary counts above exist.')
      await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()
      await expect(page.getByRole('button', { name: 'Reject', exact: true })).toBeEnabled()
    } else {
      await expect(page.getByRole('alert')).toContainText('The change report could not be loaded')
      await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeDisabled()
      await expect(page.getByRole('button', { name: 'Reject', exact: true })).toBeDisabled()
      await page.unroute(`**/api/projects/riverside/reviews/${review.id}`)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()
      await expect(page.getByRole('button', { name: 'Reject', exact: true })).toBeEnabled()
    }
  })
}

test('a refused decision shows the reason and the refreshed state', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'riverside-arch-rev-A.ifc')
  const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'riverside-arch-rev-B.ifc')
  await nameReviewer(page)
  await page.goto(reviewUrl(server, review.id))
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()

  await seedDecision(server, 'reject', review.id, 'carol')
  await page.getByRole('button', { name: 'Accept as new baseline' }).click()
  await page.getByRole('button', { name: 'Accept riverside-arch-rev-B.ifc' }).click()

  const alert = page.locator('.banner.refused')
  await expect(alert).toContainText('The decision was not recorded.')
  await expect(alert).toContainText('This review was already rejected.')
  await expect(page.locator('.banner.decided')).toContainText('Rejected by carol')
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toHaveCount(0)
  await alert.scrollIntoViewIfNeeded()
  if (EVIDENCE) await page.screenshot({ path: join(EVIDENCE, 'refused-decision.png') })
})

test('an existing baseline refusal uses reviewer wording', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  await nameReviewer(page)
  await page.goto(`${server.url}/#/new`)
  await page.getByRole('textbox', { name: 'Project', exact: true }).fill('riverside')
  await page.getByRole('textbox', { name: 'Model', exact: true }).fill('architecture')
  await page.locator('input[type=file]').setInputFiles(server.oldIfc)
  await expect(page.getByRole('alert')).toContainText('That lineage already has a baseline.')
  await expect(page.getByRole('alert')).not.toContainText('replace=True')
})

test('a superseded decision refusal uses reviewer wording', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const first = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await nameReviewer(page)
  await page.goto(reviewUrl(server, first.id))
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()
  const second = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B-again.ifc')
  await seedDecision(server, 'approve', second.id, 'carol')
  await page.getByRole('button', { name: 'Accept as new baseline' }).click()
  await page.getByRole('button', { name: 'Accept rev-B.ifc', exact: true }).click()
  await expect(page.locator('.banner.refused')).toContainText('This review was superseded')
  await expect(page.locator('.banner.stale')).toContainText('Superseded')
  expect(await page.locator('body').innerText()).not.toMatch(/\bstale\b|replace=True/i)
})

test('a superseded review explains why it cannot be decided', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const first = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  const second = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B-again.ifc')
  await seedDecision(server, 'approve', second.id, 'carol')
  await nameReviewer(page)
  await page.goto(reviewUrl(server, first.id))
  await expect(page.locator('.banner.stale')).toContainText('Superseded')
  await expect(page.locator('.banner.stale')).toContainText(`changed in evt-000004`)
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toHaveCount(0)
  await page.getByRole('link', { name: 'the register', exact: true }).click()
  await expect(page.getByRole('table', { name: 'Review register' })).toContainText('superseded')
})

test('policy violations with the same code remain distinct after history refresh', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await nameReviewer(page)
  await page.route('**/api/projects/riverside/events*', async (route) => {
    const response = await route.fetch()
    const body = await response.json()
    const event = body.events.find((entry: { id: string }) => entry.id === review.id)
    event.has_policy = true
    event.verdict = 'fail'
    event.check = { ok: false, violations: [
      { code: 'aspect_change_forbidden', message: 'Shape changes are forbidden.' },
      { code: 'aspect_change_forbidden', message: 'Position changes are forbidden.' },
    ] }
    await route.fulfill({ response, json: body })
  })
  await page.goto(reviewUrl(server, review.id))
  const violations = page.locator('.policy-banner > div')
  await expect(violations).toHaveText([
    'aspect_change_forbidden: Shape changes are forbidden.',
    'aspect_change_forbidden: Position changes are forbidden.',
  ])
  await page.getByRole('button', { name: 'Reject', exact: true }).click()
  await page.getByRole('button', { name: 'Reject rev-B.ifc', exact: true }).click()
  await expect(page.locator('.banner.decided')).toContainText('Rejected')
  await expect(violations).toHaveText([
    'aspect_change_forbidden: Shape changes are forbidden.',
    'aspect_change_forbidden: Position changes are forbidden.',
  ])
  await snap(page, 'review-policy-violations.png')
})

test('overlapping pending reviews point at each other', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const first = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  const second = await seedUpload(server, 'review', server.newIfc, 'carol', 'rev-B-carol.ifc')
  await nameReviewer(page)
  await page.goto(reviewUrl(server, second.id))
  const banner = page.locator('.conflict-banner')
  await expect(banner).toContainText('Both await a decision; accepting one supersedes the other.')
  await banner.getByRole('link', { name: first.id }).click()
  await expect(page.getByRole('heading', { level: 2, name: 'rev-B.ifc' })).toBeVisible()
})

test('without a reviewer name nothing can be uploaded or decided', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const review = await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await page.goto(`${server.url}/#/p/riverside/m/architecture`)
  const nameDialog = page.getByRole('dialog', { name: 'Who is reviewing?' })
  await nameDialog.getByRole('button', { name: 'Browse without a name' }).click()
  await expect(nameDialog).toBeHidden()

  const zone = page.getByRole('button', { name: 'Review a new delivery against this baseline' })
  await expect(zone).toHaveAttribute('aria-disabled', 'true')
  await expect(zone).toContainText('Set your reviewer name')

  await page.reload()
  await expect(nameDialog).toBeHidden()
  await page.goto(reviewUrl(server, review.id))
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeDisabled()
  await expect(page.getByRole('button', { name: 'Reject' })).toBeDisabled()

  await page.getByRole('button', { name: 'Set name' }).first().click()
  await nameDialog.getByLabel('Reviewer name').fill(REVIEWER)
  await nameDialog.getByLabel('Reviewer name').press('Enter')
  await expect(page.getByRole('button', { name: 'Accept as new baseline' })).toBeEnabled()
})

test('an unreadable delivery is refused with the engine message', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  const broken = join(fixtures.dir, 'broken-export.ifc')
  writeFileSync(broken, 'ISO-10303-21;\nthis is not a model\n')
  await nameReviewer(page)
  await page.goto(`${server.url}/#/p/riverside/m/architecture`)
  await page.locator('input[type=file]').setInputFiles(broken)
  await expect(page.getByRole('alert')).toContainText('Athar could not compare broken-export.ifc')
  await expect(page.getByRole('button', { name: 'Review a new delivery against this baseline' })).toHaveAttribute(
    'aria-disabled',
    'false',
  )
})

test('upload, review list and decision are operable by keyboard', async ({ page }) => {
  await seedUpload(server, 'baseline', server.oldIfc, 'alice', 'rev-A.ifc')
  await seedUpload(server, 'review', server.newIfc, 'bob', 'rev-B.ifc')
  await nameReviewer(page)
  await page.goto(`${server.url}/#/p/riverside/m/architecture`)

  const zone = page.getByRole('button', { name: 'Review a new delivery against this baseline' })
  await zone.focus()
  const chooser = page.waitForEvent('filechooser')
  await page.keyboard.press('Enter')
  await chooser

  const link = page.getByRole('link', { name: 'Delivery reviewed' })
  await link.focus()
  await page.keyboard.press('Enter')
  await expect(page.getByRole('heading', { level: 2, name: 'rev-B.ifc' })).toBeVisible()

  await page.getByRole('button', { name: 'Reject' }).focus()
  await page.keyboard.press('Enter')
  const confirm = page.getByRole('dialog', { name: 'Reject this delivery?' })
  await expect(confirm).toContainText('rev-B.ifc')
  await expect(confirm.getByRole('button', { name: 'Cancel' })).toBeFocused()
  await page.keyboard.press('Escape')
  await expect(confirm).toBeHidden()
  await page.getByRole('button', { name: 'Reject' }).focus()
  await page.keyboard.press('Enter')
  await page.keyboard.press('Tab')
  await expect(confirm.getByRole('button', { name: 'Reject rev-B.ifc' })).toBeFocused()
  await page.keyboard.press('Enter')
  await expect(page.locator('.banner.decided')).toContainText(`Rejected by ${REVIEWER}`)
})

test('an empty store offers to start a lineage', async ({ page }) => {
  await nameReviewer(page)
  await page.goto(server.url)
  await expect(page.getByRole('heading', { name: 'Nothing under review yet' })).toBeVisible()
  await page.getByRole('link', { name: 'Set a first baseline' }).click()
  const zone = page.getByRole('button', { name: 'Drop the first accepted IFC file to set the baseline' })
  await expect(zone).toContainText('Name the project')
  await page.getByRole('textbox', { name: 'Project', exact: true }).fill('riverside')
  await page.getByRole('textbox', { name: 'Model', exact: true }).fill('architecture')
  await expect(zone).toHaveAttribute('aria-disabled', 'false')
})
