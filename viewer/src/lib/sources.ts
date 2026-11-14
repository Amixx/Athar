/**
 * Input acquisition: drag-drop/file-picker classification and the
 * `?src=<origin>` manifest fetch used by the `athar view` CLI hand-off.
 * Manifest file urls resolve under the src path (see `dataUrl`).
 *
 * Manifest contract (served by athar_view at <src>/manifest.json, so `src`
 * is the url of the directory that contains manifest.json, not of the file):
 *
 * Schema 1 — single pair:
 * {
 *   "athar_viewer_manifest": 1,
 *   "schema_version": 1,
 *   "old":    {"name": "old.ifc", "url": "/files/old.ifc"},
 *   "new":    {"name": "new.ifc", "url": "/files/new.ifc"},
 *   "report": {"url": "/files/report.json"}
 * }
 *
 * Schema 2 — revision chain (a schema-1 manifest is one step):
 * {
 *   "athar_viewer_manifest": 1,
 *   "schema_version": 2,
 *   "steps": [
 *     {"label": "r5 → r6",
 *      "old": {...}, "new": {...}, "report": {...}}, ...],
 *   "active_step": 0
 * }
 *
 * A file url is absolute or resolves under the src path; a protocol-relative
 * url (`//host/...`) makes the manifest invalid.
 *
 * Adjacent steps share a file (rN is new-side of one step, old-side of the
 * next); the server gives that file one stable url, so a per-url mesh cache in
 * App.svelte tessellates each revision once.
 */

import { parseReport } from './report'
import type { AtharReport } from './types'

export const MANIFEST_SCHEMA_VERSION = 2

interface ManifestFile {
  name?: string
  url: string
}

interface ParsedStep {
  label: string
  old: ManifestFile
  new: ManifestFile
  report: ManifestFile
}

export interface ParsedManifest {
  steps: ParsedStep[]
  activeStep: number
  /** Non-fatal handshake warning (e.g. CLI newer than the viewer). */
  warning?: string
}

/**
 * One diff step ready to load: names for display, keys for the mesh cache, and
 * byte/report resolvers that are only invoked when the step becomes active.
 */
export interface StepSource {
  label: string
  oldName: string
  newName: string
  oldKey: string
  newKey: string
  reportKey: string
  fetchOldBytes: () => Promise<ArrayBuffer>
  fetchNewBytes: () => Promise<ArrayBuffer>
  fetchReport: () => Promise<AtharReport>
}

export interface LoadedChain {
  steps: StepSource[]
  activeStep: number
  warning?: string
}

class ManifestFormatError extends Error {}

function parseFile(value: unknown, ctx: string): ManifestFile {
  if (typeof value !== 'object' || value === null || typeof (value as ManifestFile).url !== 'string') {
    throw new ManifestFormatError(`manifest ${ctx} is missing a url.`)
  }
  const file = value as Record<string, unknown>
  if ((file.url as string).startsWith('//')) {
    throw new ManifestFormatError(`manifest ${ctx} url ${JSON.stringify(file.url)} is protocol-relative.`)
  }
  return { url: file.url as string, name: typeof file.name === 'string' ? file.name : undefined }
}

function parseStep(value: unknown, i: number): ParsedStep {
  if (typeof value !== 'object' || value === null) {
    throw new ManifestFormatError(`manifest step ${i} is not an object.`)
  }
  const step = value as Record<string, unknown>
  return {
    label: typeof step.label === 'string' ? step.label : `step ${i + 1}`,
    old: parseFile(step.old, `step ${i} old`),
    new: parseFile(step.new, `step ${i} new`),
    report: parseFile(step.report, `step ${i} report`),
  }
}

export function parseManifest(json: unknown): ParsedManifest {
  if (typeof json !== 'object' || json === null) {
    throw new ManifestFormatError('manifest is not a JSON object.')
  }
  const m = json as Record<string, unknown>
  if (m.athar_viewer_manifest !== 1) {
    throw new ManifestFormatError('not an Athar viewer manifest.')
  }
  let warning: string | undefined
  if (typeof m.schema_version === 'number' && m.schema_version > MANIFEST_SCHEMA_VERSION) {
    warning =
      `CLI manifest schema v${m.schema_version} > viewer v${MANIFEST_SCHEMA_VERSION}; ` +
      `update the viewer if something looks off.`
  }

  let steps: ParsedStep[]
  if ('steps' in m && m.steps !== undefined) {
    if (!Array.isArray(m.steps) || m.steps.length === 0) {
      throw new ManifestFormatError('manifest chain has no steps.')
    }
    steps = m.steps.map((step, i) => parseStep(step, i))
  } else {
    steps = [parseStep({ old: m.old, new: m.new, report: m.report }, 0)]
  }

  let activeStep = 0
  if (m.active_step !== undefined) {
    const step = m.active_step
    if (typeof step !== 'number' || !Number.isInteger(step) || step < 0 || step >= steps.length) {
      throw new ManifestFormatError(
        `manifest active_step ${JSON.stringify(step)} is not a step index from 0 to ${steps.length - 1}.`,
      )
    }
    activeStep = step
  }
  return { steps, activeStep, warning }
}

async function fetchOk(url: string): Promise<Response> {
  let response: Response
  try {
    response = await fetch(url, { cache: 'no-store' })
  } catch (error) {
    throw new Error(`Could not reach ${url} — is the \`athar view\` server still running? (${error})`)
  }
  if (!response.ok) {
    throw new Error(`${url} responded with ${response.status} ${response.statusText}`)
  }
  return response
}

/**
 * URL of a file the `src` server serves. `path` resolves under the src path,
 * a leading slash included. A URL on the src origin carries the src's query
 * parameters, plus the page's (except `src`) when the page itself is served
 * from the src origin; a URL on another origin carries neither.
 */
export function dataUrl(pageUrl: string, src: string, path: string): string {
  const base = new URL(src, pageUrl)
  if (!base.pathname.endsWith('/')) base.pathname += '/'
  const url = new URL(path.replace(/^\/+/, ''), base)
  if (url.origin !== base.origin) return url.toString()
  const page = new URL(pageUrl)
  const carried = new URLSearchParams(page.origin === base.origin ? page.search : '')
  carried.delete('src')
  for (const [key, value] of base.searchParams) carried.set(key, value)
  for (const [key, value] of carried) {
    if (!url.searchParams.has(key)) url.searchParams.set(key, value)
  }
  return url.toString()
}

export async function loadChainFromSrc(src: string, pageUrl: string): Promise<LoadedChain> {
  const manifestUrl = dataUrl(pageUrl, src, 'manifest.json')
  const json = await (await fetchOk(manifestUrl)).json()
  let parsed: ParsedManifest
  try {
    parsed = parseManifest(json)
  } catch (error) {
    throw new Error(`${manifestUrl} is invalid: ${error instanceof Error ? error.message : error}`)
  }
  const resolve = (file: ManifestFile) => dataUrl(pageUrl, src, file.url)
  const steps: StepSource[] = parsed.steps.map((step) => {
    const oldUrl = resolve(step.old)
    const newUrl = resolve(step.new)
    const reportUrl = resolve(step.report)
    return {
      label: step.label,
      oldName: step.old.name ?? 'old.ifc',
      newName: step.new.name ?? 'new.ifc',
      oldKey: oldUrl,
      newKey: newUrl,
      reportKey: reportUrl,
      fetchOldBytes: () => fetchOk(oldUrl).then((r) => r.arrayBuffer()),
      fetchNewBytes: () => fetchOk(newUrl).then((r) => r.arrayBuffer()),
      fetchReport: () => fetchOk(reportUrl).then((r) => r.json()).then(parseReport),
    }
  })
  return { steps, activeStep: parsed.activeStep, warning: parsed.warning }
}

export interface DroppedFiles {
  /** Dropped or picked IFC files, most recent last, at most two. */
  ifcs: File[]
  reportFile?: File
}

export const NO_DROPS: DroppedFiles = { ifcs: [] }

/** Add dropped files: `.json` replaces the report, `.ifc` files join the last two kept. */
export function addDroppedFiles(files: File[], current: DroppedFiles): DroppedFiles {
  const ifcs = files.filter((f) => f.name.toLowerCase().endsWith('.ifc'))
  const json = files.find((f) => f.name.toLowerCase().endsWith('.json'))
  return {
    ifcs: [...current.ifcs, ...ifcs].slice(-2),
    reportFile: json ?? current.reportFile,
  }
}

export interface HashedFile {
  file: File
  sha256: string
}

export async function sha256Hex(bytes: ArrayBuffer): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))
  return Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join('')
}

export async function hashFile(file: File): Promise<HashedFile> {
  return { file, sha256: await sha256Hex(await file.arrayBuffer()) }
}

/** The input file hashes a report names in `audit.inputs`, or null when it names none. */
export function reportInputHashes(report: unknown): { old: string; new: string } | null {
  const inputs = (report as { audit?: { inputs?: Record<string, { sha256?: unknown }> } })?.audit?.inputs
  const oldHash = inputs?.old?.sha256
  const newHash = inputs?.new?.sha256
  if (typeof oldHash !== 'string' || typeof newHash !== 'string') return null
  return { old: oldHash.toLowerCase(), new: newHash.toLowerCase() }
}

export type RoleAssignment<T> =
  | { verified: true; old: T; new: T }
  | { verified: false; reason: string }

/**
 * Old/new roles for two dropped IFC files, decided only by the report's input
 * hashes. Anything short of an exact match is unverified; there is no guess.
 */
export function assignRoles<T extends { file: { name: string }; sha256: string }>(
  files: [T, T],
  inputs: { old: string; new: string } | null,
): RoleAssignment<T> {
  if (!inputs) {
    return { verified: false, reason: 'The report does not record the hashes of the files it compared.' }
  }
  const [a, b] = files
  if (a.sha256 === inputs.old && b.sha256 === inputs.new) return { verified: true, old: a, new: b }
  if (b.sha256 === inputs.old && a.sha256 === inputs.new) return { verified: true, old: b, new: a }
  const unknown = files.filter((f) => f.sha256 !== inputs.old && f.sha256 !== inputs.new)
  if (unknown.length > 0) {
    const names = unknown.map((f) => f.file.name).join(' and ')
    return { verified: false, reason: `${names} ${unknown.length === 1 ? 'is' : 'are'} not one of the files the report compared.` }
  }
  return { verified: false, reason: `${a.file.name} and ${b.file.name} both match the same input of the report.` }
}

export function chainFromFiles(oldFile: HashedFile, newFile: HashedFile, report: AtharReport): LoadedChain {
  const step: StepSource = {
    label: `${oldFile.file.name} → ${newFile.file.name}`,
    oldName: oldFile.file.name,
    newName: newFile.file.name,
    oldKey: 'dropped:old',
    newKey: 'dropped:new',
    reportKey: 'dropped:report',
    fetchOldBytes: () => oldFile.file.arrayBuffer(),
    fetchNewBytes: () => newFile.file.arrayBuffer(),
    fetchReport: () => Promise.resolve(report),
  }
  return { steps: [step], activeStep: 0 }
}

/** Read and validate a dropped report file, failing with a message that names it. */
export async function readReportFile(file: File): Promise<AtharReport> {
  let json: unknown
  try {
    json = JSON.parse(await file.text())
  } catch {
    throw new Error(`${file.name} is not valid JSON.`)
  }
  return parseReport(json)
}
