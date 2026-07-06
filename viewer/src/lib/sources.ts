/**
 * Input acquisition: drag-drop/file-picker classification and the
 * `?src=<origin>` manifest fetch used by the `athar view` CLI hand-off.
 *
 * Manifest contract (served by athar_view at <src>/manifest.json):
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
 * Adjacent steps share a file (rN is new-side of one step, old-side of the
 * next); the server gives that file one stable url, so a per-url mesh cache in
 * App.svelte tessellates each revision once.
 */

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
  fetchOldBytes: () => Promise<ArrayBuffer>
  fetchNewBytes: () => Promise<ArrayBuffer>
  fetchReport: () => Promise<unknown>
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

/** Pure manifest validation, unit-tested without any network. */
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
  if (typeof m.active_step === 'number' && m.active_step >= 0 && m.active_step < steps.length) {
    activeStep = Math.floor(m.active_step)
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

export async function loadChainFromSrc(src: string): Promise<LoadedChain> {
  const base = src.replace(/\/+$/, '')
  const json = await (await fetchOk(`${base}/manifest.json`)).json()
  let parsed: ParsedManifest
  try {
    parsed = parseManifest(json)
  } catch (error) {
    throw new Error(`${base}/manifest.json is not an Athar viewer manifest. (${error})`)
  }
  const resolve = (file: ManifestFile) => new URL(file.url, `${base}/`).toString()
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
      fetchOldBytes: () => fetchOk(oldUrl).then((r) => r.arrayBuffer()),
      fetchNewBytes: () => fetchOk(newUrl).then((r) => r.arrayBuffer()),
      fetchReport: () => fetchOk(reportUrl).then((r) => r.json()),
    }
  })
  return { steps, activeStep: parsed.activeStep, warning: parsed.warning }
}

export interface DroppedFiles {
  oldFile?: File
  newFile?: File
  reportFile?: File
}

/**
 * Classify a multi-file drop: .json → report, .ifc by name-sorted order →
 * old then new (single .ifc → fills the first empty slot).
 */
export function classifyDrop(files: File[], current: DroppedFiles): DroppedFiles {
  const next = { ...current }
  const ifcs = files
    .filter((f) => f.name.toLowerCase().endsWith('.ifc'))
    .sort((a, b) => a.name.localeCompare(b.name))
  const jsons = files.filter((f) => f.name.toLowerCase().endsWith('.json'))
  if (jsons.length > 0) next.reportFile = jsons[0]
  if (ifcs.length >= 2) {
    next.oldFile = ifcs[0]
    next.newFile = ifcs[1]
  } else if (ifcs.length === 1) {
    if (!next.oldFile) next.oldFile = ifcs[0]
    else next.newFile = ifcs[0]
  }
  return next
}

export function chainFromFiles(files: Required<DroppedFiles>): LoadedChain {
  const step: StepSource = {
    label: `${files.oldFile.name} → ${files.newFile.name}`,
    oldName: files.oldFile.name,
    newName: files.newFile.name,
    oldKey: `file:${files.oldFile.name}`,
    newKey: `file:${files.newFile.name}`,
    fetchOldBytes: () => files.oldFile.arrayBuffer(),
    fetchNewBytes: () => files.newFile.arrayBuffer(),
    fetchReport: () =>
      files.reportFile.text().then((text) => {
        try {
          return JSON.parse(text)
        } catch {
          throw new Error(`${files.reportFile.name} is not valid JSON.`)
        }
      }),
  }
  return { steps: [step], activeStep: 0 }
}
