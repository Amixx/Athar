/**
 * Input acquisition: drag-drop/file-picker classification and the
 * `?src=<origin>` manifest fetch used by the `athar view` CLI hand-off.
 *
 * Manifest contract (served by athar_view at <src>/manifest.json):
 * {
 *   "athar_viewer_manifest": 1,
 *   "schema_version": 1,
 *   "generator": "athar_view/0.1.0",
 *   "old":    {"name": "old.ifc", "url": "/files/old.ifc"},
 *   "new":    {"name": "new.ifc", "url": "/files/new.ifc"},
 *   "report": {"url": "/files/report.json"}
 * }
 */

export const MANIFEST_SCHEMA_VERSION = 1

export interface LoadedInputs {
  oldBytes: ArrayBuffer
  newBytes: ArrayBuffer
  reportJson: unknown
  oldName: string
  newName: string
  /** Non-fatal handshake warning (e.g. CLI/app schema version mismatch). */
  warning?: string
}

interface ManifestFile {
  name?: string
  url: string
}

interface ViewerManifest {
  athar_viewer_manifest: number
  schema_version: number
  generator?: string
  old: ManifestFile
  new: ManifestFile
  report: ManifestFile
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

export async function loadFromSrc(src: string): Promise<LoadedInputs> {
  const base = src.replace(/\/+$/, '')
  const manifest = (await (await fetchOk(`${base}/manifest.json`)).json()) as ViewerManifest
  if (manifest.athar_viewer_manifest !== 1 || !manifest.old || !manifest.new || !manifest.report) {
    throw new Error(`${base}/manifest.json is not an Athar viewer manifest.`)
  }
  let warning: string | undefined
  if (manifest.schema_version !== MANIFEST_SCHEMA_VERSION) {
    warning =
      `CLI manifest schema v${manifest.schema_version} != viewer v${MANIFEST_SCHEMA_VERSION}; ` +
      `update athar or the viewer if something looks off.`
  }
  const resolve = (file: ManifestFile) => new URL(file.url, `${base}/`).toString()
  const [oldBytes, newBytes, reportJson] = await Promise.all([
    fetchOk(resolve(manifest.old)).then((r) => r.arrayBuffer()),
    fetchOk(resolve(manifest.new)).then((r) => r.arrayBuffer()),
    fetchOk(resolve(manifest.report)).then((r) => r.json()),
  ])
  return {
    oldBytes,
    newBytes,
    reportJson,
    oldName: manifest.old.name ?? 'old.ifc',
    newName: manifest.new.name ?? 'new.ifc',
    warning,
  }
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

export async function loadFromFiles(files: Required<DroppedFiles>): Promise<LoadedInputs> {
  const [oldBytes, newBytes, reportText] = await Promise.all([
    files.oldFile.arrayBuffer(),
    files.newFile.arrayBuffer(),
    files.reportFile.text(),
  ])
  let reportJson: unknown
  try {
    reportJson = JSON.parse(reportText)
  } catch {
    throw new Error(`${files.reportFile.name} is not valid JSON.`)
  }
  return {
    oldBytes,
    newBytes,
    reportJson,
    oldName: files.oldFile.name,
    newName: files.newFile.name,
  }
}
