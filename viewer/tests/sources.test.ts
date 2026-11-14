import { describe, expect, it } from 'bun:test'

import {
  addDroppedFiles,
  assignRoles,
  chainFromFiles,
  dataUrl,
  hashFile,
  MANIFEST_SCHEMA_VERSION,
  NO_DROPS,
  parseManifest,
  readReportFile,
  reportInputHashes,
  sha256Hex,
} from '../src/lib/sources'
import type { AtharReport } from '../src/lib/types'

const singlePair = {
  athar_viewer_manifest: 1,
  schema_version: 1,
  old: { name: 'old.ifc', url: '/files/old.ifc' },
  new: { name: 'new.ifc', url: '/files/new.ifc' },
  report: { url: '/files/report.json' },
}

const chain = {
  athar_viewer_manifest: 1,
  schema_version: 2,
  steps: [
    {
      label: 'r5 → r6',
      old: { name: 'r5.ifc', url: '/files/model/0.ifc' },
      new: { name: 'r6.ifc', url: '/files/model/1.ifc' },
      report: { url: '/files/report/0.json' },
    },
    {
      label: 'r6 → r7',
      old: { name: 'r6.ifc', url: '/files/model/1.ifc' },
      new: { name: 'r7.ifc', url: '/files/model/2.ifc' },
      report: { url: '/files/report/1.json' },
    },
  ],
  active_step: 1,
}

describe('parseManifest', () => {
  it('accepts a schema-1 single-pair manifest as a one-step chain', () => {
    const parsed = parseManifest(singlePair)
    expect(parsed.steps).toHaveLength(1)
    expect(parsed.activeStep).toBe(0)
    expect(parsed.warning).toBeUndefined()
    expect(parsed.steps[0].old.url).toBe('/files/old.ifc')
    expect(parsed.steps[0].new.name).toBe('new.ifc')
    expect(parsed.steps[0].report.url).toBe('/files/report.json')
  })

  it('parses a schema-2 chain and honors active_step', () => {
    const parsed = parseManifest(chain)
    expect(parsed.steps).toHaveLength(2)
    expect(parsed.activeStep).toBe(1)
    expect(parsed.steps[0].label).toBe('r5 → r6')
    // Adjacent steps reuse the shared file's url, enabling the mesh cache.
    expect(parsed.steps[0].new.url).toBe(parsed.steps[1].old.url)
  })

  it('rejects an out-of-range active_step', () => {
    expect(() => parseManifest({ ...chain, active_step: 9 })).toThrow('active_step')
    expect(() => parseManifest({ ...chain, active_step: -1 })).toThrow('active_step')
    expect(() => parseManifest({ ...chain, active_step: 0.5 })).toThrow('active_step')
  })

  it('warns when the manifest schema is newer than the viewer', () => {
    const parsed = parseManifest({ ...singlePair, schema_version: MANIFEST_SCHEMA_VERSION + 1 })
    expect(parsed.warning).toContain('update the viewer')
  })

  it('rejects a non-manifest payload', () => {
    expect(() => parseManifest(null)).toThrow()
    expect(() => parseManifest({})).toThrow()
    expect(() => parseManifest({ athar_viewer_manifest: 2 })).toThrow()
  })

  it('rejects a malformed chain', () => {
    expect(() => parseManifest({ ...chain, steps: [] })).toThrow()
    const missingUrl = {
      athar_viewer_manifest: 1,
      schema_version: 2,
      steps: [{ label: 'x', old: { name: 'a' }, new: { url: '/b' }, report: { url: '/r' } }],
    }
    expect(() => parseManifest(missingUrl)).toThrow()
  })

  it('rejects a protocol-relative file url', () => {
    expect(() => parseManifest({ ...singlePair, old: { url: '//elsewhere.example/x.ifc' } })).toThrow('protocol-relative')
  })

  it('rejects a single-pair manifest missing a section', () => {
    expect(() => parseManifest({ athar_viewer_manifest: 1, old: { url: '/o' } })).toThrow()
  })
})

describe('addDroppedFiles', () => {
  const file = (name: string) => new File(['x'], name)

  it('keeps ifcs in drop order without assigning roles, and takes the json as report', () => {
    const result = addDroppedFiles([file('final.ifc'), file('r.json'), file('draft.ifc')], NO_DROPS)
    expect(result.ifcs.map((f) => f.name)).toEqual(['final.ifc', 'draft.ifc'])
    expect(result.reportFile?.name).toBe('r.json')
  })

  it('keeps the two most recent ifcs across drops', () => {
    let drops = addDroppedFiles([file('a.ifc')], NO_DROPS)
    drops = addDroppedFiles([file('b.ifc'), file('c.ifc')], drops)
    expect(drops.ifcs.map((f) => f.name)).toEqual(['b.ifc', 'c.ifc'])
  })
})

describe('chainFromFiles', () => {
  it('keys models and reports by content, not file names or the model pair', async () => {
    const a = await hashFile(new File(['first'], 'same.ifc'))
    const b = await hashFile(new File(['second'], 'same.ifc'))
    const copy = await hashFile(new File(['first'], 'renamed.ifc'))
    const first = chainFromFiles(a, b, {} as AtharReport, 'report-a').steps[0]
    const second = chainFromFiles(copy, b, {} as AtharReport, 'report-b').steps[0]
    expect(first.oldKey).not.toBe(first.newKey)
    expect(first.oldKey).toBe(second.oldKey)
    expect(first.reportKey).not.toBe(second.reportKey)
    expect(await first.fetchOldBytes()).toEqual(await a.file.arrayBuffer())
  })
})

describe('sha256Hex', () => {
  it('hashes bytes as lowercase hex', async () => {
    expect(await sha256Hex(new TextEncoder().encode('abc').buffer as ArrayBuffer)).toBe(
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    )
  })
})

describe('assignRoles', () => {
  const hashed = (name: string, sha256: string) => ({ file: { name }, sha256 })
  const inputs = { old: 'aaa', new: 'bbb' }
  const report = (old: unknown, new_: unknown) => ({ audit: { inputs: { old: { sha256: old }, new: { sha256: new_ } } } })

  it('assigns by hash when the names sort the other way', () => {
    const final = hashed('final.ifc', 'aaa')
    const draft = hashed('draft.ifc', 'bbb')
    const roles = assignRoles([draft, final], inputs)
    expect(roles).toEqual({ verified: true, old: final, new: draft })
  })

  it('assigns by hash in drop order too', () => {
    const a = hashed('a.ifc', 'aaa')
    const b = hashed('b.ifc', 'bbb')
    expect(assignRoles([a, b], inputs)).toEqual({ verified: true, old: a, new: b })
  })

  it('refuses a file the report did not compare and names it', () => {
    const roles = assignRoles([hashed('a.ifc', 'aaa'), hashed('other.ifc', 'ccc')], inputs)
    expect(roles.verified).toBe(false)
    if (!roles.verified) expect(roles.reason).toContain('other.ifc')
  })

  it('refuses the same input dropped twice', () => {
    const roles = assignRoles([hashed('a.ifc', 'aaa'), hashed('copy.ifc', 'aaa')], inputs)
    expect(roles.verified).toBe(false)
  })

  it('refuses when the report records no input hashes', () => {
    expect(assignRoles([hashed('a.ifc', 'aaa'), hashed('b.ifc', 'bbb')], null).verified).toBe(false)
  })

  it('reads input hashes from the report audit block', () => {
    expect(reportInputHashes(report('AAA', 'bbb'))).toEqual({ old: 'aaa', new: 'bbb' })
    expect(reportInputHashes({})).toBeNull()
    expect(reportInputHashes(report('aaa', undefined))).toBeNull()
  })
})

describe('dataUrl', () => {
  const page = 'https://viewer.example/app/?src=http%3A%2F%2F127.0.0.1%3A4799'

  it('resolves manifest file urls against the src origin', () => {
    expect(dataUrl(page, 'http://127.0.0.1:4799', '/files/old.ifc')).toBe('http://127.0.0.1:4799/files/old.ifc')
    expect(dataUrl(page, 'http://127.0.0.1:4799/', 'manifest.json')).toBe('http://127.0.0.1:4799/manifest.json')
  })

  it('keeps a path prefix in the src', () => {
    expect(dataUrl(page, 'http://127.0.0.1:4799/s/tok', '/files/report/0.json')).toBe(
      'http://127.0.0.1:4799/s/tok/files/report/0.json',
    )
  })

  it('carries a token in the src query', () => {
    expect(dataUrl(page, 'http://127.0.0.1:4799/?token=abc', 'manifest.json')).toBe(
      'http://127.0.0.1:4799/manifest.json?token=abc',
    )
  })

  it('carries the page query, except src, and resolves a relative src against the page', () => {
    const served = 'http://127.0.0.1:4799/?token=abc&src=.'
    expect(dataUrl(served, '.', '/files/new.ifc')).toBe('http://127.0.0.1:4799/files/new.ifc?token=abc')
  })

  it('carries the page query only when the page is served from the src origin', () => {
    const hosted = 'https://viewer.example/?key=secret&src=http%3A%2F%2F127.0.0.1%3A4799%2F%3Ftoken%3Dabc'
    expect(dataUrl(hosted, 'http://127.0.0.1:4799/?token=abc', 'manifest.json')).toBe(
      'http://127.0.0.1:4799/manifest.json?token=abc',
    )
  })

  it('does not carry the query to another origin', () => {
    expect(dataUrl(page, 'http://127.0.0.1:4799/?token=abc', 'https://elsewhere.example/x.ifc')).toBe(
      'https://elsewhere.example/x.ifc',
    )
  })
})

describe('readReportFile', () => {
  const minimal = { engine: 'athar', schemas: { old: 'IFC4', new: 'IFC4' }, added: [], deleted: [], modified: [], unchanged: [] }

  it('returns the parsed report and the hash of the bytes it parsed', async () => {
    const bytes = new TextEncoder().encode(JSON.stringify(minimal))
    const read = await readReportFile(new File([bytes], 'report.json'))
    expect(read.report.engine).toBe('athar')
    expect(read.sha256).toBe(await sha256Hex(bytes.buffer as ArrayBuffer))
  })

  it('names the file when it is not JSON and rejects a JSON file that is not a report', async () => {
    await expect(readReportFile(new File(['{'], 'broken.json'))).rejects.toThrow('broken.json is not valid JSON.')
    await expect(readReportFile(new File(['{}'], 'other.json'))).rejects.toThrow('Not an Athar report')
  })
})
