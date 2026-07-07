import { describe, expect, it } from 'bun:test'

import { classifyDrop, MANIFEST_SCHEMA_VERSION, parseManifest } from '../src/lib/sources'

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

  it('clamps an out-of-range active_step to 0', () => {
    expect(parseManifest({ ...chain, active_step: 9 }).activeStep).toBe(0)
    expect(parseManifest({ ...chain, active_step: -1 }).activeStep).toBe(0)
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

  it('rejects a single-pair manifest missing a section', () => {
    expect(() => parseManifest({ athar_viewer_manifest: 1, old: { url: '/o' } })).toThrow()
  })
})

describe('classifyDrop', () => {
  const file = (name: string) => new File(['x'], name)

  it('assigns two ifcs by sorted order and a json report', () => {
    const result = classifyDrop([file('b.ifc'), file('a.ifc'), file('r.json')], {})
    expect(result.oldFile?.name).toBe('a.ifc')
    expect(result.newFile?.name).toBe('b.ifc')
    expect(result.reportFile?.name).toBe('r.json')
  })
})
