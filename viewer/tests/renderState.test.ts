import { describe, expect, it } from 'bun:test'

import { buildDiffIndex, classBreakdown, parseReport, ReportFormatError } from '../src/lib/report'
import {
  ALL_BUCKETS,
  BUCKET_COLORS,
  bucketFor,
  computeAppearances,
  DEFAULT_TOGGLES,
  GHOST_OPACITY,
  groupDisplacements,
  isPlacementOnly,
  placementDeltaNorm,
  reportCoverage,
  SNAP,
  TRANSITIVE_OPACITY,
  unreportedEntities,
} from '../src/lib/renderState'
import type { Aspects, AtharReport, EntitySummary, MatchedItem } from '../src/lib/types'

function summary(stepId: number, klass = 'IfcWall'): EntitySummary {
  return {
    step_id: stepId,
    guid: `guid-${stepId}`,
    name: `Entity ${stepId}`,
    class: klass,
    entity_type: klass.toUpperCase(),
  }
}

function aspects(overrides: Partial<Aspects> = {}): Aspects {
  return {
    geometry: 'unchanged',
    data: 'unchanged',
    topology: 'unchanged',
    placement: 'unchanged',
    placement_delta_mm: null,
    ...overrides,
  }
}

function matched(
  oldId: number,
  newId: number,
  asp: Aspects,
  changeScope: MatchedItem['change_scope'] = 'intrinsic',
): MatchedItem {
  return {
    old: summary(oldId),
    new: summary(newId),
    match: { score: 0.9, reason: 'guid' },
    aspects: asp,
    data_hash: { old: 'a', new: 'b' },
    change_scope: changeScope,
  }
}

const movedPair = matched(10, 110, aspects({ placement: 'changed', placement_delta_mm: [1000, 0, 0] }))
const reshapedPair = matched(11, 111, aspects({ geometry: 'changed' }))
const dataOnlyPair = matched(12, 112, aspects({ data: 'changed' }))
const topologyOnlyPair = matched(13, 113, aspects({ topology: 'changed' }))
// Transitive-only: flagged purely because a dependency changed (no intrinsic edit).
const transitivePair = matched(14, 114, aspects({ data: 'changed' }), 'transitive')
const unchangedPair = matched(20, 120, aspects())

const report: AtharReport = {
  engine: 'athar',
  canon_version: 'v3',
  schemas: { old: 'IFC4', new: 'IFC4' },
  stats: {
    added: 1,
    deleted: 1,
    modified: 5,
    unchanged: 1,
    old_signatures: 7,
    new_signatures: 7,
  },
  added: [summary(100)],
  deleted: [summary(1)],
  modified: [movedPair, reshapedPair, dataOnlyPair, topologyOnlyPair, transitivePair],
  unchanged: [unchangedPair],
}

describe('parseReport', () => {
  it('accepts an engine report', () => {
    expect(parseReport(JSON.parse(JSON.stringify(report)))).toBeDefined()
  })

  it('rejects non-athar payloads', () => {
    expect(() => parseReport({ engine: 'other' })).toThrow(ReportFormatError)
    expect(() => parseReport(null)).toThrow(ReportFormatError)
    expect(() => parseReport({ engine: 'athar', added: [] })).toThrow(ReportFormatError)
  })
})

describe('buildDiffIndex', () => {
  const index = buildDiffIndex(report)

  it('indexes both sides by step_id', () => {
    expect(index.old.get(1)?.section).toBe('deleted')
    expect(index.new.get(100)?.section).toBe('added')
    expect(index.old.get(10)?.section).toBe('modified')
    expect(index.new.get(110)?.section).toBe('modified')
    expect(index.old.get(20)?.section).toBe('unchanged')
    expect(index.new.get(120)?.section).toBe('unchanged')
    expect(index.old.has(100)).toBe(false)
    expect(index.new.has(1)).toBe(false)
  })

  it('keeps the matched item on both records', () => {
    expect(index.old.get(10)?.pair).toBe(movedPair)
    expect(index.new.get(110)?.pair).toBe(movedPair)
  })

  it('collects placement-changed pairs for displacement lines', () => {
    expect(index.placementPairs).toEqual([movedPair])
  })
})

describe('bucketFor', () => {
  const index = buildDiffIndex(report)

  it('maps sections to buckets per side', () => {
    expect(bucketFor('old', index.old.get(1))).toBe('deleted')
    expect(bucketFor('new', index.new.get(100))).toBe('added')
    expect(bucketFor('old', index.old.get(10))).toBe('modifiedOld')
    expect(bucketFor('new', index.new.get(110))).toBe('modifiedNew')
    expect(bucketFor('old', index.old.get(11))).toBe('modifiedOld')
    expect(bucketFor('new', index.new.get(111))).toBe('modifiedNew')
  })

  it('renders data/topology-only modifications once, from the new side', () => {
    expect(bucketFor('old', index.old.get(12))).toBeNull()
    expect(bucketFor('new', index.new.get(112))).toBe('modifiedStatic')
    expect(bucketFor('old', index.old.get(13))).toBeNull()
    expect(bucketFor('new', index.new.get(113))).toBe('modifiedStatic')
  })

  it('routes transitive-only modifications to the transitive bucket, new side', () => {
    expect(bucketFor('old', index.old.get(14))).toBeNull()
    expect(bucketFor('new', index.new.get(114))).toBe('transitive')
  })

  it('renders unchanged once, from the new side', () => {
    expect(bucketFor('old', index.old.get(20))).toBeNull()
    expect(bucketFor('new', index.new.get(120))).toBe('unchanged')
  })

  it('classifies meshes without report records as extra (new side only)', () => {
    expect(bucketFor('new', undefined)).toBe('extra')
    expect(bucketFor('old', undefined)).toBeNull()
  })
})

describe('computeAppearances', () => {
  it('shows only the old state at t=0', () => {
    const app = computeAppearances(SNAP.old, DEFAULT_TOGGLES)
    expect(app.deleted.visible).toBe(true)
    expect(app.deleted.opacity).toBe(1)
    expect(app.modifiedOld.visible).toBe(true)
    expect(app.added.visible).toBe(false)
    expect(app.modifiedNew.visible).toBe(false)
    expect(app.modifiedStatic.visible).toBe(true)
    expect(app.unchanged.visible).toBe(true)
    expect(app.unchanged.opacity).toBe(1)
  })

  it('shows only the new state at t=1', () => {
    const app = computeAppearances(SNAP.new, DEFAULT_TOGGLES)
    expect(app.deleted.visible).toBe(false)
    expect(app.modifiedOld.visible).toBe(false)
    expect(app.added.visible).toBe(true)
    expect(app.added.opacity).toBe(1)
    expect(app.modifiedNew.visible).toBe(true)
  })

  it('crossfades both states at t=0.5', () => {
    const app = computeAppearances(SNAP.both, DEFAULT_TOGGLES)
    expect(app.deleted.visible).toBe(true)
    expect(app.deleted.opacity).toBe(0.5)
    expect(app.added.visible).toBe(true)
    expect(app.added.opacity).toBe(0.5)
    expect(app.modifiedOld.opacity).toBe(0.5)
    expect(app.modifiedNew.opacity).toBe(0.5)
    expect(app.modifiedStatic.opacity).toBe(1)
  })

  it('honors section toggles', () => {
    const app = computeAppearances(SNAP.both, {
      ...DEFAULT_TOGGLES,
      deleted: false,
      modified: false,
      unchanged: false,
      extra: false,
    })
    expect(app.deleted.visible).toBe(false)
    expect(app.modifiedOld.visible).toBe(false)
    expect(app.modifiedNew.visible).toBe(false)
    expect(app.modifiedStatic.visible).toBe(false)
    expect(app.unchanged.visible).toBe(false)
    expect(app.extra.visible).toBe(false)
    expect(app.added.visible).toBe(true)
  })

  it('renders transitive subordinate and toggles it independently of direct edits', () => {
    const on = computeAppearances(SNAP.both, DEFAULT_TOGGLES)
    expect(on.transitive.visible).toBe(true)
    expect(on.transitive.opacity).toBe(TRANSITIVE_OPACITY)
    // Hiding transitive leaves direct modifications untouched.
    const transitiveOff = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, transitive: false })
    expect(transitiveOff.transitive.visible).toBe(false)
    expect(transitiveOff.modifiedStatic.visible).toBe(true)
    // Hiding direct modifications leaves transitive untouched.
    const modifiedOff = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, modified: false })
    expect(modifiedOff.transitive.visible).toBe(true)
    expect(modifiedOff.modifiedStatic.visible).toBe(false)
  })

  it('renders unchanged solid when ghosting is off', () => {
    const app = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, ghostUnchanged: false })
    expect(app.unchanged.opacity).toBe(1)
  })

  it('ghosts unchanged only when explicitly enabled', () => {
    const app = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, ghostUnchanged: true })
    expect(app.unchanged.opacity).toBe(GHOST_OPACITY)
  })
})

describe('extra bucket', () => {
  it('has a colour of its own', () => {
    const others = ALL_BUCKETS.filter((bucket) => bucket !== 'extra').map((bucket) => BUCKET_COLORS[bucket])
    expect(others).not.toContain(BUCKET_COLORS.extra)
  })

  it('toggles independently of unchanged and is never ghosted', () => {
    const hidden = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, extra: false, ghostUnchanged: true })
    expect(hidden.extra.visible).toBe(false)
    expect(hidden.unchanged.visible).toBe(true)
    const shown = computeAppearances(SNAP.both, { ...DEFAULT_TOGGLES, unchanged: false, ghostUnchanged: true })
    expect(shown.extra.visible).toBe(true)
    expect(shown.extra.opacity).toBe(1)
    expect(shown.unchanged.visible).toBe(false)
  })
})

describe('classBreakdown', () => {
  it('rolls up per-class counts, busiest first', () => {
    const rows = classBreakdown(report)
    expect(rows[0]).toEqual({ klass: 'IfcWall', added: 1, deleted: 1, modified: 5 })
    expect(rows).toHaveLength(1)
  })

  it('honors the row limit', () => {
    const wide: AtharReport = {
      ...report,
      added: [summary(100), { ...summary(101), class: 'IfcDoor' }],
    }
    expect(classBreakdown(wide, 1)).toHaveLength(1)
  })
})

describe('placement helpers', () => {
  it('detects placement-only modifications', () => {
    expect(isPlacementOnly(movedPair)).toBe(true)
    expect(isPlacementOnly(reshapedPair)).toBe(false)
    expect(isPlacementOnly(dataOnlyPair)).toBe(false)
    const movedAndReshaped = matched(
      30,
      130,
      aspects({ placement: 'changed', geometry: 'changed' }),
    )
    expect(isPlacementOnly(movedAndReshaped)).toBe(false)
  })

  it('computes the delta norm', () => {
    expect(placementDeltaNorm(movedPair)).toBe(1000)
    expect(placementDeltaNorm(dataOnlyPair)).toBeNull()
  })
})

describe('groupDisplacements', () => {
  const cohortA1 = matched(40, 140, aspects({ placement: 'changed', placement_delta_mm: [500, 0, 0] }))
  const cohortA2 = matched(
    41,
    141,
    aspects({ placement: 'changed', geometry: 'changed', placement_delta_mm: [500, 0, 0] }),
  )
  const cohorts = [{ delta_mm: [500, 0, 0] as [number, number, number], rotation_deg: 0, count: 2, members: [140, 141] }]

  it('collapses cohort members into one group and keeps loners as singles', () => {
    const groups = groupDisplacements([cohortA1, cohortA2, movedPair, reshapedPair], cohorts)
    expect(groups).toEqual([
      { pairs: [{ oldId: 40, newId: 140 }, { oldId: 41, newId: 141 }] },
      { pairs: [{ oldId: 10, newId: 110 }] },
    ])
  })

  it('includes cohort members that also changed geometry, but never loner reshapes', () => {
    const groups = groupDisplacements([cohortA2, reshapedPair], cohorts)
    expect(groups).toEqual([{ pairs: [{ oldId: 41, newId: 141 }] }])
  })

  it('is one single-pair group per placement-only move without cohorts', () => {
    const groups = groupDisplacements([movedPair, dataOnlyPair], [])
    expect(groups).toEqual([{ pairs: [{ oldId: 10, newId: 110 }] }])
  })
})

describe('reportCoverage', () => {
  it('counts each report entity once per legend row and lists the ones the scene does not draw', () => {
    const index = buildDiffIndex(report)
    const meshes = { old: new Set([1, 10, 14, 20]), new: new Set([100, 112]) }
    const coverage = reportCoverage(
      report,
      (side, stepId) => meshes[side].has(stepId),
      (side, stepId) => bucketFor(side, index[side].get(stepId)),
    )
    expect(coverage.inReport).toEqual({ added: 1, deleted: 1, modified: 4, transitive: 1, unchanged: 1 })
    expect(coverage.drawn).toEqual({ added: 1, deleted: 1, modified: 2, transitive: 0, unchanged: 0 })
    expect(coverage.undrawn.map(({ key, side, stepId }) => [key, side, stepId])).toEqual([
      ['modified', 'new', 111],
      ['modified', 'new', 113],
      ['transitive', 'new', 114],
      ['unchanged', 'new', 120],
    ])
  })
})

describe('unreportedEntities', () => {
  it('lists the tessellated entities of each side that have no report record', () => {
    const index = buildDiffIndex(report)
    const unreported = unreportedEntities([900, 1, 10, 901], [100, 950, 112], (side, stepId) => index[side].has(stepId))
    expect(unreported).toEqual({ old: [900, 901], new: [950] })
  })
})
