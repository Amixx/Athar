/**
 * Pure report → render-state mapping; no three.js, no DOM.
 *
 * Entities are grouped into a small set of visual buckets; the whole bucket
 * shares one color/opacity, so the old↔new slider only touches per-bucket
 * material state (no per-entity GPU writes during a drag).
 *
 * Bucket layout:
 * - deleted      old side, red, fades out as the slider moves old → new
 * - added        new side, green, fades in
 * - modifiedOld  old side, blue, fades out (only when the shape or placement
 *                actually differs between the two sides)
 * - modifiedNew  new side, blue, fades in (counterpart of modifiedOld)
 * - modifiedStatic  new side, blue, constant — modified pairs whose geometry
 *                and placement are both unchanged (data/topology-only edits),
 *                drawn once because coincident crossfading meshes z-fight.
 * - transitive   new side, bronze, constant + semi-transparent — modified pairs
 *                with change_scope 'transitive', i.e. flagged only because a
 *                dependency changed (no intrinsic edit). Separately
 *                toggleable; labelled "Indirect" in the UI.
 * - unchanged    new side only (old copy is geometrically identical), gray,
 *                ghosted, constant across the slider
 * - extra        new-side meshes with no report record (e.g. products the
 *                engine doesn't signature), magenta and separately toggleable:
 *                the report makes no claim about them.
 *
 * Old-side meshes for unchanged/extra entities are not rendered at all to
 * avoid duplicate coincident geometry; `unreportedEntities` lists the old-side
 * ones without a report record so the legend still accounts for them.
 */

import type { EntityRecord } from './report'
import type { AtharReport, EntitySummary, MatchedItem, PlacementCohort, Side } from './types'

export type Bucket =
  | 'deleted'
  | 'added'
  | 'modifiedOld'
  | 'modifiedNew'
  | 'modifiedStatic'
  | 'transitive'
  | 'unchanged'
  | 'extra'

export const ALL_BUCKETS: Bucket[] = [
  'deleted',
  'added',
  'modifiedOld',
  'modifiedNew',
  'modifiedStatic',
  'transitive',
  'unchanged',
  'extra',
]

/** Which model side each bucket's geometry comes from. */
export const BUCKET_SIDE: Record<Bucket, Side> = {
  deleted: 'old',
  modifiedOld: 'old',
  added: 'new',
  modifiedNew: 'new',
  modifiedStatic: 'new',
  transitive: 'new',
  unchanged: 'new',
  extra: 'new',
}

export const BUCKET_COLORS: Record<Bucket, string> = {
  deleted: '#e5484d',
  added: '#30a46c',
  modifiedOld: '#3e63dd',
  modifiedNew: '#3e63dd',
  modifiedStatic: '#3e63dd',
  transitive: '#a87a3c',
  unchanged: '#8b8d98',
  extra: '#d6409f',
}

/** Slider snap points along the old → new sweep. */
export const SNAP = { old: 0, both: 0.5, new: 1 } as const

const VISIBILITY_EPS = 0.02
export const GHOST_OPACITY = 0.15
/** Transitive (indirect) changes render subordinate (semi-transparent) to direct edits. */
export const TRANSITIVE_OPACITY = 0.5

export function isVoidingFeature(klass: string | undefined): boolean {
  return ['IFCOPENINGELEMENT', 'IFCOPENINGSTANDARDCASE', 'IFCVOIDINGFEATURE'].includes(klass?.toUpperCase() ?? '')
}

export function bucketFor(side: Side, rec: EntityRecord | undefined, meshClass?: string): Bucket | null {
  if (isVoidingFeature(rec?.klass ?? meshClass) && (!rec || rec.section === 'unchanged')) return null
  if (rec === undefined) {
    // Mesh without a report record: render the new-side copy only; the old-side
    // copy would coincide when nothing changed, and there is no identity
    // evidence to pair them.
    return side === 'new' ? 'extra' : null
  }
  switch (rec.section) {
    case 'deleted':
      return side === 'old' ? 'deleted' : null
    case 'added':
      return side === 'new' ? 'added' : null
    case 'unchanged':
      return side === 'new' ? 'unchanged' : null
    case 'modified': {
      if (rec.pair?.change_scope === 'transitive') {
        return side === 'new' ? 'transitive' : null
      }
      const aspects = rec.pair?.aspects
      const moved = aspects?.placement === 'changed'
      const reshaped = aspects?.geometry === 'changed'
      if (!moved && !reshaped) {
        return side === 'new' ? 'modifiedStatic' : null
      }
      return side === 'old' ? 'modifiedOld' : 'modifiedNew'
    }
  }
}

export interface Toggles {
  deleted: boolean
  added: boolean
  modified: boolean
  /** Transitive-only changes (entities flagged only because a dependency changed). */
  transitive: boolean
  unchanged: boolean
  /** Ghost (translucent) unchanged geometry instead of solid gray. */
  ghostUnchanged: boolean
  extra: boolean
}

export const DEFAULT_TOGGLES: Toggles = {
  deleted: true,
  added: true,
  modified: true,
  transitive: true,
  unchanged: true,
  ghostUnchanged: false,
  extra: true,
}

export interface Appearance {
  visible: boolean
  opacity: number
  color: string
}

/**
 * Per-bucket appearance for slider position t ∈ [0, 1]
 * (0 = old state, 1 = new state; the changed sets crossfade,
 * unchanged stays constant).
 */
export function computeAppearances(t: number, toggles: Toggles): Record<Bucket, Appearance> {
  const oldWeight = 1 - t
  const newWeight = t
  const ghost = toggles.ghostUnchanged ? GHOST_OPACITY : 1
  const appearance = (bucket: Bucket, on: boolean, opacity: number): Appearance => ({
    visible: on && opacity > VISIBILITY_EPS,
    opacity,
    color: BUCKET_COLORS[bucket],
  })
  return {
    deleted: appearance('deleted', toggles.deleted, oldWeight),
    added: appearance('added', toggles.added, newWeight),
    modifiedOld: appearance('modifiedOld', toggles.modified, oldWeight),
    modifiedNew: appearance('modifiedNew', toggles.modified, newWeight),
    modifiedStatic: appearance('modifiedStatic', toggles.modified, 1),
    transitive: appearance('transitive', toggles.transitive, TRANSITIVE_OPACITY),
    unchanged: appearance('unchanged', toggles.unchanged, ghost),
    extra: appearance('extra', toggles.extra, 1),
  }
}

/**
 * Placement-only modification: the entity moved but its shape is the same.
 * These get an old→new displacement line between centroids.
 */
export function isPlacementOnly(item: MatchedItem): boolean {
  return item.aspects.placement === 'changed' && item.aspects.geometry === 'unchanged'
}

/** One displacement arrow: a single pair, or a whole placement cohort. */
export interface DisplacementGroup {
  pairs: Array<{ oldId: number; newId: number }>
}

/**
 * Displacement arrows with group moves collapsed: every engine placement
 * cohort becomes one group (all members, whatever else changed on them —
 * membership already proves the shared translation), and each remaining
 * placement-only pair stays its own single-arrow group. Cohorts first in
 * engine order, then singles in report order.
 */
export function groupDisplacements(
  modified: MatchedItem[],
  cohorts: PlacementCohort[],
): DisplacementGroup[] {
  const cohortIndex = new Map<number, number>()
  cohorts.forEach((cohort, i) => {
    for (const member of cohort.members) cohortIndex.set(member, i)
  })
  const grouped: DisplacementGroup[] = cohorts.map(() => ({ pairs: [] }))
  const singles: DisplacementGroup[] = []
  for (const item of modified) {
    const pair = { oldId: item.old.step_id, newId: item.new.step_id }
    const index = cohortIndex.get(item.new.step_id)
    if (index !== undefined) grouped[index].pairs.push(pair)
    else if (isPlacementOnly(item)) singles.push({ pairs: [pair] })
  }
  return [...grouped.filter((group) => group.pairs.length > 0), ...singles]
}

/** Euclidean norm of placement_delta_mm, or null. */
export function placementDeltaNorm(item: MatchedItem): number | null {
  const delta = item.aspects.placement_delta_mm
  if (!delta) return null
  return Math.hypot(delta[0], delta[1], delta[2])
}

/** Entities per bucket over the tessellated entity id sets, as the scene build assigns them. */
export function countBucketEntities(
  oldIds: Iterable<number>,
  newIds: Iterable<number>,
  bucketOfFn: (side: Side, stepId: number) => Bucket | null,
): Partial<Record<Bucket, number>> {
  const counts: Partial<Record<Bucket, number>> = {}
  const tally = (side: Side, ids: Iterable<number>) => {
    for (const stepId of ids) {
      const bucket = bucketOfFn(side, stepId)
      if (bucket === null || BUCKET_SIDE[bucket] !== side) continue
      counts[bucket] = (counts[bucket] ?? 0) + 1
    }
  }
  tally('old', oldIds)
  tally('new', newIds)
  return counts
}

/** Tessellated entities of each side that have no report record, in ascending step id order. */
export function unreportedEntities(
  oldIds: Iterable<number>,
  newIds: Iterable<number>,
  hasRecord: (side: Side, stepId: number) => boolean,
): Record<Side, number[]> {
  const missing = (side: Side, ids: Iterable<number>) =>
    [...ids].filter((stepId) => !hasRecord(side, stepId)).sort((a, b) => a - b)
  return { old: missing('old', oldIds), new: missing('new', newIds) }
}

/** Legend rows that count report entities; `extra` meshes have no report record. */
export type ReportKey = 'added' | 'deleted' | 'modified' | 'transitive' | 'unchanged'

export const REPORT_KEYS: ReportKey[] = ['added', 'deleted', 'modified', 'transitive', 'unchanged']

export interface UndrawnEntity {
  key: ReportKey
  side: Side
  stepId: number
  name: string | null
  klass: string
}

export interface ReportCoverage {
  inReport: Record<ReportKey, number>
  drawn: Record<ReportKey, number>
  /** Report entities none of whose meshes the scene renders, in report order. */
  undrawn: UndrawnEntity[]
}

/**
 * Report entities per legend row, and which of them the scene draws: an entity
 * is drawn when one of its sides has meshes and that side maps to a bucket.
 */
export function reportCoverage(
  report: AtharReport,
  hasMeshes: (side: Side, stepId: number) => boolean,
  bucketOfFn: (side: Side, stepId: number) => Bucket | null,
): ReportCoverage {
  const zero = (): Record<ReportKey, number> => ({ added: 0, deleted: 0, modified: 0, transitive: 0, unchanged: 0 })
  const coverage: ReportCoverage = { inReport: zero(), drawn: zero(), undrawn: [] }
  const visit = (key: ReportKey, sides: Array<[Side, EntitySummary]>) => {
    coverage.inReport[key] += 1
    const drawn = sides.some(
      ([side, summary]) => hasMeshes(side, summary.step_id) && bucketOfFn(side, summary.step_id) !== null,
    )
    if (drawn) {
      coverage.drawn[key] += 1
      return
    }
    const [side, summary] = sides[sides.length - 1]
    coverage.undrawn.push({ key, side, stepId: summary.step_id, name: summary.name, klass: summary.class })
  }
  for (const summary of report.added) visit('added', [['new', summary]])
  for (const summary of report.deleted) visit('deleted', [['old', summary]])
  for (const item of report.modified) {
    visit(item.change_scope === 'transitive' ? 'transitive' : 'modified', [
      ['old', item.old],
      ['new', item.new],
    ])
  }
  for (const item of report.unchanged) {
    visit('unchanged', [
      ['old', item.old],
      ['new', item.new],
    ])
  }
  return coverage
}
