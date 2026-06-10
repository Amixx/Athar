/**
 * Pure report → render-state mapping. No three.js, no DOM — unit-tested.
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
 *                and placement are both unchanged (data/topology-only edits);
 *                old+new meshes would be coincident and crossfading two
 *                identical transparent meshes just z-fights.
 * - unchanged    new side only (old copy is geometrically identical), gray,
 *                ghosted, constant across the slider
 * - extra        new-side meshes with no report record (e.g. products the
 *                engine doesn't signature) — treated like unchanged.
 *
 * Old-side meshes for unchanged/extra entities are not rendered at all to
 * avoid duplicate coincident geometry.
 */

import type { EntityRecord } from './report'
import type { AtharReport, MatchedItem, Section, Side } from './types'

export type Bucket =
  | 'deleted'
  | 'added'
  | 'modifiedOld'
  | 'modifiedNew'
  | 'modifiedStatic'
  | 'unchanged'
  | 'extra'

export const ALL_BUCKETS: Bucket[] = [
  'deleted',
  'added',
  'modifiedOld',
  'modifiedNew',
  'modifiedStatic',
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
  unchanged: 'new',
  extra: 'new',
}

export const BUCKET_COLORS: Record<Bucket, string> = {
  deleted: '#e5484d',
  added: '#30a46c',
  modifiedOld: '#3e63dd',
  modifiedNew: '#3e63dd',
  modifiedStatic: '#3e63dd',
  unchanged: '#8b8d98',
  extra: '#8b8d98',
}

/** Slider snap points (Onshape-style old → new sweep). */
export const SNAP = { old: 0, both: 0.5, new: 1 } as const

const VISIBILITY_EPS = 0.02
export const GHOST_OPACITY = 0.15

/**
 * Assign the visual bucket for one side's mesh of an entity.
 * Returns null when that side's mesh must not be rendered.
 */
export function bucketFor(side: Side, rec: EntityRecord | undefined): Bucket | null {
  if (rec === undefined) {
    // Mesh without a report record: render the new-side copy as uncategorized
    // gray; skip the old-side copy (it would coincide when nothing changed,
    // and we have no identity evidence to do better).
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
  unchanged: boolean
  /** Ghost (translucent) unchanged geometry instead of solid gray. */
  ghostUnchanged: boolean
}

export const DEFAULT_TOGGLES: Toggles = {
  deleted: true,
  added: true,
  modified: true,
  unchanged: true,
  ghostUnchanged: true,
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
    unchanged: appearance('unchanged', toggles.unchanged, ghost),
    extra: appearance('extra', toggles.unchanged, ghost),
  }
}

/**
 * Placement-only modification: the entity moved but its shape is the same.
 * These get an old→new displacement line between centroids.
 */
export function isPlacementOnly(item: MatchedItem): boolean {
  return item.aspects.placement === 'changed' && item.aspects.geometry === 'unchanged'
}

/** Euclidean norm of placement_delta_mm, or null. */
export function placementDeltaNorm(item: MatchedItem): number | null {
  const delta = item.aspects.placement_delta_mm
  if (!delta) return null
  return Math.hypot(delta[0], delta[1], delta[2])
}

/**
 * Entities-per-bucket over the tessellated entity id sets. Pure counterpart
 * of the scene build — the debug hook and tests use it so headless runs
 * without WebGL still report deterministic counts.
 */
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

/**
 * Report entities per section whose rendering side produced no meshes
 * (spatial containers, products without Body representation, …). These are
 * still listed in panels; they just cannot be colored in 3D.
 */
export function countMeshless(
  report: AtharReport,
  oldHas: (stepId: number) => boolean,
  newHas: (stepId: number) => boolean,
): Record<Section, number> {
  const counts: Record<Section, number> = { added: 0, deleted: 0, modified: 0, unchanged: 0 }
  for (const summary of report.added) if (!newHas(summary.step_id)) counts.added += 1
  for (const summary of report.deleted) if (!oldHas(summary.step_id)) counts.deleted += 1
  for (const item of report.modified) {
    if (!newHas(item.new.step_id) && !oldHas(item.old.step_id)) counts.modified += 1
  }
  for (const item of report.unchanged) if (!newHas(item.new.step_id)) counts.unchanged += 1
  return counts
}
