/**
 * Types mirroring the Athar core engine's JSON delta report
 * (athar/delta/report.py — build_delta_report).
 */

export interface EntitySummary {
  step_id: number
  guid: string | null
  name: string | null
  class: string
  entity_type: string
}

export type AspectState = 'changed' | 'unchanged'

export interface Aspects {
  geometry: AspectState
  data: AspectState
  topology: AspectState
  placement: AspectState
  relations: AspectState
  class: AspectState
  /** Translation delta (dx, dy, dz) in millimetres, or null when either side lacks a placement. */
  placement_delta_mm: [number, number, number] | null
}

export type ChangeScope = 'intrinsic' | 'transitive' | 'mixed' | 'none'

/** One relationship whose target differs: the entities only the old or only the new side names. */
export interface RelationChange {
  relation: string
  role: string
  old: EntitySummary[]
  new: EntitySummary[]
}

export interface MatchedItem {
  old: EntitySummary
  new: EntitySummary
  match: { score: number; reason: string }
  aspects: Aspects
  data_hash: { old: string; new: string }
  change_scope: ChangeScope
  relations?: RelationChange[]
  class?: { old_class: string; new_class: string }
  /** Nearest ancestor whose own placement change this entity's placement change follows. */
  moved_with?: EntitySummary
  /** Present when the pair matched by content and its GlobalId was regenerated. */
  identity?: { guid_changed: boolean; old_guid: string; new_guid: string }
}

/** 2+ placement-changed pairs sharing one quantized rigid motion. */
export interface PlacementCohort {
  delta_mm: [number, number, number]
  rotation_deg: number
  root_cause?: EntitySummary
  count: number
  /** New-side step ids of the cohort members. */
  members: number[]
}

export interface ReportStats {
  added: number
  deleted: number
  modified: number
  unchanged: number
  old_signatures: number
  new_signatures: number
  modified_change_scope?: Record<string, number>
  placement_cohorts?: PlacementCohort[]
  [key: string]: unknown
}

export interface AtharReport {
  engine: string
  canon_version: string
  schemas: { old: string; new: string }
  stats: ReportStats
  added: EntitySummary[]
  deleted: EntitySummary[]
  modified: MatchedItem[]
  unchanged: MatchedItem[]
}

export type Side = 'old' | 'new'
export type Section = 'added' | 'deleted' | 'modified' | 'unchanged'
