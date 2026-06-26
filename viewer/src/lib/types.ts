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
  /** Translation delta (dx, dy, dz) in millimetres, or null when either side lacks a placement. */
  placement_delta_mm: [number, number, number] | null
}

export type ChangeScope = 'intrinsic' | 'transitive' | 'mixed' | 'none'

export interface MatchedItem {
  old: EntitySummary
  new: EntitySummary
  match: { score: number; reason: string }
  aspects: Aspects
  data_hash: { old: string; new: string }
  change_scope: ChangeScope
}

export interface ReportStats {
  added: number
  deleted: number
  modified: number
  unchanged: number
  old_signatures: number
  new_signatures: number
  modified_change_scope?: Record<string, number>
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
