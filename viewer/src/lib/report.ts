/**
 * Report parsing/validation and the step_id-keyed diff index.
 *
 * Identity mapping is exact by design: ifc-lite mesh expressIds ARE STEP ids,
 * and the engine report carries step_id per entity (old-side ids for deleted
 * and modified-old, new-side ids for added and modified-new/unchanged).
 */

import type { AtharReport, EntitySummary, MatchedItem, Section, Side } from './types'

export interface EntityRecord {
  side: Side
  stepId: number
  guid: string | null
  name: string | null
  klass: string
  entityType: string
  section: Section
  /** Present for modified/unchanged records: the full matched item. */
  pair?: MatchedItem
}

export interface DiffIndex {
  report: AtharReport
  /** Old-side step_id → record (deleted + modified.old + unchanged.old). */
  old: Map<number, EntityRecord>
  /** New-side step_id → record (added + modified.new + unchanged.new). */
  new: Map<number, EntityRecord>
  /** Modified pairs whose placement changed — displacement-line candidates. */
  placementPairs: MatchedItem[]
}

export class ReportFormatError extends Error {}

export function parseReport(json: unknown): AtharReport {
  if (typeof json !== 'object' || json === null) {
    throw new ReportFormatError('Report is not a JSON object.')
  }
  const r = json as Record<string, unknown>
  if (r.engine !== 'athar') {
    throw new ReportFormatError('Not an Athar report: missing "engine": "athar".')
  }
  for (const section of ['added', 'deleted', 'modified', 'unchanged'] as const) {
    if (!Array.isArray(r[section])) {
      throw new ReportFormatError(`Report is missing the "${section}" section.`)
    }
  }
  if (typeof r.schemas !== 'object' || r.schemas === null) {
    throw new ReportFormatError('Report is missing "schemas".')
  }
  return json as AtharReport
}

function record(side: Side, summary: EntitySummary, section: Section, pair?: MatchedItem): EntityRecord {
  return {
    side,
    stepId: summary.step_id,
    guid: summary.guid,
    name: summary.name,
    klass: summary.class,
    entityType: summary.entity_type,
    section,
    pair,
  }
}

export interface ClassRow {
  klass: string
  added: number
  deleted: number
  modified: number
}

/** Per-class change rollup for the summary panel, busiest classes first. */
export function classBreakdown(report: AtharReport, limit = 8): ClassRow[] {
  const rows = new Map<string, ClassRow>()
  const bump = (klass: string, key: 'added' | 'deleted' | 'modified') => {
    let row = rows.get(klass)
    if (!row) rows.set(klass, (row = { klass, added: 0, deleted: 0, modified: 0 }))
    row[key] += 1
  }
  for (const summary of report.added) bump(summary.class, 'added')
  for (const summary of report.deleted) bump(summary.class, 'deleted')
  for (const item of report.modified) bump(item.new.class, 'modified')
  return [...rows.values()]
    .sort(
      (a, b) =>
        b.added + b.deleted + b.modified - (a.added + a.deleted + a.modified) ||
        a.klass.localeCompare(b.klass),
    )
    .slice(0, limit)
}

export function buildDiffIndex(report: AtharReport): DiffIndex {
  const oldMap = new Map<number, EntityRecord>()
  const newMap = new Map<number, EntityRecord>()
  const placementPairs: MatchedItem[] = []

  for (const summary of report.deleted) {
    oldMap.set(summary.step_id, record('old', summary, 'deleted'))
  }
  for (const summary of report.added) {
    newMap.set(summary.step_id, record('new', summary, 'added'))
  }
  for (const item of report.modified) {
    oldMap.set(item.old.step_id, record('old', item.old, 'modified', item))
    newMap.set(item.new.step_id, record('new', item.new, 'modified', item))
    if (item.aspects.placement === 'changed') {
      placementPairs.push(item)
    }
  }
  for (const item of report.unchanged) {
    oldMap.set(item.old.step_id, record('old', item.old, 'unchanged', item))
    newMap.set(item.new.step_id, record('new', item.new, 'unchanged', item))
  }

  return { report, old: oldMap, new: newMap, placementPairs }
}
