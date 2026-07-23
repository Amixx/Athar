/**
 * Pure presentation helpers for the review UI — unit-tested with bun, no DOM.
 */

import type { ModifiedEntry, Report, ReportEntity, StoreEvent } from './api'

export function shortSha(sha: string | undefined | null): string {
  return sha ? sha.slice(0, 10) : '—'
}

export function formatBytes(bytes: number | undefined | null): string {
  if (bytes === undefined || bytes === null) return '—'
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} kB`
  return `${bytes} B`
}

export function formatWhen(iso: string | undefined): string {
  if (!iso) return '—'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return iso
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export interface StatSummary {
  added: number
  deleted: number
  modified: number
  unchanged: number
}

export function statSummary(stats: Record<string, unknown> | undefined): StatSummary {
  const n = (key: string) => {
    const value = stats?.[key]
    return typeof value === 'number' ? value : 0
  }
  return { added: n('added'), deleted: n('deleted'), modified: n('modified'), unchanged: n('unchanged') }
}

export function isCleanDiff(stats: Record<string, unknown> | undefined): boolean {
  const s = statSummary(stats)
  return s.added === 0 && s.deleted === 0 && s.modified === 0
}

/** Revision letter for the register: baseline_set/approval events advance A, B, C… */
export function revisionLetters(events: StoreEvent[]): Map<string, string> {
  const letters = new Map<string, string>()
  let index = 0
  for (const event of events) {
    if (event.event === 'baseline_set' || event.event === 'approval') {
      letters.set(event.id, revisionLetter(index))
      index += 1
    }
  }
  return letters
}

export function revisionLetter(index: number): string {
  let label = ''
  let n = index
  do {
    label = String.fromCharCode(65 + (n % 26)) + label
    n = Math.floor(n / 26) - 1
  } while (n >= 0)
  return label
}

export function describeEvent(event: StoreEvent): string {
  switch (event.event) {
    case 'baseline_set':
      return 'Baseline established'
    case 'review': {
      const s = statSummary(event.diff?.stats)
      if (s.added === 0 && s.deleted === 0 && s.modified === 0) return 'Revision reviewed — no changes'
      return 'Revision reviewed'
    }
    case 'approval':
      return `Accepted review ${event.review_id}`
    case 'rejection':
      return `Rejected review ${event.review_id}`
    default:
      return event.event
  }
}

export function entityLabel(entity: ReportEntity): string {
  const name = entity.name ? ` "${entity.name}"` : ''
  return `${entity.class}${name}`
}

export interface ClassGroup {
  className: string
  kind: 'added' | 'deleted' | 'modified'
  entries: (ReportEntity | ModifiedEntry)[]
}

/** Group report entries by (kind, class), largest groups first, ripples last. */
export function groupChanges(report: Report): ClassGroup[] {
  const groups = new Map<string, ClassGroup>()
  const push = (kind: ClassGroup['kind'], className: string, entry: ReportEntity | ModifiedEntry) => {
    const key = `${kind}:${className}`
    let group = groups.get(key)
    if (!group) {
      group = { className, kind, entries: [] }
      groups.set(key, group)
    }
    group.entries.push(entry)
  }
  for (const entry of report.deleted ?? []) push('deleted', entry.class, entry)
  for (const entry of report.added ?? []) push('added', entry.class, entry)
  for (const entry of report.modified ?? []) push('modified', entry.new?.class ?? entry.old?.class ?? '?', entry)
  const kindOrder = { deleted: 0, added: 1, modified: 2 }
  return [...groups.values()].sort(
    (a, b) => kindOrder[a.kind] - kindOrder[b.kind] || b.entries.length - a.entries.length,
  )
}

export function modifiedAspects(entry: ModifiedEntry): string[] {
  const aspects = entry.aspects ?? {}
  return Object.entries(aspects)
    .filter(([key, value]) => value === 'changed' && key !== 'placement_delta_mm')
    .map(([key]) => key)
}

export function placementDeltaMm(entry: ModifiedEntry): number | null {
  const delta = entry.aspects?.['placement_delta_mm']
  if (!Array.isArray(delta) || delta.length !== 3) return null
  const [x, y, z] = delta as number[]
  const norm = Math.sqrt(x * x + y * y + z * z)
  return norm > 0 ? norm : null
}
