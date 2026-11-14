import type { ModifiedEntry, PropertyDelta, PropertyValue, Report, ReportEntity, StoreEvent } from './api'

const LOCALE = 'en-US'

function decimal(value: number, minDigits: number, maxDigits: number): string {
  return new Intl.NumberFormat(LOCALE, {
    minimumFractionDigits: minDigits,
    maximumFractionDigits: maxDigits,
  }).format(value + 0)
}

export function formatCount(value: number): string {
  return decimal(value, 0, 0)
}

export function shortSha(sha: string | undefined | null): string {
  return sha ? sha.slice(0, 12) : 'none'
}

export function formatBytes(bytes: number | undefined | null): string {
  if (bytes === undefined || bytes === null) return 'unknown size'
  if (bytes >= 1_000_000_000) return `${decimal(bytes / 1_000_000_000, 1, 1)} GB`
  if (bytes >= 1_000_000) return `${decimal(bytes / 1_000_000, 1, 1)} MB`
  if (bytes >= 1_000) return `${decimal(bytes / 1_000, 1, 1)} kB`
  return `${formatCount(bytes)} B`
}

export function formatWhen(iso: string | undefined, timeZone?: string): string {
  if (!iso) return 'unknown time'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return iso
  return date.toLocaleString(LOCALE, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short',
    timeZone,
  })
}

/** One length scaled to m at 1 m and above, else mm. */
export function formatLengthMm(mm: number): string {
  return Math.abs(mm) >= 1000 ? `${decimal(mm / 1000, 1, 4)} m` : `${decimal(mm, 1, 1)} mm`
}

/** A translation vector in one unit: m when any component reaches 1 m. */
export function formatVectorMm(mm: readonly number[]): string {
  const metres = mm.some((component) => Math.abs(component) >= 1000)
  const parts = mm.map((component) => (metres ? decimal(component / 1000, 1, 4) : decimal(component, 1, 1)))
  return `${parts.join(', ')} ${metres ? 'm' : 'mm'}`
}

export function formatPropertyValue(value: PropertyValue | undefined, fullPrecision = false): string {
  if (value === undefined || value === null) return 'not set'
  if (Array.isArray(value)) return value.map((part) => formatPropertyValue(part, fullPrecision)).join(', ')
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'number') {
    if (fullPrecision) return new Intl.NumberFormat(LOCALE, { maximumSignificantDigits: 17 }).format(value)
    const magnitude = Math.abs(value)
    if (magnitude !== 0 && magnitude < 0.001) {
      return new Intl.NumberFormat(LOCALE, { maximumSignificantDigits: 6 }).format(value)
    }
    return decimal(value, 0, 6)
  }
  return value
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

/** Revision letter for the register: baseline_set/approval events advance A, B, C. */
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

export type ReviewState =
  | { state: 'pending' }
  | { state: 'approved' | 'rejected' | 'stale'; eventId: string; at: string; actor: string }

/**
 * The decision state of every review in one model lineage's history, in
 * order. Mirrors the store: a decision on the review wins over staleness; a
 * later baseline_set or approval makes an undecided review stale.
 */
export function reviewStates(lineage: StoreEvent[]): Map<string, ReviewState> {
  const states = new Map<string, ReviewState>()
  lineage.forEach((event, index) => {
    if (event.event !== 'review') return
    const later = lineage.slice(index + 1)
    const decision = later.find(
      (other) => other.review_id === event.id && (other.event === 'approval' || other.event === 'rejection'),
    )
    const blocker = decision ?? later.find((other) => other.event === 'baseline_set' || other.event === 'approval')
    if (!blocker) {
      states.set(event.id, { state: 'pending' })
      return
    }
    const state = blocker === decision ? (decision.event === 'approval' ? 'approved' : 'rejected') : 'stale'
    states.set(event.id, { state, eventId: blocker.id, at: blocker.at, actor: blocker.actor })
  })
  return states
}

export function describeEvent(event: StoreEvent): string {
  switch (event.event) {
    case 'baseline_set':
      return 'Baseline established'
    case 'review':
      return isCleanDiff(event.diff?.stats) ? 'Delivery reviewed, no changes' : 'Delivery reviewed'
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

export interface PropertyRow {
  kind: 'changed' | 'added' | 'removed' | 'count'
  pset: string
  name: string
  inherited: boolean
  before: string
  after: string
}

/** Property-level changes of one modified entity as "Pset, Name: before to after" rows. */
export function propertyRows(entry: ModifiedEntry): PropertyRow[] {
  const deltas = entry.property_deltas
  if (!deltas) return []
  const row = (kind: PropertyRow['kind'], delta: PropertyDelta, before: string, after: string): PropertyRow => ({
    kind,
    pset: delta.pset,
    name: delta.name,
    inherited: delta.inherited === true,
    before,
    after,
  })
  return [
    ...(deltas.changed ?? []).map((d) => {
      const before = formatPropertyValue(d.old_value)
      const after = formatPropertyValue(d.new_value)
      return row('changed', d,
        before === after ? formatPropertyValue(d.old_value, true) : before,
        before === after ? formatPropertyValue(d.new_value, true) : after,
      )
    }),
    ...(deltas.added ?? []).map((d) => row('added', d, 'not set', formatPropertyValue(d.new_value))),
    ...(deltas.removed ?? []).map((d) => row('removed', d, formatPropertyValue(d.old_value), 'removed')),
    ...(deltas.count_changed ?? []).map((d) =>
      row('count', d, `${formatCount(d.old_count ?? 0)}x`, `${formatCount(d.new_count ?? 0)}x`),
    ),
  ]
}

const ASPECT_LABELS: Record<string, string> = {
  geometry: 'shape',
  data: 'data',
  topology: 'relationships',
  placement: 'position',
}

export function modifiedAspects(entry: ModifiedEntry): string[] {
  const aspects = entry.aspects ?? {}
  return Object.entries(aspects)
    .filter(([key, value]) => value === 'changed' && key !== 'placement_delta_mm')
    .map(([key]) => ASPECT_LABELS[key] ?? key)
}

export function placementDeltaMm(entry: ModifiedEntry): number | null {
  const delta = entry.aspects?.['placement_delta_mm']
  if (!Array.isArray(delta) || delta.length !== 3) return null
  const [x, y, z] = delta as number[]
  const norm = Math.sqrt(x * x + y * y + z * z)
  return norm > 0 ? norm : null
}

function movedOnly(entry: ModifiedEntry): boolean {
  const changed = modifiedAspects(entry)
  return changed.length === 1 && changed[0] === ASPECT_LABELS.placement
}

export interface CohortView {
  deltaMm: readonly number[]
  count: number
  members: ModifiedEntry[]
}

export interface Cohorts {
  cohorts: CohortView[]
  /** New-side step ids of cohort members whose only change is the move. */
  moveOnly: Set<number>
}

export function placementCohorts(report: Report): Cohorts {
  const byStep = new Map(report.modified.map((entry) => [entry.new.step_id, entry]))
  const moveOnly = new Set<number>()
  const cohorts = (report.stats.placement_cohorts ?? []).map((cohort) => {
    const members = cohort.members.flatMap((step) => {
      const entry = byStep.get(step)
      return entry ? [entry] : []
    })
    for (const entry of members) if (movedOnly(entry)) moveOnly.add(entry.new.step_id)
    return { deltaMm: cohort.delta_mm, count: cohort.count, members }
  })
  return { cohorts, moveOnly }
}

export interface ClassGroup {
  className: string
  kind: 'added' | 'deleted' | 'modified'
  entries: (ReportEntity | ModifiedEntry)[]
}

/**
 * Group report entries by (kind, class): deleted, then added, then modified,
 * larger groups first within a kind. Modified entries whose step id is in
 * `hidden` are left out (they are shown as part of a bulk move).
 */
export function groupChanges(report: Report, hidden: ReadonlySet<number> = new Set()): ClassGroup[] {
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
  for (const entry of report.modified ?? []) {
    if (hidden.has(entry.new.step_id)) continue
    push('modified', entry.new?.class ?? entry.old?.class ?? '?', entry)
  }
  const kindOrder = { deleted: 0, added: 1, modified: 2 }
  return [...groups.values()].sort(
    (a, b) =>
      kindOrder[a.kind] - kindOrder[b.kind] ||
      b.entries.length - a.entries.length ||
      a.className.localeCompare(b.className),
  )
}
