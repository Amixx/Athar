import { describe, expect, test } from 'bun:test'

import type { ModifiedEntry, Report, StoreEvent } from '../src/review/lib/api'
import {
  formatBytes,
  formatCount,
  formatLengthMm,
  formatPropertyValue,
  formatVectorMm,
  formatWhen,
  groupChanges,
  placementCohorts,
  propertyRows,
  reviewStates,
} from '../src/review/lib/format'

function event(partial: Partial<StoreEvent> & { id: string }): StoreEvent {
  return { event: 'review', at: '2026-11-14T10:00:00Z', actor: 'a', model_key: 'm', ...partial }
}

function moved(step: number, aspects: Record<string, unknown>, extra: Partial<ModifiedEntry> = {}): ModifiedEntry {
  return {
    old: { class: 'IfcDoor', step_id: step, guid: `g${step}`, name: `D${step}` },
    new: { class: 'IfcDoor', step_id: step, guid: `g${step}`, name: `D${step}` },
    aspects: { geometry: 'unchanged', data: 'unchanged', topology: 'unchanged', ...aspects },
    ...extra,
  }
}

describe('numbers', () => {
  test('counts and sizes carry thousands separators and scale units', () => {
    expect(formatCount(2071)).toBe('2,071')
    expect(formatCount(1_234_567)).toBe('1,234,567')
    expect(formatBytes(1_234_567_890_000)).toBe('1,234.6 GB')
    expect(formatBytes(999)).toBe('999 B')
    expect(formatBytes(12_345_678)).toBe('12.3 MB')
  })

  test('lengths scale mm to m and never use scientific notation', () => {
    expect(formatVectorMm([-214000, -299000, 1000])).toBe('-214.0, -299.0, 1.0 m')
    expect(formatVectorMm([2500, -3500, 0])).toBe('2.5, -3.5, 0.0 m')
    expect(formatVectorMm([12.3, -0, 0.04])).toBe('12.3, 0.0, 0.0 mm')
    expect(formatLengthMm(4301.2)).toBe('4.3012 m')
    expect(formatLengthMm(999.94)).toBe('999.9 mm')
    expect(formatLengthMm(1e9)).toBe('1,000,000.0 m')
  })

  test('property values read as plain text', () => {
    expect(formatPropertyValue(2.4)).toBe('2.4')
    expect(formatPropertyValue(12500)).toBe('12,500')
    expect(formatPropertyValue(1e-7)).toBe('0.0000001')
    expect(formatPropertyValue(1e21)).toBe('1,000,000,000,000,000,000,000')
    expect(formatPropertyValue(true)).toBe('true')
    expect(formatPropertyValue(null)).toBe('not set')
    expect(formatPropertyValue(['EI30', 'EI60'])).toBe('EI30, EI60')
  })

  test('times are shown in the given zone with its name', () => {
    const text = formatWhen('2026-11-14T10:05:00Z', 'Europe/Berlin')
    expect(text).toStartWith('Nov 14, 2026')
    expect(text).toEndWith('11:05 AM GMT+1')
  })
})

describe('reviewStates', () => {
  test('mirrors the store: decisions win over staleness, later baselines make reviews stale', () => {
    const lineage = [
      event({ id: 'e1', event: 'baseline_set' }),
      event({ id: 'e2' }),
      event({ id: 'e3' }),
      event({ id: 'e4' }),
      event({ id: 'e5', event: 'rejection', review_id: 'e3', actor: 'rita' }),
      event({ id: 'e6', event: 'approval', review_id: 'e2', actor: 'alice' }),
      event({ id: 'e7' }),
    ]
    const states = reviewStates(lineage)
    expect(states.get('e2')).toMatchObject({ state: 'approved', eventId: 'e6', actor: 'alice' })
    expect(states.get('e3')).toMatchObject({ state: 'rejected', eventId: 'e5', actor: 'rita' })
    expect(states.get('e4')).toMatchObject({ state: 'stale', eventId: 'e6' })
    expect(states.get('e7')).toEqual({ state: 'pending' })
    expect(states.has('e1')).toBe(false)
  })
})

describe('property deltas', () => {
  const entry = moved(
    8,
    { data: 'changed' },
    {
      property_deltas: {
        changed: [{ pset: 'Pset_DoorCommon', name: 'HeadHeight', inherited: false, old_value: 2.1, new_value: 2.4 }],
        added: [{ pset: 'Pset_DoorCommon', name: 'FireRating', inherited: true, new_value: 'EI30' }],
        removed: [{ pset: 'Identity Data', name: 'Host Id', inherited: false, old_value: 'L1' }],
        count_changed: [{ pset: 'Pset_DoorCommon', name: 'Reference', inherited: false, old_count: 1, new_count: 2 }],
      },
      data_delta: [
        { path: 'IfcPropertySingleValue.NominalValue[2]', old: 'IFCREAL: 2100000', new: 'IFCREAL: 2400000' },
        { path: 'IfcPropertySingleValue.Name[10]', old: 'Host Id', new: 'Head Height' },
        { path: 'IfcPropertySet[GlobalId=0abc].HasProperties', old: '[<ref>]', new: '[<ref>, <ref>]' },
        { path: 'IfcPropertySingleValue.Description[3]', old: null, new: '<null>' },
        { path: 'IfcPropertySingleValue.Unit[2]', old: '#5', new: '#12' },
        { path: 'IfcDoor.OverallHeight', old: '2100', new: '2400' },
      ],
    },
  )

  test('render as pset, name, before and after rows', () => {
    expect(propertyRows(entry).map((r) => `${r.kind}|${r.pset}, ${r.name}: ${r.before} to ${r.after}|${r.inherited}`)).toEqual([
      'changed|Pset_DoorCommon, HeadHeight: 2.1 to 2.4|false',
      'added|Pset_DoorCommon, FireRating: not set to EI30|true',
      'removed|Identity Data, Host Id: L1 to removed|false',
      'count|Pset_DoorCommon, Reference: 1x to 2x|false',
    ])
  })

})

describe('bulk moves', () => {
  const report: Report = {
    stats: {
      added: 1,
      deleted: 1,
      modified: 4,
      placement_cohorts: [{ delta_mm: [2500, -3500, 0], count: 3, members: [8, 24, 39] }],
    },
    added: [{ class: 'IfcWall', step_id: 90, guid: 'w' }],
    deleted: [{ class: 'IfcDoor', step_id: 84, guid: 'd5' }],
    modified: [
      moved(8, { data: 'changed', placement: 'changed', placement_delta_mm: [2500, -3500, 0] }),
      moved(24, { placement: 'changed', placement_delta_mm: [2500, -3500, 0] }),
      moved(39, { placement: 'changed', placement_delta_mm: [2500, -3500, 0] }),
      moved(50, { data: 'changed' }),
    ],
  }

  test('a cohort is one row whose members resolve to their entries', () => {
    const { cohorts, moveOnly } = placementCohorts(report)
    expect(cohorts).toHaveLength(1)
    expect(cohorts[0].count).toBe(3)
    expect(cohorts[0].members.map((m) => m.new.step_id)).toEqual([8, 24, 39])
    expect([...moveOnly].sort()).toEqual([24, 39])
  })

  test('move-only members leave the class groups; other changes stay listed', () => {
    const { moveOnly } = placementCohorts(report)
    const groups = groupChanges(report, moveOnly)
    expect(groups.map((g) => `${g.kind}:${g.className}`)).toEqual(['deleted:IfcDoor', 'added:IfcWall', 'modified:IfcDoor'])
    expect(groups[2].entries.map((e) => (e as ModifiedEntry).new.step_id)).toEqual([8, 50])
  })
})
