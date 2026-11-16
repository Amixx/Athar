import { describe, expect, test } from 'bun:test'

import type { Report, StoreEvent } from '../src/review/lib/api'
import {
  formatBytes,
  groupChanges,
  isCleanDiff,
  modifiedAspects,
  placementDeltaMm,
  propertyRows,
  revisionLetter,
  revisionLetters,
  statSummary,
} from '../src/review/lib/format'

function event(partial: Partial<StoreEvent>): StoreEvent {
  return { id: 'evt-000001', event: 'review', at: '', actor: '', model_key: 'm', ...partial }
}

describe('statSummary', () => {
  test('extracts counts and defaults missing keys to zero', () => {
    expect(statSummary({ added: 2, modified: 1 })).toEqual({
      added: 2,
      deleted: 0,
      modified: 1,
      unchanged: 0,
    })
    expect(isCleanDiff({ added: 0, deleted: 0, modified: 0, unchanged: 9 })).toBe(true)
    expect(isCleanDiff({ added: 1 })).toBe(false)
  })
})

describe('formatBytes', () => {
  test('scales units without scientific notation', () => {
    expect(formatBytes(435_400_000)).toBe('435.4 MB')
    expect(formatBytes(1_500)).toBe('1.5 kB')
    expect(formatBytes(12)).toBe('12 B')
    expect(formatBytes(2_100_000_000)).toBe('2.1 GB')
  })
})

describe('revision letters', () => {
  test('advance on baseline_set and approval only, A..Z then AA', () => {
    const events = [
      event({ id: 'e1', event: 'baseline_set' }),
      event({ id: 'e2', event: 'review' }),
      event({ id: 'e3', event: 'approval', review_id: 'e2' }),
      event({ id: 'e4', event: 'rejection', review_id: 'e2' }),
      event({ id: 'e5', event: 'approval', review_id: 'e2' }),
    ]
    const letters = revisionLetters(events)
    expect(letters.get('e1')).toBe('A')
    expect(letters.get('e3')).toBe('B')
    expect(letters.get('e5')).toBe('C')
    expect(letters.has('e2')).toBe(false)
    expect(letters.has('e4')).toBe(false)
    expect(revisionLetter(25)).toBe('Z')
    expect(revisionLetter(26)).toBe('AA')
  })
})

describe('groupChanges', () => {
  const report: Report = {
    stats: {},
    added: [
      { class: 'IfcDoor', step_id: 1, guid: 'g1' },
      { class: 'IfcDoor', step_id: 2, guid: 'g2' },
      { class: 'IfcWall', step_id: 3, guid: 'g3' },
    ],
    deleted: [{ class: 'IfcWindow', step_id: 4, guid: 'g4' }],
    modified: [
      {
        old: { class: 'IfcWall', step_id: 5, guid: 'g5' },
        new: { class: 'IfcWall', step_id: 6, guid: 'g5' },
        change_scope: 'intrinsic',
        aspects: { data: 'changed', geometry: 'unchanged' },
      },
    ],
  }

  test('groups by kind and class, deletions first, larger groups first', () => {
    const groups = groupChanges(report)
    expect(groups.map((g) => `${g.kind}:${g.className}:${g.entries.length}`)).toEqual([
      'deleted:IfcWindow:1',
      'added:IfcDoor:2',
      'added:IfcWall:1',
      'modified:IfcWall:1',
    ])
  })
})

test('changed property values retain distinguishing precision', () => {
  const rows = propertyRows({
    old: { class: 'IfcDoor', step_id: 1 },
    new: { class: 'IfcDoor', step_id: 2 },
    property_deltas: {
      changed: [
        { pset: 'Dimensions', name: 'Height', old_value: 12.340000001, new_value: 12.34 },
        { pset: 'Dimensions', name: 'Tiny', old_value: 1.234567891e-9, new_value: 1.234567892e-9 },
        { pset: 'Dimensions', name: 'List', old_value: [12.340000001], new_value: [12.34] },
      ],
    },
  })
  expect(rows.map(({ before, after }) => [before, after])).toEqual([
    ['12.340000001', '12.34'],
    ['0.000000001234567891', '0.000000001234567892'],
    ['12.340000001', '12.34'],
  ])
})

describe('modified helpers', () => {
  test('lists changed aspects and computes placement delta norm', () => {
    const entry = {
      old: { class: 'IfcWall', step_id: 1, guid: 'g' },
      new: { class: 'IfcWall', step_id: 2, guid: 'g' },
      aspects: {
        data: 'changed',
        placement: 'changed',
        geometry: 'unchanged',
        placement_delta_mm: [3, 4, 0],
      },
    }
    expect(modifiedAspects(entry)).toEqual(['data', 'position'])
    expect(placementDeltaMm(entry)).toBe(5)
    expect(placementDeltaMm({ ...entry, aspects: {} })).toBeNull()
    expect(placementDeltaMm({ ...entry, aspects: { placement_delta_mm: [0, 0, 0] } })).toBeNull()
  })
})
