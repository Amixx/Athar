import { describe, expect, it } from 'bun:test'

import { formatRelationTargets } from '../src/lib/report'
import type { EntitySummary } from '../src/lib/types'

const storey = (overrides: Partial<EntitySummary>): EntitySummary => ({
  step_id: 7,
  guid: '2Fz0aBc9x1QvKq3mN5tY8w',
  name: 'Level 2',
  class: 'IfcBuildingStorey',
  entity_type: 'IfcBuildingStorey',
  ...overrides,
})

describe('formatRelationTargets', () => {
  it('shows an em dash when a side names no target', () => {
    expect(formatRelationTargets([])).toBe('—')
  })

  it('shows names, falling back to the GlobalId and then the step id', () => {
    expect(formatRelationTargets([storey({})])).toBe('Level 2')
    expect(formatRelationTargets([storey({ name: null })])).toBe('2Fz0aBc9x1QvKq3mN5tY8w')
    expect(formatRelationTargets([storey({ name: null, guid: null })])).toBe('#7')
  })

  it('joins several targets', () => {
    expect(formatRelationTargets([storey({}), storey({ name: 'Level 3', step_id: 8 })])).toBe('Level 2, Level 3')
  })
})
