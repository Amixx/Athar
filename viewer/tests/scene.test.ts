import { expect, test } from 'bun:test'
import * as THREE from 'three'
import { createBucketObject } from '../src/lib/scene'
import { bucketFor, isVoidingFeature } from '../src/lib/renderState'
import type { EntityRecord } from '../src/lib/report'

const opening: EntityRecord = {
  side: 'new', stepId: 42, guid: null, name: 'Void', klass: 'IfcOpeningElement',
  entityType: 'IFCOPENINGELEMENT', section: 'unchanged',
}

test('unchanged openings have no drawable bucket', () => {
  expect(bucketFor('new', opening)).toBeNull()
  expect(bucketFor('old', opening)).toBeNull()
})

test('voiding subtypes preserve change sides and suppress unreported voids', () => {
  for (const klass of ['IfcOpeningElement', 'IfcOpeningStandardCase', 'IFCVOIDINGFEATURE']) {
    expect(isVoidingFeature(klass)).toBe(true)
    expect(bucketFor('new', { ...opening, klass })).toBeNull()
    expect(bucketFor('new', undefined, klass)).toBeNull()
    expect(bucketFor('old', { ...opening, klass, section: 'deleted' })).toBe('deleted')
    expect(bucketFor('new', { ...opening, klass, section: 'deleted' })).toBeNull()
    expect(bucketFor('new', { ...opening, klass, section: 'added' })).toBe('added')
    expect(bucketFor('old', { ...opening, klass, section: 'added' })).toBeNull()
    expect(bucketFor('new', { ...opening, klass, section: 'modified' })).toBe('modifiedStatic')
  }
  expect(bucketFor('new', undefined, 'IFCWALL')).toBe('extra')
})

test('changed openings use line segments without filled faces', () => {
  const source = new THREE.BoxGeometry(2, 3, 5)
  const edges = new THREE.EdgesGeometry(source)
  const object = createBucketObject(edges, true)
  source.dispose()
  expect(object).toBeInstanceOf(THREE.LineSegments)
  expect(object).not.toBeInstanceOf(THREE.Mesh)
  expect(object.geometry.getAttribute('position').count).toBe(24)
  object.geometry.dispose()
  object.material.dispose()
})

test('ordinary classes retain filled geometry and unchanged buckets', () => {
  expect(bucketFor('new', { ...opening, klass: 'IfcWall', entityType: 'IFCWALL' })).toBe('unchanged')
  const source = new THREE.BoxGeometry(2, 3, 5)
  const object = createBucketObject(source, false)
  expect(object).toBeInstanceOf(THREE.Mesh)
  expect(object.geometry).toBe(source)
  object.geometry.dispose()
  object.material.dispose()
})
