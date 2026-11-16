import { describe, expect, it } from 'bun:test'

import type { MeshData } from '@ifc-lite/geometry'
import { entityBounds, frameTranslations, ifcToScene, modelOrigin, type Vec3 } from '../src/lib/frame'

function add(a: Vec3, b: Vec3): Vec3 {
  return { x: a.x + b.x, y: a.y + b.y, z: a.z + b.z }
}

function sub(a: Vec3, b: Vec3): Vec3 {
  return { x: a.x - b.x, y: a.y - b.y, z: a.z - b.z }
}

function mesh(positions: number[]): MeshData {
  return {
    expressId: 1,
    positions: new Float32Array(positions),
    normals: new Float32Array(positions.length),
    indices: new Uint32Array(),
    color: [1, 1, 1, 1],
  }
}

describe('ifcToScene', () => {
  it('maps IFC Z-up onto three.js Y-up', () => {
    expect(ifcToScene({ x: 1, y: 2, z: 3 })).toEqual({ x: 1, y: 3, z: -2 })
  })
})

describe('modelOrigin', () => {
  it('converts the Z-up RTC offset to scene axes and adds the Y-up origin shift', () => {
    expect(modelOrigin({ x: 500005, y: 6000004, z: 100 }, { x: 1, y: 2, z: 3 })).toEqual({
      x: 500006,
      y: 102,
      z: -6000001,
    })
  })

  it('is the scene origin when nothing was subtracted', () => {
    expect(modelOrigin(null, null)).toEqual({ x: 0, y: 0, z: 0 })
  })
})

describe('frameTranslations', () => {
  it('puts one world point at the same scene position in both models', () => {
    const world = ifcToScene({ x: 500003.25, y: 6000011.5, z: 102 })
    const oldOrigin = modelOrigin({ x: 500005, y: 6000004, z: 100 }, null)
    const newOrigin = modelOrigin({ x: 500002.5, y: 6000020, z: 97 }, null)
    const translations = frameTranslations(oldOrigin, newOrigin)

    const oldDrawn = add(sub(world, oldOrigin), translations.old)
    const newDrawn = add(sub(world, newOrigin), translations.new)
    expect(newDrawn.x).toBeCloseTo(oldDrawn.x, 9)
    expect(newDrawn.y).toBeCloseTo(oldDrawn.y, 9)
    expect(newDrawn.z).toBeCloseTo(oldDrawn.z, 9)
    expect(translations.old).toEqual({ x: 0, y: 0, z: 0 })
    expect(translations.new).toEqual({ x: -2.5, y: -3, z: -16 })
  })
})

describe('entityBounds', () => {
  it('bounds every mesh of an entity after translation', () => {
    const bounds = entityBounds([mesh([0, 0, 0, 1, 2, 3]), mesh([-1, 5, 0])], { x: 10, y: 0, z: -1 })
    expect(bounds).toEqual({ min: [9, 0, -1], max: [11, 5, 2] })
  })

  it('is null for meshes without vertices', () => {
    expect(entityBounds([mesh([])], { x: 0, y: 0, z: 0 })).toBeNull()
  })
})
