/**
 * Coordinate-frame arithmetic for overlaying two tessellated models. Pure, no
 * three.js.
 *
 * The tessellator returns each model's positions in its own local frame: it
 * subtracts a per-file RTC offset (IFC Z-up) and may subtract a further origin
 * shift on the emitted Y-up positions. Scene coordinates are three.js Y-up,
 * where IFC (x, y, z) maps to (x, z, -y).
 */

import type { MeshData } from '@ifc-lite/geometry'
import type { Side } from './types'

export interface Vec3 {
  x: number
  y: number
  z: number
}

export interface Bounds {
  min: [number, number, number]
  max: [number, number, number]
}

const ZERO: Vec3 = { x: 0, y: 0, z: 0 }

export function ifcToScene(point: Vec3): Vec3 {
  return { x: point.x, y: point.z, z: -point.y }
}

/** Scene-space position of a model's local origin. */
export function modelOrigin(rtcOffset: Vec3 | null, originShift: Vec3 | null): Vec3 {
  const rtc = rtcOffset ? ifcToScene(rtcOffset) : ZERO
  const shift = originShift ?? ZERO
  return { x: rtc.x + shift.x, y: rtc.y + shift.y, z: rtc.z + shift.z }
}

/**
 * Per-side translation that moves both models into the old model's local
 * frame: the old side stays put, the new side moves by the difference of the
 * two origins.
 */
export function frameTranslations(oldOrigin: Vec3, newOrigin: Vec3): Record<Side, Vec3> {
  return {
    old: ZERO,
    new: {
      x: newOrigin.x - oldOrigin.x,
      y: newOrigin.y - oldOrigin.y,
      z: newOrigin.z - oldOrigin.z,
    },
  }
}

/** Axis-aligned bounds of an entity's meshes after translation, or null without vertices. */
export function entityBounds(meshes: MeshData[], translation: Vec3): Bounds | null {
  const min: [number, number, number] = [Infinity, Infinity, Infinity]
  const max: [number, number, number] = [-Infinity, -Infinity, -Infinity]
  const t = [translation.x, translation.y, translation.z]
  let vertices = 0
  for (const mesh of meshes) {
    const p = mesh.positions
    for (let i = 0; i < p.length; i += 3) {
      for (let k = 0; k < 3; k++) {
        const v = p[i + k] + t[k]
        if (v < min[k]) min[k] = v
        if (v > max[k]) max[k] = v
      }
      vertices += 1
    }
  }
  return vertices > 0 ? { min, max } : null
}
