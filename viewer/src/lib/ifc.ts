/**
 * Wrapper around @ifc-lite/geometry: bytes in, per-entity meshes out. All
 * tessellation happens in the browser (WASM). Nothing outside this module
 * imports the library at runtime.
 */

import { GeometryProcessor } from '@ifc-lite/geometry'
import type { MeshData } from '@ifc-lite/geometry'
import { modelOrigin, type Vec3 } from './frame'

export interface ModelGeometry {
  /** expressId (STEP id) → mesh pieces for that entity. */
  byEntity: Map<number, MeshData[]>
  meshCount: number
  /** Scene-space (Y-up) position of the local frame the mesh positions are relative to. */
  origin: Vec3
}

let processorPromise: Promise<GeometryProcessor> | null = null

function getProcessor(): Promise<GeometryProcessor> {
  if (!processorPromise) {
    const processor = new GeometryProcessor()
    processorPromise = processor.init().then(() => processor)
  }
  return processorPromise
}

export async function tessellate(
  buffer: ArrayBuffer,
  options: { onProgress?: (meshCount: number) => void } = {},
): Promise<ModelGeometry> {
  const processor = await getProcessor()
  const byEntity = new Map<number, MeshData[]>()
  let meshCount = 0
  let rtcOffset: Vec3 | null = null
  let originShift: Vec3 | null = null

  for await (const event of processor.processStreaming(new Uint8Array(buffer))) {
    if (event.type === 'batch') {
      for (const mesh of event.meshes) {
        const list = byEntity.get(mesh.expressId)
        if (list) {
          list.push(mesh)
        } else {
          byEntity.set(mesh.expressId, [mesh])
        }
        meshCount += 1
      }
      options.onProgress?.(meshCount)
    } else if (event.type === 'rtcOffset' && event.hasRtc) {
      rtcOffset = event.rtcOffset
    } else if (event.type === 'complete') {
      originShift = event.coordinateInfo.originShift
    }
  }
  return { byEntity, meshCount, origin: modelOrigin(rtcOffset, originShift) }
}
