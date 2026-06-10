/**
 * Thin wrapper around @ifc-lite/geometry: bytes in, per-entity meshes out.
 * All tessellation happens in the browser (WASM); no Python-side geometry.
 *
 * The wrapper is the swap point if ifc-lite stalls — nothing outside this
 * module touches the library.
 */

import { GeometryProcessor } from '@ifc-lite/geometry'
import type { MeshData, Vec3 } from '@ifc-lite/geometry'

export interface ModelGeometry {
  /** expressId (STEP id) → mesh pieces for that entity. */
  byEntity: Map<number, MeshData[]>
  meshCount: number
  /**
   * RTC offset the WASM tessellator subtracted (IFC Z-up coordinates).
   * Feed it to the second model so both sides share one origin.
   */
  rtcOffset: Vec3 | null
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
  options: {
    sharedRtcOffset?: Vec3 | null
    onProgress?: (meshCount: number) => void
  } = {},
): Promise<ModelGeometry> {
  const processor = await getProcessor()
  const byEntity = new Map<number, MeshData[]>()
  let meshCount = 0
  let rtcOffset: Vec3 | null = null

  const bytes = new Uint8Array(buffer)
  const stream = processor.processStreaming(
    bytes,
    undefined,
    undefined,
    options.sharedRtcOffset ?? undefined,
  )
  for await (const event of stream) {
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
    }
  }
  return { byEntity, meshCount, rtcOffset }
}
