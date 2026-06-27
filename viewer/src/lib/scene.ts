/**
 * Imperative three.js scene for the diff overlay. Lives entirely outside
 * Svelte reactivity — the UI talks to it through this class only.
 *
 * Rendering model: entities are merged into one THREE.Mesh per visual bucket
 * (per-entity colors are uniform within a bucket), so a slider drag updates a
 * handful of materials instead of touching entities. Per-entity identity is
 * kept as index ranges into the merged buffers for picking and highlighting.
 *
 * Renderer note: the plan's primary was @ifc-lite/renderer, but its per-entity
 * color override path (`Scene.setColorOverrides`) is internal plumbing that
 * needs GPU device/pipeline handles, and its federation registry assumes one
 * model namespace while we overlay two files with colliding STEP ids. ifc-lite
 * still does all parsing/tessellation; meshes are fed to three.js exactly as
 * the spec's sanctioned fallback. This class is the only module that knows.
 */

import * as THREE from 'three'
import { OrbitControls } from 'three/addons/controls/OrbitControls.js'
import type { MeshData } from '@ifc-lite/geometry'
import type { ModelGeometry } from './ifc'
import { BUCKET_SIDE, type Appearance, type Bucket } from './renderState'
import type { Side } from './types'

export interface PickHit {
  side: Side
  stepId: number
  bucket: Bucket
}

export interface SceneStats {
  bucketEntityCounts: Partial<Record<Bucket, number>>
  displacementLines: number
}

interface EntityRange {
  /** Offset into the merged index buffer (in indices, not triangles). */
  start: number
  count: number
  stepId: number
}

interface BucketEntry {
  bucket: Bucket
  mesh: THREE.Mesh
  material: THREE.MeshLambertMaterial
  ranges: EntityRange[]
}

interface EntityLocation {
  bucket: Bucket
  start: number
  count: number
}

const HIGHLIGHT_COLOR = 0xf5d90a
const LINE_COLOR = 0xf5a524
const ARROW_HEAD_FRACTION = 0.18
const ARROW_HEAD_MIN = 0.25

export class DiffScene {
  private renderer: THREE.WebGLRenderer
  private scene = new THREE.Scene()
  private camera: THREE.PerspectiveCamera
  private controls: OrbitControls
  private raycaster = new THREE.Raycaster()
  private observer: ResizeObserver
  private rafId = 0
  private needsRender = true

  private buckets = new Map<Bucket, BucketEntry>()
  private entityLocations: Record<Side, Map<number, EntityLocation>> = {
    old: new Map(),
    new: new Map(),
  }
  private entityBoxes: Record<Side, Map<number, THREE.Box3>> = {
    old: new Map(),
    new: new Map(),
  }
  private modelBox = new THREE.Box3()
  private changesBox = new THREE.Box3()
  private displacement: THREE.Group | null = null
  private highlight: THREE.Mesh | null = null

  constructor(private canvas: HTMLCanvasElement) {
    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true })
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))

    this.camera = new THREE.PerspectiveCamera(55, 1, 0.1, 5000)
    this.camera.position.set(15, 12, 15)

    this.controls = new OrbitControls(this.camera, canvas)
    this.controls.enableDamping = true
    this.controls.dampingFactor = 0.12
    // OrbitControls applies wheel zoom synchronously inside its event handler.
    // By the next animation frame `controls.update()` may report no movement,
    // so mark the scene dirty on every control change instead of relying only
    // on the render loop's damping return value.
    this.controls.addEventListener('change', () => this.invalidate())

    const hemi = new THREE.HemisphereLight(0xffffff, 0x39424e, 1.1)
    const key = new THREE.DirectionalLight(0xffffff, 1.6)
    key.position.set(1, 1.6, 0.8)
    const fill = new THREE.DirectionalLight(0xa8c0ff, 0.4)
    fill.position.set(-1, 0.4, -0.9)
    this.scene.add(hemi, key, fill)

    const host = canvas.parentElement ?? canvas
    this.observer = new ResizeObserver(() => this.resize())
    this.observer.observe(host)
    this.resize()

    const loop = () => {
      this.rafId = requestAnimationFrame(loop)
      const moved = this.controls.update()
      if (this.needsRender || moved) {
        this.renderer.render(this.scene, this.camera)
        this.needsRender = false
      }
    }
    loop()
  }

  invalidate(): void {
    this.needsRender = true
  }

  private resize(): void {
    const host = this.canvas.parentElement ?? this.canvas
    const width = Math.max(1, host.clientWidth)
    const height = Math.max(1, host.clientHeight)
    this.renderer.setSize(width, height, false)
    this.camera.aspect = width / height
    this.camera.updateProjectionMatrix()
    this.invalidate()
  }

  /**
   * Build the bucket meshes for an old/new model pair.
   * `bucketOf` decides each entity's bucket (or null = not rendered).
   * `displacementPairs` are placement-only modifications (old/new step ids).
   */
  setModels(
    oldGeometry: ModelGeometry,
    newGeometry: ModelGeometry,
    bucketOf: (side: Side, stepId: number) => Bucket | null,
    displacementPairs: Array<{ oldId: number; newId: number }>,
  ): SceneStats {
    this.clearModels()

    const grouped = new Map<Bucket, Array<{ stepId: number; meshes: MeshData[] }>>()
    const collect = (side: Side, geometry: ModelGeometry) => {
      for (const [stepId, meshes] of geometry.byEntity) {
        const bucket = bucketOf(side, stepId)
        if (bucket === null || BUCKET_SIDE[bucket] !== side) continue
        let list = grouped.get(bucket)
        if (!list) grouped.set(bucket, (list = []))
        list.push({ stepId, meshes })
      }
    }
    collect('old', oldGeometry)
    collect('new', newGeometry)

    const counts: Partial<Record<Bucket, number>> = {}
    for (const [bucket, entities] of grouped) {
      const entry = this.buildBucket(bucket, entities)
      this.buckets.set(bucket, entry)
      this.scene.add(entry.mesh)
      counts[bucket] = entities.length
      entry.mesh.geometry.computeBoundingBox()
      const box = entry.mesh.geometry.boundingBox
      if (box && !box.isEmpty()) {
        this.modelBox.union(box)
        if (bucket !== 'unchanged' && bucket !== 'extra') this.changesBox.union(box)
      }
    }

    const lineCount = this.buildDisplacementLines(displacementPairs)
    this.fitBox(this.modelBox)
    this.invalidate()
    return { bucketEntityCounts: counts, displacementLines: lineCount }
  }

  private buildBucket(
    bucket: Bucket,
    entities: Array<{ stepId: number; meshes: MeshData[] }>,
  ): BucketEntry {
    const side = BUCKET_SIDE[bucket]
    let vertexFloats = 0
    let indexCount = 0
    for (const entity of entities) {
      for (const mesh of entity.meshes) {
        vertexFloats += mesh.positions.length
        indexCount += mesh.indices.length
      }
    }

    const positions = new Float32Array(vertexFloats)
    const normals = new Float32Array(vertexFloats)
    const indices = new Uint32Array(indexCount)
    const ranges: EntityRange[] = []

    let vertexOffset = 0 // in vertices
    let indexOffset = 0
    for (const entity of entities) {
      const start = indexOffset
      const box = new THREE.Box3()
      for (const mesh of entity.meshes) {
        positions.set(mesh.positions, vertexOffset * 3)
        normals.set(mesh.normals, vertexOffset * 3)
        for (let i = 0; i < mesh.indices.length; i++) {
          indices[indexOffset + i] = mesh.indices[i] + vertexOffset
        }
        for (let i = 0; i < mesh.positions.length; i += 3) {
          expandBox(box, mesh.positions[i], mesh.positions[i + 1], mesh.positions[i + 2])
        }
        vertexOffset += mesh.positions.length / 3
        indexOffset += mesh.indices.length
      }
      const count = indexOffset - start
      ranges.push({ start, count, stepId: entity.stepId })
      this.entityLocations[side].set(entity.stepId, { bucket, start, count })
      this.entityBoxes[side].set(entity.stepId, box)
    }

    const geometry = new THREE.BufferGeometry()
    geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
    geometry.setAttribute('normal', new THREE.BufferAttribute(normals, 3))
    geometry.setIndex(new THREE.BufferAttribute(indices, 1))

    const material = new THREE.MeshLambertMaterial({ side: THREE.DoubleSide })
    const mesh = new THREE.Mesh(geometry, material)
    mesh.name = `bucket:${bucket}`
    mesh.userData.bucket = bucket
    return { bucket, mesh, material, ranges }
  }

  private buildDisplacementLines(pairs: Array<{ oldId: number; newId: number }>): number {
    const group = new THREE.Group()
    group.name = 'placement-displacement-arrows'

    let count = 0
    for (const pair of pairs) {
      const from = this.entityBoxes.old.get(pair.oldId)
      const to = this.entityBoxes.new.get(pair.newId)
      if (!from || !to || from.isEmpty() || to.isEmpty()) continue
      const a = from.getCenter(new THREE.Vector3())
      const b = to.getCenter(new THREE.Vector3())
      const delta = b.clone().sub(a)
      const length = delta.length()
      if (length <= 1e-9) continue

      const headLength = Math.min(length * ARROW_HEAD_FRACTION, Math.max(ARROW_HEAD_MIN, length * 0.35))
      const arrow = new THREE.ArrowHelper(
        delta.normalize(),
        a,
        length,
        LINE_COLOR,
        headLength,
        headLength * 0.45,
      )
      arrow.renderOrder = 998
      arrow.traverse((object) => {
        const material = (object as THREE.Mesh | THREE.Line).material
        if (material instanceof THREE.Material) {
          material.depthTest = false
          material.transparent = true
          material.opacity = 0.92
        }
      })
      group.add(arrow)
      count += 1
    }
    if (count === 0) return 0
    this.displacement = group
    this.scene.add(this.displacement)
    return count
  }

  applyAppearances(appearances: Record<Bucket, Appearance>, showLines: boolean): void {
    for (const [bucket, entry] of this.buckets) {
      const appearance = appearances[bucket]
      entry.mesh.visible = appearance.visible
      entry.material.color.set(appearance.color)
      entry.material.emissive.set(appearance.color)
      entry.material.emissiveIntensity = bucket === 'unchanged' || bucket === 'extra' ? 0.28 : 0.06
      entry.material.opacity = appearance.opacity
      const solid = appearance.opacity >= 0.999
      entry.material.transparent = !solid
      entry.material.depthWrite = solid
      entry.material.needsUpdate = true
    }
    if (this.displacement) this.displacement.visible = showLines
    this.invalidate()
  }

  pick(clientX: number, clientY: number): PickHit | null {
    const rect = this.canvas.getBoundingClientRect()
    const ndc = new THREE.Vector2(
      ((clientX - rect.left) / rect.width) * 2 - 1,
      -((clientY - rect.top) / rect.height) * 2 + 1,
    )
    this.raycaster.setFromCamera(ndc, this.camera)
    const targets = [...this.buckets.values()].filter((b) => b.mesh.visible).map((b) => b.mesh)
    const hits = this.raycaster.intersectObjects(targets, false)
    for (const hit of hits) {
      if (hit.faceIndex === undefined || hit.faceIndex === null) continue
      const bucket = hit.object.userData.bucket as Bucket
      const entry = this.buckets.get(bucket)
      if (!entry) continue
      const range = findRange(entry.ranges, hit.faceIndex * 3)
      if (!range) continue
      return { side: BUCKET_SIDE[bucket], stepId: range.stepId, bucket }
    }
    return null
  }

  /** Overlay highlight for one entity (zero-copy: reuses merged buffers). */
  setHighlight(target: { side: Side; stepId: number } | null): void {
    if (this.highlight) {
      this.scene.remove(this.highlight)
      ;(this.highlight.material as THREE.Material).dispose()
      this.highlight.geometry.dispose()
      this.highlight = null
    }
    if (target) {
      const location = this.entityLocations[target.side].get(target.stepId)
      const entry = location && this.buckets.get(location.bucket)
      if (location && entry) {
        const source = entry.mesh.geometry
        const geometry = new THREE.BufferGeometry()
        geometry.setAttribute('position', source.getAttribute('position'))
        geometry.setAttribute('normal', source.getAttribute('normal'))
        const sourceIndex = source.getIndex()!.array as Uint32Array
        geometry.setIndex(
          new THREE.BufferAttribute(sourceIndex.subarray(location.start, location.start + location.count), 1),
        )
        const material = new THREE.MeshBasicMaterial({
          color: HIGHLIGHT_COLOR,
          transparent: true,
          opacity: 0.35,
          depthTest: false,
          side: THREE.DoubleSide,
        })
        this.highlight = new THREE.Mesh(geometry, material)
        this.highlight.renderOrder = 999
        this.scene.add(this.highlight)
      }
    }
    this.invalidate()
  }

  entityBox(side: Side, stepId: number): THREE.Box3 | undefined {
    return this.entityBoxes[side].get(stepId)
  }

  fitModel(): void {
    this.fitBox(this.modelBox)
  }

  fitChanges(): void {
    this.fitBox(this.changesBox.isEmpty() ? this.modelBox : this.changesBox)
  }

  fitBox(box: THREE.Box3): void {
    if (box.isEmpty()) return
    const center = box.getCenter(new THREE.Vector3())
    const sphere = box.getBoundingSphere(new THREE.Sphere())
    const radius = Math.max(sphere.radius, 0.5)
    const distance = radius / Math.sin(THREE.MathUtils.degToRad(this.camera.fov / 2))
    const direction = this.camera.position.clone().sub(this.controls.target)
    if (direction.lengthSq() < 1e-9) direction.set(1, 0.8, 1)
    direction.normalize()
    this.camera.position.copy(center).addScaledVector(direction, distance * 1.15)
    this.camera.near = Math.max(radius / 500, 0.01)
    this.camera.far = Math.max(distance * 20, 100)
    this.camera.updateProjectionMatrix()
    this.controls.target.copy(center)
    this.controls.update()
    this.invalidate()
  }

  private clearModels(): void {
    this.setHighlight(null)
    for (const entry of this.buckets.values()) {
      this.scene.remove(entry.mesh)
      entry.mesh.geometry.dispose()
      entry.material.dispose()
    }
    this.buckets.clear()
    this.entityLocations.old.clear()
    this.entityLocations.new.clear()
    this.entityBoxes.old.clear()
    this.entityBoxes.new.clear()
    this.modelBox.makeEmpty()
    this.changesBox.makeEmpty()
    if (this.displacement) {
      this.scene.remove(this.displacement)
      this.displacement.traverse((object) => {
        const mesh = object as THREE.Mesh | THREE.Line
        mesh.geometry?.dispose()
        const material = mesh.material
        if (Array.isArray(material)) {
          for (const item of material) item.dispose()
        } else {
          material?.dispose()
        }
      })
      this.displacement = null
    }
  }

  dispose(): void {
    cancelAnimationFrame(this.rafId)
    this.observer.disconnect()
    this.clearModels()
    this.controls.dispose()
    this.renderer.dispose()
  }
}

function expandBox(box: THREE.Box3, x: number, y: number, z: number): void {
  if (x < box.min.x) box.min.x = x
  if (y < box.min.y) box.min.y = y
  if (z < box.min.z) box.min.z = z
  if (x > box.max.x) box.max.x = x
  if (y > box.max.y) box.max.y = y
  if (z > box.max.z) box.max.z = z
}

/** Binary search the (start-sorted, disjoint) ranges for an index position. */
function findRange(ranges: EntityRange[], indexPosition: number): EntityRange | null {
  let lo = 0
  let hi = ranges.length - 1
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    const range = ranges[mid]
    if (indexPosition < range.start) hi = mid - 1
    else if (indexPosition >= range.start + range.count) lo = mid + 1
    else return range
  }
  return null
}
