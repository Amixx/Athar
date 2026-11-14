/**
 * Deterministic state mirror for headless tests (and quick console poking).
 * The playwright smoke test asserts on `window.__athar_viewer` instead of
 * reading pixels, so it stays meaningful even where WebGL is unavailable.
 */

import type { Bounds, Vec3 } from './frame'
import type { Bucket, Appearance } from './renderState'
import type { Section, Side } from './types'

export interface ViewerDebugState {
  phase: 'empty' | 'loading' | 'ready' | 'error'
  error?: string
  warning?: string
  rendererReady?: boolean
  sliderT?: number
  /** Entities per visual bucket that actually have meshes. */
  bucketEntityCounts?: Partial<Record<Bucket, number>>
  /** Report entities per section that have no tessellated geometry. */
  meshlessBySection?: Partial<Record<Section, number>>
  reportStats?: Record<string, unknown>
  appearances?: Record<Bucket, Appearance>
  displacementLines?: number
  selection?: { side: string; stepId: number } | null
  /** Chain navigation: total steps, active index, and its label. */
  stepCount?: number
  activeStep?: number
  stepLabel?: string
  /** Translation added to each side's meshes to put both models in one frame. */
  frameTranslations?: Record<Side, Vec3>
  /** Scene-space bounds of one entity's meshes as drawn, whether or not its bucket renders that side. */
  entityBounds?: (side: Side, stepId: number) => Bounds | null
}

declare global {
  interface Window {
    __athar_viewer?: ViewerDebugState
  }
}

export function publishDebug(partial: Partial<ViewerDebugState>): void {
  if (typeof window === 'undefined') return
  window.__athar_viewer = { phase: 'empty', ...window.__athar_viewer, ...partial }
}
