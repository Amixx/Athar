/**
 * Deterministic state mirror for headless tests (and quick console poking).
 * The playwright smoke test asserts on `window.__athar_viewer` instead of
 * reading pixels, so it stays meaningful even where WebGL is unavailable.
 */

import type { Bounds, Vec3 } from './frame'
import type { Appearance, Bucket, ReportCoverage } from './renderState'
import type { Side } from './types'

export interface ViewerDebugState {
  phase: 'empty' | 'confirm' | 'loading' | 'ready' | 'error'
  error?: string
  warning?: string
  rendererReady?: boolean
  sliderT?: number
  /** Entities per visual bucket that actually have meshes. */
  bucketEntityCounts?: Partial<Record<Bucket, number>>
  reportCoverage?: ReportCoverage | null
  reportStats?: Record<string, unknown>
  appearances?: Record<Bucket, Appearance>
  displacementLines?: number
  selection?: { side: string; stepId: number } | null
  /** three.js uuids of the visible highlight overlay's geometry and material. */
  highlightObjects?: { geometry: string; material: string } | null
  /** Chain navigation: total steps, active index, and its label. */
  stepCount?: number
  activeStep?: number
  stepLabel?: string
  /** Translation added to each side's meshes to put both models in one frame. */
  frameTranslations?: Record<Side, Vec3>
  /** Scene-space bounds of one entity's meshes as drawn, whether or not its bucket renders that side. */
  entityBounds?: (side: Side, stepId: number) => Bounds | null
  /** Scene-space bounds of one entity as the scene built it, or null when that side is not drawn. */
  drawnBounds?: (side: Side, stepId: number) => Bounds | null
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
