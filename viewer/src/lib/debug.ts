/**
 * Deterministic state mirror for headless tests (and quick console poking).
 * The playwright smoke test asserts on `window.__athar_viewer` instead of
 * reading pixels, so it stays meaningful even where WebGL is unavailable.
 */

import type { Bucket, Appearance } from './renderState'
import type { Section } from './types'

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
