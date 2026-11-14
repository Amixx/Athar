/**
 * Typed fetch client for the athar_review localhost API. Same-origin only;
 * every helper throws an ApiError carrying the server's error code on non-2xx.
 */

export interface Artifact {
  sha256: string
  schema: string | null
  size_bytes: number
  encoding?: string
  stored_bytes?: number
}

export interface Baseline {
  model_key: string
  artifact: Artifact
  approved_by: string
  approved_at: string
}

export interface ModelSummary {
  model_key: string
  baseline: Baseline
  pending_count: number
  last_event_at?: string
}

export interface ProjectSummary {
  project_id: string
  name: string
  created: string
  event_count: number
  models: ModelSummary[]
}

export interface Overview {
  projects: ProjectSummary[]
  max_upload_bytes: number
}

export interface Conflict {
  review_id: string
  actor?: string
  overlap_count: number
  overlap_guids: string[]
}

export interface StoreEvent {
  id: string
  event: 'baseline_set' | 'review' | 'approval' | 'rejection'
  at: string
  actor: string
  model_key: string
  source_name?: string | null
  athar_version?: string
  canon_version?: string
  verdict?: 'pass' | 'fail'
  has_policy?: boolean
  note?: string | null
  review_id?: string
  artifact?: Artifact
  baseline_artifact?: Artifact
  candidate_artifact?: Artifact
  conflicts?: Conflict[]
  check?: { ok: boolean; violations: { code: string; message: string }[] }
  diff?: { schemas?: { old?: string; new?: string }; stats?: Record<string, number> }
}

export interface ReportEntity {
  class: string
  step_id: number
  guid: string | null
  name?: string | null
}

export type PropertyValue = string | number | boolean | null | PropertyValue[]

export interface PropertyDelta {
  pset: string
  name: string
  inherited?: boolean
  old_value?: PropertyValue
  new_value?: PropertyValue
  old_count?: number
  new_count?: number
}

export interface PropertyDeltas {
  added?: PropertyDelta[]
  removed?: PropertyDelta[]
  changed?: PropertyDelta[]
  count_changed?: PropertyDelta[]
}

export interface ModifiedEntry {
  old: ReportEntity
  new: ReportEntity
  change_scope?: string
  aspects?: Record<string, unknown>
  match?: { reason?: string; score?: number }
  data_delta?: { path: string; old: string | null; new: string | null }[]
  property_deltas?: PropertyDeltas
}

export interface PlacementCohort {
  delta_mm: [number, number, number]
  count: number
  members: number[]
}

export interface Report {
  schemas?: { old?: string; new?: string }
  stats: Record<string, unknown> & { placement_cohorts?: PlacementCohort[] }
  added: ReportEntity[]
  deleted: ReportEntity[]
  modified: ModifiedEntry[]
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly detail: Record<string, unknown> = {},
  ) {
    super(message)
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  let response: Response
  try {
    response = await fetch(url, { cache: 'no-store', ...init })
  } catch {
    throw new ApiError(0, 'unreachable', 'The review server is not reachable. Is `athar review` still running?')
  }
  if (!response.ok) {
    let body: unknown = null
    try {
      body = await response.json()
    } catch {
      body = null
    }
    const error = (body as { error?: { code?: unknown; message?: unknown } } | null)?.error
    if (error && typeof error.code === 'string' && typeof error.message === 'string') {
      const { code, message, ...detail } = error as Record<string, unknown>
      throw new ApiError(response.status, code as string, message as string, detail)
    }
    throw new ApiError(response.status, 'http_error', `The server answered ${response.status} ${response.statusText}.`)
  }
  return (await response.json()) as T
}

const segment = encodeURIComponent

export const api = {
  overview: () => request<Overview>('/api/overview'),

  events: (projectId: string, modelKey?: string) =>
    request<{ events: StoreEvent[] }>(
      `/api/projects/${segment(projectId)}/events${modelKey ? `?model_key=${segment(modelKey)}` : ''}`,
    ),

  report: (projectId: string, reviewId: string) =>
    request<Report>(`/api/projects/${segment(projectId)}/reviews/${segment(reviewId)}`),

  upload: (projectId: string, modelKey: string, kind: 'review' | 'baseline', file: File, actor: string) =>
    request<StoreEvent>(
      `/api/projects/${segment(projectId)}/models/${segment(modelKey)}/${kind}` +
        `?actor=${segment(actor)}&filename=${segment(file.name)}`,
      { method: 'POST', body: file, headers: { 'Content-Type': 'application/x-step' } },
    ),

  resolve: (
    projectId: string,
    modelKey: string,
    action: 'approve' | 'reject',
    reviewId: string,
    actor: string,
    note?: string,
  ) =>
    request<StoreEvent>(`/api/projects/${segment(projectId)}/models/${segment(modelKey)}/${action}`, {
      method: 'POST',
      body: JSON.stringify({ review_id: reviewId, actor, note }),
      headers: { 'Content-Type': 'application/json' },
    }),
}

/** URL that opens the recorded review in the 3D diff viewer (index.html SPA). */
export function viewerUrl(projectId: string, reviewId: string): string {
  const src = `${window.location.origin}/viewer/${projectId}/${reviewId}`
  return `/index.html?src=${encodeURIComponent(src)}`
}
