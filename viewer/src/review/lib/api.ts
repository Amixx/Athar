/**
 * Typed fetch client for the athar_review localhost API. Same-origin only;
 * every helper throws with the server's error message on non-2xx.
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

export interface ModifiedEntry {
  old: ReportEntity
  new: ReportEntity
  change_scope?: string
  aspects?: Record<string, unknown>
  match?: { reason?: string; score?: number }
  data_delta?: { path: string; old: string | null; new: string | null }[]
}

export interface Report {
  schemas?: { old?: string; new?: string }
  stats: Record<string, unknown>
  added: ReportEntity[]
  deleted: ReportEntity[]
  modified: ModifiedEntry[]
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { cache: 'no-store', ...init })
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = await response.json()
      if (body?.error) message = body.error
    } catch {
      /* non-JSON error body; keep the status line */
    }
    throw new Error(message)
  }
  return (await response.json()) as T
}

export const api = {
  overview: () => request<{ projects: ProjectSummary[] }>('/api/overview'),

  events: (projectId: string, modelKey?: string) =>
    request<{ events: StoreEvent[] }>(
      `/api/projects/${projectId}/events${modelKey ? `?model_key=${encodeURIComponent(modelKey)}` : ''}`,
    ),

  report: (projectId: string, reviewId: string) =>
    request<Report>(`/api/projects/${projectId}/reviews/${reviewId}`),

  uploadCandidate: (
    projectId: string,
    modelKey: string,
    kind: 'review' | 'baseline',
    file: File,
    actor: string,
  ) =>
    request<StoreEvent>(
      `/api/projects/${projectId}/models/${modelKey}/${kind}` +
        `?actor=${encodeURIComponent(actor)}&filename=${encodeURIComponent(file.name)}`,
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
    request<StoreEvent>(`/api/projects/${projectId}/models/${modelKey}/${action}`, {
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
