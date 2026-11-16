<script lang="ts">
  import type { ProjectSummary, Report, StoreEvent } from './lib/api'
  import { api } from './lib/api'
  import Register from './components/Register.svelte'
  import ReviewDetail from './components/ReviewDetail.svelte'
  import Sidebar from './components/Sidebar.svelte'
  import UploadZone from './components/UploadZone.svelte'
  import { formatBytes, formatWhen, shortSha } from './lib/format'

  let projects = $state<ProjectSummary[]>([])
  let loadError = $state<string | null>(null)
  let selectedProject = $state<string | null>(null)
  let selectedModel = $state<string | null>(null)
  let openReviewId = $state<string | null>(null)
  let events = $state<StoreEvent[]>([])
  let report = $state<Report | null>(null)
  let reportError = $state<string | null>(null)
  let actionError = $state<string | null>(null)
  let busy = $state(false)
  let actor = $state(localStorage.getItem('athar-review-actor') ?? '')

  const project = $derived(projects.find((p) => p.project_id === selectedProject) ?? null)
  const model = $derived(project?.models.find((m) => m.model_key === selectedModel) ?? null)
  const resolvedIds = $derived(
    new Set(
      events
        .filter((e) => e.event === 'approval' || e.event === 'rejection')
        .map((e) => e.review_id!),
    ),
  )
  const pendingIds = $derived(
    new Set(
      events
        .filter(
          (e) =>
            e.event === 'review' &&
            !resolvedIds.has(e.id) &&
            e.baseline_artifact?.sha256 === model?.baseline.artifact.sha256,
        )
        .map((e) => e.id),
    ),
  )
  const openReview = $derived(events.find((e) => e.id === openReviewId) ?? null)

  $effect(() => {
    void refresh()
    const onHash = () => applyRoute()
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  })

  $effect(() => {
    localStorage.setItem('athar-review-actor', actor)
  })

  $effect(() => {
    if (openReviewId && selectedProject) {
      report = null
      reportError = null
      api
        .report(selectedProject, openReviewId)
        .then((r) => (report = r))
        .catch((error) => (reportError = String(error.message ?? error)))
    }
  })

  async function refresh(): Promise<void> {
    try {
      loadError = null
      projects = (await api.overview()).projects
      if (!selectedProject) applyRoute()
      if (selectedProject && selectedModel) await loadEvents()
    } catch (error: any) {
      loadError = String(error.message ?? error)
    }
  }

  function applyRoute(): void {
    const match = window.location.hash.match(/^#\/p\/([^/]+)\/m\/([^/]+)(?:\/r\/([^/]+))?$/)
    if (match) {
      selectedProject = decodeURIComponent(match[1])
      selectedModel = decodeURIComponent(match[2])
      openReviewId = match[3] ? decodeURIComponent(match[3]) : null
    } else if (projects.length > 0 && projects[0].models.length > 0) {
      select(projects[0].project_id, projects[0].models[0].model_key)
      return
    }
    if (selectedProject && selectedModel) void loadEvents()
  }

  function select(projectId: string, modelKey: string, reviewId?: string): void {
    selectedProject = projectId
    selectedModel = modelKey
    openReviewId = reviewId ?? null
    const suffix = reviewId ? `/r/${encodeURIComponent(reviewId)}` : ''
    window.location.hash = `#/p/${encodeURIComponent(projectId)}/m/${encodeURIComponent(modelKey)}${suffix}`
    void loadEvents()
  }

  async function loadEvents(): Promise<void> {
    if (!selectedProject || !selectedModel) return
    try {
      events = (await api.events(selectedProject, selectedModel)).events
    } catch (error: any) {
      loadError = String(error.message ?? error)
    }
  }

  async function upload(file: File): Promise<void> {
    if (!selectedProject || !selectedModel) return
    busy = true
    actionError = null
    try {
      const event = await api.uploadCandidate(
        selectedProject,
        selectedModel,
        'review',
        file,
        actor || 'reviewer',
      )
      await refresh()
      select(selectedProject, selectedModel, event.id)
    } catch (error: any) {
      actionError = String(error.message ?? error)
    } finally {
      busy = false
    }
  }

  async function resolveReview(action: 'approve' | 'reject', note: string): Promise<void> {
    if (!selectedProject || !selectedModel || !openReviewId) return
    busy = true
    actionError = null
    try {
      await api.resolve(selectedProject, selectedModel, action, openReviewId, actor || 'reviewer', note || undefined)
      await refresh()
      select(selectedProject, selectedModel)
    } catch (error: any) {
      actionError = String(error.message ?? error)
    } finally {
      busy = false
    }
  }
</script>

<div class="frame">
  <header class="titleblock">
    <div class="brand">
      <span class="mark">A</span>
      <div>
        <div class="eyebrow">Athar</div>
        <h1>Model review register</h1>
      </div>
    </div>
    <label class="actor">
      <span class="eyebrow">Reviewer</span>
      <input placeholder="your name" bind:value={actor} />
    </label>
  </header>

  {#if loadError}
    <div class="load-error">
      Could not reach the review server: {loadError}
      <button class="btn" onclick={() => refresh()}>Retry</button>
    </div>
  {/if}

  <div class="columns">
    <aside class="side">
      <Sidebar {projects} {selectedProject} {selectedModel} onselect={select} />
    </aside>

    <main>
      {#if model && project}
        {#if openReview}
          <ReviewDetail
            projectId={project.project_id}
            review={openReview}
            {report}
            {reportError}
            pending={pendingIds.has(openReview.id)}
            {busy}
            onresolve={resolveReview}
            onback={() => select(project.project_id, model.model_key)}
          />
        {:else}
          <section class="lineage-head">
            <div>
              <div class="eyebrow">Current baseline</div>
              <div class="baseline">
                <span class="mono">{shortSha(model.baseline.artifact.sha256)}</span>
                · {model.baseline.artifact.schema ?? '?'}
                · {formatBytes(model.baseline.artifact.size_bytes)}
                · accepted by {model.baseline.approved_by}, {formatWhen(model.baseline.approved_at)}
              </div>
            </div>
          </section>
          <UploadZone label="Review a new revision against this baseline" {busy} onupload={upload} />
          {#if actionError}
            <div class="action-error">{actionError}</div>
          {/if}
          <Register {events} {pendingIds} onopen={(id) => select(project.project_id, model.model_key, id)} />
        {/if}
      {:else}
        <section class="welcome">
          <h2>Nothing under review yet</h2>
          <p>
            Pick a model lineage on the left, or seed a store from the command line:
            <span class="mono">athar store set-baseline &lt;project&gt; &lt;model&gt; model.ifc --actor you</span>
          </p>
        </section>
      {/if}
    </main>
  </div>
</div>

<style>
  .frame {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px 24px 60px;
  }

  .titleblock {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 16px;
    padding-bottom: 14px;
    border-bottom: 2.5px solid var(--rule-strong);
    margin-bottom: 20px;
  }

  .brand {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .mark {
    display: grid;
    place-items: center;
    width: 38px;
    height: 38px;
    border: 2px solid var(--rule-strong);
    border-radius: 4px;
    font: 700 20px/1 var(--mono);
    background: var(--surface);
  }

  h1 {
    margin: 0;
    font-size: 19px;
    letter-spacing: -0.01em;
  }

  .actor {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  .actor input {
    border: 1px solid var(--rule);
    border-radius: 3px;
    padding: 5px 10px;
    background: var(--surface);
    width: 180px;
  }

  .columns {
    display: grid;
    grid-template-columns: 220px 1fr;
    gap: 28px;
    align-items: start;
  }

  main {
    display: flex;
    flex-direction: column;
    gap: 14px;
    min-width: 0;
  }

  .lineage-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .baseline {
    color: var(--ink-dim);
    font-size: 13px;
    margin-top: 2px;
  }

  .load-error,
  .action-error {
    padding: 10px 14px;
    border-radius: 4px;
    background: var(--c-del-soft);
    border-left: 3px solid var(--c-del);
    color: #7c2a2d;
    font-size: 13px;
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .action-error {
    margin-bottom: 0;
  }

  .welcome {
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
    border-radius: 4px;
    padding: 32px;
  }

  .welcome h2 {
    margin: 0 0 8px;
  }

  .welcome p {
    color: var(--ink-dim);
    margin: 0;
  }

  @media (max-width: 800px) {
    .columns {
      grid-template-columns: 1fr;
    }
  }
</style>
