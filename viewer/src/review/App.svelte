<script lang="ts">
  import type { ProjectSummary, Report, StoreEvent } from './lib/api'
  import { ApiError, api } from './lib/api'
  import { formatBytes, formatWhen, reviewStates, shortSha } from './lib/format'
  import ActorDialog from './components/ActorDialog.svelte'
  import NewLineage from './components/NewLineage.svelte'
  import Register from './components/Register.svelte'
  import ReviewDetail from './components/ReviewDetail.svelte'
  import Sidebar from './components/Sidebar.svelte'
  import UploadZone from './components/UploadZone.svelte'

  type Route =
    | { view: 'home' }
    | { view: 'new'; projectId: string | null }
    | { view: 'lineage'; projectId: string; modelKey: string; reviewId: string | null }

  const ACTOR_KEY = 'athar-review-actor'
  const ASKED_KEY = 'athar-review-actor-asked'

  let route = $state<Route>(parseRoute(window.location.hash))
  let projects = $state<ProjectSummary[] | null>(null)
  let maxUploadBytes = $state<number | null>(null)
  let loadError = $state<string | null>(null)
  let events = $state<StoreEvent[]>([])
  let eventsKey = $state<string | null>(null)
  let eventsError = $state<string | null>(null)
  let report = $state<Report | null>(null)
  let reportError = $state<ApiError | null>(null)
  let reportAttempt = $state(0)
  let uploading = $state<File | null>(null)
  let uploadError = $state<string | null>(null)
  let decisionError = $state<string | null>(null)
  let deciding = $state(false)
  let actor = $state(sessionValue(ACTOR_KEY))
  let askingName = $state(!sessionValue(ACTOR_KEY) && !sessionValue(ASKED_KEY))

  const lineage = $derived(route.view === 'lineage' ? route : null)
  const lineageKey = $derived(lineage ? `${lineage.projectId}/${lineage.modelKey}` : null)
  const project = $derived(lineage ? (projects?.find((p) => p.project_id === lineage.projectId) ?? null) : null)
  const model = $derived(lineage ? (project?.models.find((m) => m.model_key === lineage.modelKey) ?? null) : null)
  const lineageEvents = $derived(eventsKey === lineageKey ? events : [])
  const states = $derived(reviewStates(lineageEvents))
  const openReview = $derived(
    lineage?.reviewId
      ? (lineageEvents.find((e) => e.id === lineage.reviewId && e.event === 'review') ?? null)
      : null,
  )
  const named = $derived(actor.trim().length > 0)
  const busy = $derived(uploading !== null || deciding)

  function sessionValue(key: string): string {
    try {
      return sessionStorage.getItem(key) ?? ''
    } catch {
      return ''
    }
  }

  function remember(key: string, value: string): void {
    try {
      sessionStorage.setItem(key, value)
    } catch {
      // Storage can be blocked; the value then lasts until the page reloads.
    }
  }

  function parseRoute(hash: string): Route {
    const parts = hash.replace(/^#\/?/, '').split('/').filter(Boolean).map(decodeURIComponent)
    if (parts[0] === 'new') return { view: 'new', projectId: parts[1] ?? null }
    if (parts[0] === 'p' && parts[2] === 'm' && parts.length >= 4) {
      const reviewId = parts[4] === 'r' && parts[5] ? parts[5] : null
      return { view: 'lineage', projectId: parts[1], modelKey: parts[3], reviewId }
    }
    return { view: 'home' }
  }

  function lineageHref(projectId: string, modelKey: string, reviewId?: string): string {
    const base = `#/p/${encodeURIComponent(projectId)}/m/${encodeURIComponent(modelKey)}`
    return reviewId ? `${base}/r/${encodeURIComponent(reviewId)}` : base
  }

  function go(hash: string): void {
    if (window.location.hash === hash) route = parseRoute(hash)
    else window.location.hash = hash
  }

  $effect(() => {
    const onHash = () => {
      route = parseRoute(window.location.hash)
      uploadError = null
      decisionError = null
    }
    window.addEventListener('hashchange', onHash)
    void refreshOverview()
    return () => window.removeEventListener('hashchange', onHash)
  })

  $effect(() => {
    if (route.view !== 'home' || projects === null) return
    const first = projects.find((p) => p.models.length > 0)
    if (first) go(lineageHref(first.project_id, first.models[0].model_key))
  })

  $effect(() => {
    if (lineageKey && route.view === 'lineage') void loadEvents(route.projectId, route.modelKey)
  })

  $effect(() => {
    void reportAttempt
    const reviewId = route.view === 'lineage' ? route.reviewId : null
    const projectId = route.view === 'lineage' ? route.projectId : null
    report = null
    reportError = null
    if (!projectId || !reviewId) return
    let current = true
    api
      .report(projectId, reviewId)
      .then((loaded) => current && (report = loaded))
      .catch((error: unknown) => current && (reportError = asApiError(error)))
    return () => {
      current = false
    }
  })

  function asApiError(error: unknown): ApiError {
    return error instanceof ApiError ? error : new ApiError(0, 'client_error', String(error))
  }

  async function refreshOverview(): Promise<void> {
    try {
      const overview = await api.overview()
      projects = overview.projects
      maxUploadBytes = overview.max_upload_bytes
      loadError = null
    } catch (error) {
      loadError = asApiError(error).message
    }
  }

  async function loadEvents(projectId: string, modelKey: string): Promise<void> {
    const key = `${projectId}/${modelKey}`
    try {
      const loaded = (await api.events(projectId, modelKey)).events
      if (lineageKey !== key) return
      events = loaded
      eventsKey = key
      eventsError = null
    } catch (error) {
      if (lineageKey === key) eventsError = asApiError(error).message
    }
  }

  async function refreshAll(): Promise<void> {
    await refreshOverview()
    if (route.view === 'lineage') await loadEvents(route.projectId, route.modelKey)
  }

  function saveName(name: string): void {
    actor = name
    remember(ACTOR_KEY, name)
    askingName = false
  }

  function closeNameDialog(): void {
    remember(ASKED_KEY, '1')
    askingName = false
  }

  function uploadMessage(error: ApiError, file: File, kind: 'review' | 'baseline'): string {
    switch (error.code) {
      case 'engine_refused':
        return (
          `Athar could not ${kind === 'review' ? 'compare' : 'read'} ${file.name}: ${error.message} ` +
          (kind === 'review'
            ? `Check that it is an IFC export in the baseline's schema, then drop it again.`
            : 'Check that it is an IFC4 or IFC2X3 export, then drop it again.')
        )
      case 'baseline_changed':
        return `The baseline changed while ${file.name} was being compared. Drop it again to compare it with the current baseline.`
      case 'baseline_exists':
        return 'That lineage already has a baseline. Open it from the list on the left to review deliveries against it.'
      case 'actor_required':
        askingName = true
        return error.message
      default:
        return error.message
    }
  }

  async function uploadReview(file: File): Promise<void> {
    if (route.view !== 'lineage') return
    const { projectId, modelKey } = route
    uploading = file
    uploadError = null
    try {
      const event = await api.upload(projectId, modelKey, 'review', file, actor)
      await refreshAll()
      go(lineageHref(projectId, modelKey, event.id))
    } catch (error) {
      uploadError = uploadMessage(asApiError(error), file, 'review')
      await refreshAll()
    } finally {
      uploading = null
    }
  }

  async function uploadBaseline(projectId: string, modelKey: string, file: File): Promise<void> {
    uploading = file
    uploadError = null
    try {
      await api.upload(projectId, modelKey, 'baseline', file, actor)
      await refreshOverview()
      go(lineageHref(projectId, modelKey))
    } catch (error) {
      uploadError = uploadMessage(asApiError(error), file, 'baseline')
      await refreshOverview()
    } finally {
      uploading = null
    }
  }

  async function resolveReview(action: 'approve' | 'reject', note: string): Promise<boolean> {
    if (route.view !== 'lineage' || !route.reviewId) return false
    deciding = true
    decisionError = null
    try {
      await api.resolve(route.projectId, route.modelKey, action, route.reviewId, actor, note || undefined)
      await refreshAll()
      return true
    } catch (raw) {
      const error = asApiError(raw)
      if (error.code === 'actor_required') askingName = true
      const refusalMessages: Record<string, string> = {
        review_stale: 'This review was superseded by a later baseline change.',
        review_approved: 'This review was already accepted.',
        review_rejected: 'This review was already rejected.',
        baseline_changed: 'The baseline changed before this decision could be recorded.',
        no_baseline: 'This model lineage has no baseline to decide against.',
      }
      decisionError = error.status === 409
        ? `${refusalMessages[error.code] ?? 'The review state changed before this decision could be recorded.'} The current state is shown below.`
        : error.message
      await refreshAll()
      return false
    } finally {
      deciding = false
    }
  }
</script>

<div class="frame">
  <header class="titleblock">
    <div class="brand">
      <span class="mark" aria-hidden="true">A</span>
      <div>
        <div class="eyebrow">Athar</div>
        <h1>Model review register</h1>
      </div>
    </div>
    <div class="actor">
      <span class="eyebrow">Reviewer</span>
      {#if named}
        <span class="name" data-testid="reviewer-name">{actor}</span>
        <button class="link" onclick={() => (askingName = true)}>Change</button>
      {:else}
        <span class="unnamed">Not set</span>
        <button class="link" onclick={() => (askingName = true)}>Set name</button>
      {/if}
    </div>
  </header>

  <ActorDialog open={askingName} current={actor} onsave={saveName} onclose={closeNameDialog} />

  {#if loadError}
    <div class="load-error" role="alert">
      <span>The review server did not answer: {loadError}</span>
      <button class="btn" onclick={() => refreshAll()}>Retry</button>
    </div>
  {/if}

  <div class="columns">
    <aside class="side">
      <Sidebar
        projects={projects ?? []}
        selectedProject={route.view === 'lineage' ? route.projectId : null}
        selectedModel={route.view === 'lineage' ? route.modelKey : null}
        onselect={(p, m) => go(lineageHref(p, m))}
        onnew={(p) => go(p ? `#/new/${encodeURIComponent(p)}` : '#/new')}
      />
    </aside>

    <main>
      {#if projects === null}
        {#if !loadError}<p class="dim" role="status">Loading the store…</p>{/if}
      {:else if route.view === 'new'}
        {#key route.projectId}
          <NewLineage
            initialProject={route.projectId}
            {actor}
            {uploading}
            error={uploadError}
            maxBytes={maxUploadBytes}
            onupload={uploadBaseline}
            onrefuse={(message) => (uploadError = message)}
          />
        {/key}
      {:else if route.view === 'home'}
        {@const empty = projects.find((p) => p.models.length === 0)}
        <section class="welcome">
          <h2>Nothing under review yet</h2>
          <p>
            {#if projects.length === 0}
              This store has no projects. Start one by setting the first accepted model as its
              baseline.
            {:else}
              Project {empty?.name} has no accepted baseline yet. Set one to start reviewing
              deliveries.
            {/if}
          </p>
          <a class="btn primary" href={empty ? `#/new/${encodeURIComponent(empty.project_id)}` : '#/new'}>
            Set a first baseline
          </a>
        </section>
      {:else if !project || !model}
        <section class="welcome" role="alert">
          <h2>No such model lineage</h2>
          <p>
            This store has no accepted baseline for {route.projectId}, {route.modelKey}. Pick a lineage
            on the left, or start this one.
          </p>
          <a class="btn primary" href={`#/new/${encodeURIComponent(route.projectId)}`}>Set a first baseline</a>
        </section>
      {:else if eventsError}
        <div class="load-error" role="alert">
          <span>The history of {project.name}, {model.model_key} could not be loaded: {eventsError}</span>
          <button class="btn" onclick={() => refreshAll()}>Retry</button>
        </div>
      {:else if route.reviewId && openReview}
        {#key openReview.id}
          <ReviewDetail
            projectId={project.project_id}
            modelKey={model.model_key}
            review={openReview}
            {report}
            {reportError}
            reviewState={states.get(openReview.id)!}
            {states}
            {actor}
            {busy}
            {decisionError}
            registerHref={lineageHref(project.project_id, model.model_key)}
            reviewHref={(id) => lineageHref(project.project_id, model.model_key, id)}
            onresolve={resolveReview}
            onaskname={() => (askingName = true)}
            onretryreport={() => (reportAttempt += 1)}
          />
        {/key}
      {:else if route.reviewId && eventsKey === lineageKey}
        <section class="welcome" role="alert">
          <h2>No review {route.reviewId} in this lineage</h2>
          <p>The link may point to another model or to an event that is not a review.</p>
          <a class="btn" href={lineageHref(project.project_id, model.model_key)}>Open the register</a>
        </section>
      {:else}
        <section class="lineage-head">
          <div class="eyebrow">Current baseline of {project.name}, {model.model_key}</div>
          <div class="baseline">
            <span class="mono" title={model.baseline.artifact.sha256}>{shortSha(model.baseline.artifact.sha256)}</span>
            <span>{model.baseline.artifact.schema ?? 'unknown schema'}</span>
            <span>{formatBytes(model.baseline.artifact.size_bytes)}</span>
            <span>accepted by {model.baseline.approved_by}, {formatWhen(model.baseline.approved_at)}</span>
          </div>
        </section>
        <UploadZone
          label="Review a new delivery against this baseline"
          busyLabel="Comparing the delivery with the baseline…"
          busyFile={uploading}
          disabledReason={named ? null : 'Set your reviewer name (top right) to upload a delivery.'}
          maxBytes={maxUploadBytes}
          onupload={uploadReview}
          onrefuse={(message) => (uploadError = message)}
        />
        {#if uploadError}
          <div class="action-error" role="alert">{uploadError}</div>
        {/if}
        {#key lineageKey}
          <Register
            events={lineageEvents}
            {states}
            reviewHref={(id) => lineageHref(project.project_id, model.model_key, id)}
          />
        {/key}
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
    display: grid;
    grid-template-columns: auto auto;
    column-gap: 10px;
    align-items: baseline;
  }

  .actor .eyebrow {
    grid-column: 1 / -1;
  }

  .name {
    font-weight: 600;
  }

  .unnamed {
    color: var(--c-pend);
  }

  .link {
    border: none;
    background: none;
    padding: 0;
    color: var(--stamp);
    text-decoration: underline;
    font-size: 13px;
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

  .baseline {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 14px;
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
    justify-content: space-between;
    gap: 12px;
  }

  .action-error {
    margin-bottom: 0;
  }

  .welcome {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
    border-radius: 4px;
    padding: 32px;
  }

  .welcome h2 {
    margin: 0;
  }

  .welcome p,
  .dim {
    color: var(--ink-dim);
    margin: 0;
  }

  .welcome .btn {
    text-decoration: none;
  }

  @media (max-width: 800px) {
    .columns {
      grid-template-columns: 1fr;
    }

    .titleblock {
      flex-direction: column;
      align-items: flex-start;
    }
  }
</style>
