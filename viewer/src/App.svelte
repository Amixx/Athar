<script lang="ts">
  import { onMount } from 'svelte'
  import { publishDebug } from './lib/debug'
  import { tessellate, type ModelGeometry } from './lib/ifc'
  import {
    buildDiffIndex,
    classBreakdown,
    parseReport,
    type DiffIndex,
    type EntityRecord,
  } from './lib/report'
  import {
    bucketFor,
    computeAppearances,
    countBucketEntities,
    countMeshless,
    DEFAULT_TOGGLES,
    isPlacementOnly,
    placementDeltaNorm,
    SNAP,
    type Toggles,
  } from './lib/renderState'
  import { DiffScene, type PickHit } from './lib/scene'
  import {
    chainFromFiles,
    classifyDrop,
    loadChainFromSrc,
    type DroppedFiles,
    type StepSource,
  } from './lib/sources'
  import type { Side } from './lib/types'

  type Phase = 'empty' | 'loading' | 'ready' | 'error'

  let phase = $state<Phase>('empty')
  let error = $state('')
  let warning = $state('')
  let progress = $state({ stage: '', count: 0 })
  let drops = $state<DroppedFiles>({})
  let names = $state({ old: 'old.ifc', new: 'new.ifc' })
  let rendererReady = $state(false)
  let sliderT = $state<number>(SNAP.both)
  let toggles = $state<Toggles>({ ...DEFAULT_TOGGLES })
  let showLines = $state(true)
  let dragging = $state(false)
  let selection = $state<PickHit | null>(null)
  let meshless = $state(0)

  let index = $state<DiffIndex | null>(null)
  let scene: DiffScene | null = null
  let canvasEl: HTMLCanvasElement
  let pointerDown: { x: number; y: number; time: number } | null = null

  let steps = $state<StepSource[]>([])
  let activeStep = $state(0)
  // One RTC offset from the first file tessellated is reused for every step, so
  // cached meshes stay in a single coordinate frame across step switches.
  let sharedOffset: ModelGeometry['rtcOffset'] = null
  const meshCache = new Map<string, ModelGeometry>()
  const reportCache = new Map<string, unknown>()
  let loadSeq = 0

  let oldInput: HTMLInputElement
  let newInput: HTMLInputElement
  let reportInput: HTMLInputElement

  const ASPECT_KEYS = ['geometry', 'data', 'topology', 'placement'] as const

  const appearances = $derived(computeAppearances(sliderT, toggles))
  const classes = $derived(index ? classBreakdown(index.report) : [])
  const modifiedScope = $derived(index?.report.stats.modified_change_scope ?? null)
  const directModified = $derived((modifiedScope?.intrinsic ?? 0) + (modifiedScope?.mixed ?? 0))
  const transitiveModified = $derived(modifiedScope?.transitive ?? 0)
  const selectedRecord = $derived.by<EntityRecord | null>(() => {
    if (!selection || !index) return null
    const map = selection.side === 'old' ? index.old : index.new
    return map.get(selection.stepId) ?? null
  })
  const selectedPair = $derived(selectedRecord?.pair ?? null)
  const complete = $derived(Boolean(drops.oldFile && drops.newFile && drops.reportFile))
  const hasChain = $derived(steps.length > 1)
  const currentLabel = $derived(steps[activeStep]?.label ?? '')

  $effect(() => {
    if (phase === 'ready') {
      scene?.applyAppearances(appearances, showLines)
      publishDebug({ sliderT, appearances })
    }
  })

  $effect(() => {
    if (phase === 'empty' && complete) {
      const chain = chainFromFiles(drops as Required<DroppedFiles>)
      steps = chain.steps
      void loadStep(chain.activeStep)
    }
  })

  onMount(() => {
    publishDebug({ phase: 'empty' })
    const src = new URLSearchParams(location.search).get('src')
    if (src) {
      phase = 'loading'
      progress = { stage: `Fetching from ${src}`, count: 0 }
      publishDebug({ phase: 'loading' })
      loadChainFromSrc(src).then((chain) => {
        steps = chain.steps
        warning = chain.warning ?? ''
        return loadStep(chain.activeStep)
      }, fail)
    }
    return () => scene?.dispose()
  })

  function fail(cause: unknown): void {
    error = cause instanceof Error ? cause.message : String(cause)
    phase = 'error'
    publishDebug({ phase: 'error', error })
  }

  async function getGeometry(
    key: string,
    fetchBytes: () => Promise<ArrayBuffer>,
    name: string,
  ): Promise<ModelGeometry> {
    const cached = meshCache.get(key)
    if (cached) return cached
    progress = { stage: `Tessellating ${name}`, count: 0 }
    const bytes = await fetchBytes()
    const geometry = await tessellate(bytes, {
      sharedRtcOffset: sharedOffset ?? undefined,
      onProgress: (count) => (progress = { stage: `Tessellating ${name}`, count }),
    })
    if (!sharedOffset && geometry.rtcOffset) sharedOffset = geometry.rtcOffset
    meshCache.set(key, geometry)
    return geometry
  }

  async function getReport(step: StepSource): Promise<unknown> {
    const key = `${step.oldKey}=>${step.newKey}`
    const cached = reportCache.get(key)
    if (cached !== undefined) return cached
    const json = await step.fetchReport()
    reportCache.set(key, json)
    return json
  }

  function stepTo(target: number): void {
    if (phase === 'loading') return
    const clamped = Math.max(0, Math.min(steps.length - 1, target))
    if (clamped === activeStep && phase === 'ready') return
    void loadStep(clamped)
  }

  async function loadStep(target: number): Promise<void> {
    const token = ++loadSeq
    const step = steps[target]
    if (!step) return
    try {
      phase = 'loading'
      activeStep = target
      names = { old: step.oldName, new: step.newName }
      publishDebug({
        phase: 'loading',
        warning,
        stepCount: steps.length,
        activeStep: target,
        stepLabel: step.label,
      })

      progress = { stage: 'Computing diff', count: 0 }
      const reportJson = await getReport(step)
      if (token !== loadSeq) return
      const report = parseReport(reportJson)
      const nextIndex = buildDiffIndex(report)

      const oldGeometry = await getGeometry(step.oldKey, step.fetchOldBytes, step.oldName)
      if (token !== loadSeq) return
      const newGeometry = await getGeometry(step.newKey, step.fetchNewBytes, step.newName)
      if (token !== loadSeq) return

      index = nextIndex
      const diff = nextIndex
      const bucketOfFn = (side: Side, stepId: number) =>
        bucketFor(side, side === 'old' ? diff.old.get(stepId) : diff.new.get(stepId))
      const displacementPairs = diff.placementPairs.filter(isPlacementOnly).map((item) => ({
        oldId: item.old.step_id,
        newId: item.new.step_id,
      }))

      // Pure counts first, so headless environments without WebGL still get
      // deterministic state for the smoke test.
      const bucketEntityCounts = countBucketEntities(
        oldGeometry.byEntity.keys(),
        newGeometry.byEntity.keys(),
        bucketOfFn,
      )
      const meshlessBySection = countMeshless(
        report,
        (id) => oldGeometry.byEntity.has(id),
        (id) => newGeometry.byEntity.has(id),
      )
      meshless = Object.values(meshlessBySection).reduce((a, b) => a + b, 0)

      if (!scene) {
        try {
          scene = new DiffScene(canvasEl)
          rendererReady = true
        } catch (cause) {
          rendererReady = false
          warning = `3D view unavailable (${cause instanceof Error ? cause.message : cause}). Counts and panels still work.`
        }
      }
      let displacementLines = displacementPairs.length
      if (scene) {
        const stats = scene.setModels(oldGeometry, newGeometry, bucketOfFn, displacementPairs)
        displacementLines = stats.displacementLines
        scene.applyAppearances(computeAppearances(sliderT, toggles), showLines)
      }

      selection = null
      phase = 'ready'
      publishDebug({
        phase: 'ready',
        rendererReady,
        warning,
        bucketEntityCounts,
        meshlessBySection,
        reportStats: report.stats as Record<string, unknown>,
        displacementLines,
        selection: null,
        stepCount: steps.length,
        activeStep: target,
        stepLabel: step.label,
      })
    } catch (cause) {
      if (token === loadSeq) fail(cause)
    }
  }

  function onPageDrop(event: DragEvent): void {
    event.preventDefault()
    dragging = false
    if (phase !== 'empty' || !event.dataTransfer) return
    drops = classifyDrop([...event.dataTransfer.files], drops)
  }

  function onSlotFiles(slot: 'oldFile' | 'newFile' | 'reportFile', input: HTMLInputElement): void {
    const file = input.files?.[0]
    if (file) drops = { ...drops, [slot]: file }
    input.value = ''
  }

  function onPointerDown(event: PointerEvent): void {
    pointerDown = { x: event.clientX, y: event.clientY, time: performance.now() }
  }

  function onPointerUp(event: PointerEvent): void {
    if (!pointerDown || !scene || phase !== 'ready') return
    const moved = Math.hypot(event.clientX - pointerDown.x, event.clientY - pointerDown.y)
    const elapsed = performance.now() - pointerDown.time
    pointerDown = null
    if (moved > 6 || elapsed > 600) return
    select(scene.pick(event.clientX, event.clientY))
  }

  function select(hit: PickHit | null): void {
    selection = hit
    scene?.setHighlight(hit ? { side: hit.side, stepId: hit.stepId } : null)
    publishDebug({ selection: hit ? { side: hit.side, stepId: hit.stepId } : null })
  }

  function sectionOf(record: EntityRecord | null, hit: PickHit | null): string {
    if (record) return record.section
    if (hit?.bucket === 'extra') return 'unchanged'
    return ''
  }

  function fmtDelta(mm: [number, number, number] | null): string {
    if (!mm) return '—'
    const norm = Math.hypot(mm[0], mm[1], mm[2])
    return `${norm.toFixed(1)} mm (${mm.map((v) => v.toFixed(1)).join(', ')})`
  }

  function shortHash(hash: string): string {
    return hash.slice(0, 10)
  }

  function onKeydown(event: KeyboardEvent): void {
    if (event.key === 'Escape') select(null)
    else if (hasChain && event.key === '[') stepTo(activeStep - 1)
    else if (hasChain && event.key === ']') stepTo(activeStep + 1)
  }
</script>

<svelte:window onkeydown={onKeydown} />

<main
  class="app"
  ondragover={(e) => {
    e.preventDefault()
    if (phase === 'empty') dragging = true
  }}
  ondragleave={() => (dragging = false)}
  ondrop={onPageDrop}
>
  <div
    class="viewport"
    role="application"
    aria-label="3D diff view — drag to orbit, click an entity to inspect"
    onpointerdown={onPointerDown}
    onpointerup={onPointerUp}
  >
    <canvas bind:this={canvasEl}></canvas>
  </div>

  {#if warning && phase !== 'error'}
    <div class="panel banner">{warning}</div>
  {/if}

  {#if hasChain && (phase === 'ready' || phase === 'loading')}
    <div class="panel stepnav">
      <button
        onclick={() => stepTo(activeStep - 1)}
        disabled={activeStep === 0 || phase === 'loading'}
        aria-label="Previous step"
        title="Previous step ( [ )">‹</button
      >
      <span class="stepind" data-testid="step-indicator">
        {activeStep + 1}/{steps.length}: {currentLabel}
      </span>
      <button
        onclick={() => stepTo(activeStep + 1)}
        disabled={activeStep === steps.length - 1 || phase === 'loading'}
        aria-label="Next step"
        title="Next step ( ] )">›</button
      >
    </div>
  {/if}

  {#if phase === 'ready' && index}
    <div class="brand-stack">
      <div class="panel brand">
        <span class="name">Athar</span>
        <span class="sub">IFC diff</span>
        <span class="schema-chip">{index.report.schemas.new}</span>
      </div>
      <div class="panel filechip">
        <span class="microlabel">old</span><span class="mono">{names.old}</span>
        <span class="microlabel">new</span><span class="mono">{names.new}</span>
      </div>
    </div>

    <div class="panel sliderbar">
      <span class="endlabel old">Old</span>
      <input
        type="range"
        min="0"
        max="1"
        step="0.01"
        bind:value={sliderT}
        aria-label="Blend between old and new model state"
      />
      <span class="endlabel new">New</span>
      <div class="snapbtns">
        <button class:active={sliderT === SNAP.old} onclick={() => (sliderT = SNAP.old)}>Old</button>
        <button class:active={sliderT === SNAP.both} onclick={() => (sliderT = SNAP.both)}>Both</button>
        <button class:active={sliderT === SNAP.new} onclick={() => (sliderT = SNAP.new)}>New</button>
      </div>
    </div>

    <aside class="panel summary">
      <span class="microlabel">summary</span>
      <div class="statgrid">
        <div class="stat add"><div class="num">+{index.report.stats.added}</div><div class="microlabel">added</div></div>
        <div class="stat del"><div class="num">−{index.report.stats.deleted}</div><div class="microlabel">deleted</div></div>
        <div class="stat mod">
          <div class="num">~{index.report.stats.modified}</div>
          <div class="microlabel">
            {#if modifiedScope && index.report.stats.modified > 0}
              {directModified} direct · {transitiveModified} indirect
            {:else}
              modified
            {/if}
          </div>
        </div>
        <div class="stat unch"><div class="num">={index.report.stats.unchanged}</div><div class="microlabel">unchanged</div></div>
      </div>
      {#if modifiedScope}
        <div class="footnote">
          scope: {modifiedScope.intrinsic ?? 0} intrinsic · {modifiedScope.transitive ?? 0} transitive · {modifiedScope.mixed ?? 0} mixed
        </div>
      {/if}
      {#if classes.length}
        <span class="microlabel">by class</span>
        <div class="classrows">
          {#each classes as row (row.klass)}
            <div class="classrow">
              <span class="cls" title={row.klass}>{row.klass}</span>
              <span class="nums">
                {#if row.added}<span class="a">+{row.added}</span>{/if}
                {#if row.deleted}<span class="d">−{row.deleted}</span>{/if}
                {#if row.modified}<span class="m">~{row.modified}</span>{/if}
              </span>
            </div>
          {/each}
        </div>
      {/if}
      {#if meshless > 0}
        <div class="footnote">{meshless} report entit{meshless === 1 ? 'y has' : 'ies have'} no 3D geometry (spatial containers etc.) — counted above, not drawn.</div>
      {/if}
      {#if index.placementPairs.filter(isPlacementOnly).length > 0}
        <div class="footnote">Amber arrows trace placement-only moves (old → new centroid).</div>
      {/if}
    </aside>

    {#if selection}
      <aside class="panel inspector">
        <button class="close" onclick={() => select(null)} aria-label="Close inspector">✕</button>
        <span class="microlabel">entity</span>
        <div class="title">{selectedRecord?.name ?? `#${selection.stepId}`}</div>
        <div>
          <span class="badge {sectionOf(selectedRecord, selection)}">{sectionOf(selectedRecord, selection) || 'unlisted'}</span>
        </div>
        <dl class="kv">
          <dt>Class</dt>
          <dd>{selectedRecord?.klass ?? '—'}</dd>
          <dt>GUID</dt>
          <dd class="mono">{selectedRecord?.guid ?? '—'}</dd>
          {#if selectedPair}
            <dt>STEP id</dt>
            <dd class="mono">old #{selectedPair.old.step_id} → new #{selectedPair.new.step_id}</dd>
          {:else}
            <dt>STEP id</dt>
            <dd class="mono">{selection.side} #{selection.stepId}</dd>
          {/if}
          {#if selectedPair}
            <dt>Match</dt>
            <dd>{selectedPair.match.reason} · score {selectedPair.match.score}</dd>
            <dt>Scope</dt>
            <dd>{selectedPair.change_scope}</dd>
          {/if}
        </dl>
        {#if selectedPair}
          <span class="microlabel">aspects</span>
          <div class="aspects">
            {#each ASPECT_KEYS as aspect (aspect)}
              <div class="aspect {selectedPair.aspects[aspect]}">
                <span>{aspect}</span>
                <span class="state">{selectedPair.aspects[aspect] === 'changed' ? 'Δ' : '='}</span>
              </div>
            {/each}
          </div>
          {#if selectedPair.aspects.placement === 'changed'}
            <dl class="kv">
              <dt>Δ placement</dt>
              <dd class="mono">{fmtDelta(selectedPair.aspects.placement_delta_mm)}</dd>
            </dl>
          {/if}
          {#if selectedPair.aspects.data === 'changed'}
            <dl class="kv">
              <dt>Data hash</dt>
              <dd class="mono">{shortHash(selectedPair.data_hash.old)} → {shortHash(selectedPair.data_hash.new)}</dd>
            </dl>
          {/if}
        {:else if !selectedRecord}
          <p class="footnote">Not in the report — the engine does not signature this entity (no identity evidence). Shown gray.</p>
        {/if}
      </aside>
    {/if}

    <div class="panel toolbar">
      <button class:off={!toggles.added} onclick={() => (toggles.added = !toggles.added)}>
        <i class="dot add"></i>Added
      </button>
      <button class:off={!toggles.deleted} onclick={() => (toggles.deleted = !toggles.deleted)}>
        <i class="dot del"></i>Deleted
      </button>
      <button class:off={!toggles.modified} onclick={() => (toggles.modified = !toggles.modified)}>
        <i class="dot mod"></i>Direct
      </button>
      <button
        class:off={!toggles.transitive}
        title="Indirect changes caused only by a changed dependency"
        onclick={() => (toggles.transitive = !toggles.transitive)}
      >
        <i class="dot transitive"></i>Indirect
      </button>
      <button class:off={!toggles.unchanged} onclick={() => (toggles.unchanged = !toggles.unchanged)}>
        <i class="dot unch"></i>Unchanged
      </button>
      <div class="sep"></div>
      <button class:off={!toggles.ghostUnchanged} onclick={() => (toggles.ghostUnchanged = !toggles.ghostUnchanged)}>
        Ghost
      </button>
      <button class:off={!showLines} onclick={() => (showLines = !showLines)}>Move arrows</button>
    </div>

    <div class="panel viewbtns">
      <button onclick={() => scene?.fitChanges()}>Fit changes</button>
      <button onclick={() => scene?.fitModel()}>Fit model</button>
    </div>
  {/if}

  {#if phase === 'empty'}
    <div class="center-stage">
      <div class="panel hero" class:armed={dragging}>
        <h1><em>Athar</em> — semantic IFC diff viewer</h1>
        <p>
          Drop two IFC revisions and the Athar report that compares them. Everything runs in your
          browser — models never leave this machine.
        </p>
        <div class="dropzones">
          <button class="dropzone" class:filled={drops.oldFile} onclick={() => oldInput.click()}>
            <span class="slot">Old .ifc</span>
            <span class="hint">{drops.oldFile?.name ?? 'drop or browse'}</span>
          </button>
          <button class="dropzone" class:filled={drops.newFile} onclick={() => newInput.click()}>
            <span class="slot">New .ifc</span>
            <span class="hint">{drops.newFile?.name ?? 'drop or browse'}</span>
          </button>
          <button
            class="dropzone"
            class:filled={drops.reportFile}
            onclick={() => reportInput.click()}
          >
            <span class="slot">Report .json</span>
            <span class="hint">{drops.reportFile?.name ?? 'drop or browse'}</span>
          </button>
        </div>
        <p class="footnote">
          No report yet? Generate one with <span class="mono">python -m athar old.ifc new.ifc &gt;
          report.json</span> — or skip all of this with <span class="mono">athar view old.ifc
          new.ifc</span>, which opens this app preloaded.
        </p>
      </div>
    </div>
  {:else if phase === 'loading'}
    <div class="center-stage">
      <div class="panel progress">
        <span class="microlabel">loading</span>
        <div class="bar"></div>
        <span class="mono">{progress.stage}{progress.count ? ` — ${progress.count} meshes` : ''}</span>
      </div>
    </div>
  {:else if phase === 'error'}
    <div class="center-stage">
      <div class="panel hero">
        <h1>Could not load</h1>
        <p class="panel banner error" style="position: static; transform: none;">{error}</p>
        <div class="cta">
          <button
            class="primary"
            onclick={() => {
              phase = 'empty'
              error = ''
              drops = {}
              publishDebug({ phase: 'empty', error: undefined })
            }}>Start over</button
          >
        </div>
      </div>
    </div>
  {/if}

  <input
    type="file"
    accept=".ifc"
    bind:this={oldInput}
    data-testid="input-old"
    onchange={() => onSlotFiles('oldFile', oldInput)}
  />
  <input
    type="file"
    accept=".ifc"
    bind:this={newInput}
    data-testid="input-new"
    onchange={() => onSlotFiles('newFile', newInput)}
  />
  <input
    type="file"
    accept=".json,application/json"
    bind:this={reportInput}
    data-testid="input-report"
    onchange={() => onSlotFiles('reportFile', reportInput)}
  />
</main>
