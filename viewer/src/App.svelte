<script lang="ts">
  import { onMount } from 'svelte'
  import { publishDebug } from './lib/debug'
  import { entityBounds, frameTranslations } from './lib/frame'
  import { tessellate, type ModelGeometry } from './lib/ifc'
  import {
    buildDiffIndex,
    classBreakdown,
    type DiffIndex,
    type EntityRecord,
  } from './lib/report'
  import {
    bucketFor,
    computeAppearances,
    countBucketEntities,
    REPORT_KEYS,
    reportCoverage,
    DEFAULT_TOGGLES,
    groupDisplacements,
    isPlacementOnly,
    SNAP,
    unreportedEntities,
    type ReportCoverage,
    type ReportKey,
    type Toggles,
  } from './lib/renderState'
  import { DiffScene } from './lib/scene'
  import {
    addDroppedFiles,
    assignRoles,
    chainFromFiles,
    hashFile,
    loadChainFromSrc,
    NO_DROPS,
    readReportFile,
    reportInputHashes,
    type DroppedFiles,
    type HashedFile,
    type StepSource,
  } from './lib/sources'
  import type { AtharReport, Side } from './lib/types'

  type Phase = 'empty' | 'confirm' | 'loading' | 'ready' | 'error'

  interface UnverifiedPair {
    files: [HashedFile, HashedFile]
    report: AtharReport
    reason: string
    oldIndex: 0 | 1 | null
  }

  let phase = $state<Phase>('empty')
  let error = $state('')
  let warning = $state('')
  let dropNotes = $state<string[]>([])
  let progress = $state({ stage: '', count: 0 })
  let drops = $state<DroppedFiles>(NO_DROPS)
  let unverified = $state<UnverifiedPair | null>(null)
  let names = $state({ old: 'old.ifc', new: 'new.ifc' })
  let rendererReady = $state(false)
  let sliderT = $state<number>(SNAP.both)
  let toggles = $state<Toggles>({ ...DEFAULT_TOGGLES })
  let showLines = $state(true)
  let dragging = $state(false)
  let selection = $state<{ side: Side; stepId: number } | null>(null)
  let coverage = $state<ReportCoverage | null>(null)
  let extraDrawn = $state(0)
  let unreported = $state<{ old: Array<{ stepId: number; klass: string }>; new: number }>({ old: [], new: 0 })

  const REPORT_KEY_LEGEND: Record<ReportKey, { label: string; dot: string }> = {
    added: { label: 'Added', dot: 'add' },
    deleted: { label: 'Deleted', dot: 'del' },
    modified: { label: 'Direct', dot: 'mod' },
    transitive: { label: 'Indirect', dot: 'transitive' },
    unchanged: { label: 'Unchanged', dot: 'unch' },
  }

  let index = $state<DiffIndex | null>(null)
  let scene: DiffScene | null = null
  let canvasEl: HTMLCanvasElement
  let pointerDown: { x: number; y: number; time: number } | null = null

  let steps = $state<StepSource[]>([])
  let activeStep = $state(0)
  const meshCache = new Map<string, ModelGeometry>()
  const reportCache = new Map<string, AtharReport>()
  let loadSeq = 0

  let ifcInput: HTMLInputElement
  let reportInput: HTMLInputElement

  const ASPECT_KEYS = ['geometry', 'data', 'topology', 'placement'] as const

  const appearances = $derived(computeAppearances(sliderT, toggles))
  const classes = $derived(index ? classBreakdown(index.report) : [])
  const modifiedScope = $derived(index?.report.stats.modified_change_scope ?? null)
  const cohorts = $derived(index?.report.stats.placement_cohorts ?? [])
  const directModified = $derived((modifiedScope?.intrinsic ?? 0) + (modifiedScope?.mixed ?? 0))
  const transitiveModified = $derived(modifiedScope?.transitive ?? 0)
  const selectedRecord = $derived.by<EntityRecord | null>(() => {
    if (!selection || !index) return null
    const map = selection.side === 'old' ? index.old : index.new
    return map.get(selection.stepId) ?? null
  })
  const selectedPair = $derived(selectedRecord?.pair ?? null)
  const complete = $derived(drops.ifcs.length === 2 && Boolean(drops.reportFile))
  const hasChain = $derived(steps.length > 1)
  const currentLabel = $derived(steps[activeStep]?.label ?? '')

  $effect(() => {
    if (phase === 'ready') {
      scene?.applyAppearances(appearances, showLines)
      publishDebug({ sliderT, appearances })
    }
  })

  $effect(() => {
    if (phase === 'empty' && complete) void verifyDrops(drops)
  })

  onMount(() => {
    publishDebug({ phase: 'empty' })
    const src = new URLSearchParams(location.search).get('src')
    if (src) {
      phase = 'loading'
      progress = { stage: `Fetching from ${src}`, count: 0 }
      publishDebug({ phase: 'loading' })
      loadChainFromSrc(src, location.href).then((chain) => {
        steps = chain.steps
        warning = chain.warning ?? ''
        return loadStep(chain.activeStep)
      }, fail)
    }
    return () => scene?.dispose()
  })

  async function verifyDrops(dropped: DroppedFiles): Promise<void> {
    phase = 'loading'
    progress = { stage: 'Checking the files against the report', count: 0 }
    publishDebug({ phase: 'loading' })
    try {
      const [a, b] = await Promise.all(dropped.ifcs.map(hashFile))
      const report = await readReportFile(dropped.reportFile!)
      const roles = assignRoles([a, b], reportInputHashes(report))
      if (roles.verified) {
        startDropped(roles.old, roles.new, report)
      } else {
        unverified = { files: [a, b], report, reason: roles.reason, oldIndex: null }
        phase = 'confirm'
        publishDebug({ phase: 'confirm' })
      }
    } catch (cause) {
      fail(cause)
    }
  }

  function startDropped(oldFile: HashedFile, newFile: HashedFile, report: AtharReport): void {
    unverified = null
    const chain = chainFromFiles(oldFile, newFile, report)
    steps = chain.steps
    void loadStep(chain.activeStep)
  }

  function confirmUnverified(): void {
    if (!unverified || unverified.oldIndex === null) return
    const oldFile = unverified.files[unverified.oldIndex]
    const newFile = unverified.files[1 - unverified.oldIndex]
    startDropped(oldFile, newFile, unverified.report)
  }

  function startOver(): void {
    loadSeq += 1
    scene?.dispose()
    scene = null
    rendererReady = false
    selection = null
    warning = ''
    phase = 'empty'
    error = ''
    meshCache.clear()
    reportCache.clear()
    steps = []
    index = null
    coverage = null
    extraDrawn = 0
    unreported = { old: [], new: 0 }
    drops = NO_DROPS
    dropNotes = []
    unverified = null
    publishDebug({ phase: 'empty', error: undefined })
  }

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
      onProgress: (count) => (progress = { stage: `Tessellating ${name}`, count }),
    })
    meshCache.set(key, geometry)
    return geometry
  }

  async function getReport(step: StepSource): Promise<AtharReport> {
    const cached = reportCache.get(step.reportKey)
    if (cached) return cached
    const report = await step.fetchReport()
    reportCache.set(step.reportKey, report)
    return report
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
      const report = await getReport(step)
      if (token !== loadSeq) return
      const nextIndex = buildDiffIndex(report)

      const oldGeometry = await getGeometry(step.oldKey, step.fetchOldBytes, step.oldName)
      if (token !== loadSeq) return
      const newGeometry = await getGeometry(step.newKey, step.fetchNewBytes, step.newName)
      if (token !== loadSeq) return

      index = nextIndex
      const diff = nextIndex
      const translations = frameTranslations(oldGeometry.origin, newGeometry.origin)
      const geometries = { old: oldGeometry, new: newGeometry }
      const bucketOfFn = (side: Side, stepId: number) =>
        bucketFor(side, side === 'old' ? diff.old.get(stepId) : diff.new.get(stepId), geometries[side].byEntity.get(stepId)?.[0]?.ifcType)
      const displacementGroups = groupDisplacements(
        report.modified,
        report.stats.placement_cohorts ?? [],
      )

      // Computed before the scene exists so they are published even without WebGL.
      const bucketEntityCounts = countBucketEntities(
        oldGeometry.byEntity.keys(),
        newGeometry.byEntity.keys(),
        bucketOfFn,
      )
      extraDrawn = bucketEntityCounts.extra ?? 0
      const unreportedIds = unreportedEntities(oldGeometry.byEntity.keys(), newGeometry.byEntity.keys(), (side, stepId) =>
        (side === 'old' ? diff.old : diff.new).has(stepId),
      )
      unreported = {
        old: unreportedIds.old.map((stepId) => ({
          stepId,
          klass: oldGeometry.byEntity.get(stepId)?.[0]?.ifcType ?? 'unknown class',
        })),
        new: unreportedIds.new.length,
      }
      coverage = reportCoverage(report, (side, stepId) => geometries[side].byEntity.has(stepId), bucketOfFn)

      if (!scene) {
        try {
          scene = new DiffScene(canvasEl)
          rendererReady = true
        } catch (cause) {
          rendererReady = false
          warning = `3D view unavailable (${cause instanceof Error ? cause.message : cause}). Counts and panels still work.`
        }
      }
      let displacementLines = displacementGroups.length
      if (scene) {
        const stats = scene.setModels(
          oldGeometry,
          newGeometry,
          bucketOfFn,
          displacementGroups,
          translations,
          (side, stepId) => (side === 'old' ? diff.old : diff.new).get(stepId)?.klass,
        )
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
        reportCoverage: coverage,
        reportStats: report.stats as Record<string, unknown>,
        displacementLines,
        selection: null,
        stepCount: steps.length,
        activeStep: target,
        stepLabel: step.label,
        frameTranslations: translations,
        entityBounds: (side: Side, stepId: number) => {
          const meshes = geometries[side].byEntity.get(stepId)
          return meshes ? entityBounds(meshes, translations[side]) : null
        },
        entityDrawing: (side: Side, stepId: number) => scene?.entityDrawing(side, stepId) ?? null,
        drawnBounds: (side: Side, stepId: number) => {
          const box = scene?.entityBox(side, stepId)
          return box ? { min: box.min.toArray(), max: box.max.toArray() } : null
        },
      })
    } catch (cause) {
      if (token === loadSeq) fail(cause)
    }
  }

  function onPageDrop(event: DragEvent): void {
    event.preventDefault()
    dragging = false
    if (phase !== 'empty' || !event.dataTransfer) return
    addFiles([...event.dataTransfer.files])
  }

  function onPickedFiles(input: HTMLInputElement): void {
    addFiles([...(input.files ?? [])])
    input.value = ''
  }

  function addFiles(files: File[]): void {
    const added = addDroppedFiles(files, drops)
    drops = added.drops
    dropNotes = added.notes
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

  function select(target: { side: Side; stepId: number } | null): void {
    selection = target
    scene?.setHighlight(target)
    publishDebug({ selection: target, highlightObjects: scene?.highlightObjects() ?? null })
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

  {#if (warning || dropNotes.length > 0) && phase !== 'error'}
    <div class="panel banner">
      {#if warning}<div>{warning}</div>{/if}
      {#if dropNotes.length > 0}
        <div data-testid="drop-notes">
          {#each dropNotes as note, i (i)}<div>{note}</div>{/each}
        </div>
      {/if}
    </div>
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
              {directModified} direct, {transitiveModified} indirect
            {:else}
              modified
            {/if}
          </div>
        </div>
        <div class="stat unch"><div class="num">={index.report.stats.unchanged}</div><div class="microlabel">unchanged</div></div>
      </div>
      {#if modifiedScope}
        <div class="footnote">
          scope: {modifiedScope.intrinsic ?? 0} intrinsic, {modifiedScope.transitive ?? 0} transitive, {modifiedScope.mixed ?? 0} mixed
        </div>
      {/if}
      {#if cohorts.length}
        <span class="microlabel">group moves</span>
        <div class="classrows">
          {#each cohorts as cohort (cohort.delta_mm.join(','))}
            <div class="classrow cohort" title={fmtDelta(cohort.delta_mm)}>
              <span class="cls">{cohort.count} moved together</span>
              <span class="nums mono">Δ {(Math.hypot(...cohort.delta_mm) / 1000).toFixed(2)} m</span>
            </div>
          {/each}
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
      <span class="microlabel">legend</span>
      <div class="legendrows">
        {#if coverage}
          {#each REPORT_KEYS as key (key)}
            <div class="legendrow" data-testid="legend-{key}">
              <span><i class="dot {REPORT_KEY_LEGEND[key].dot}"></i>{REPORT_KEY_LEGEND[key].label}</span>
              <span class="mono"
                >{coverage.inReport[key]} in report{rendererReady ? `, ${coverage.drawn[key]} drawn` : ''}</span
              >
            </div>
          {/each}
        {/if}
        <div class="legendrow" data-testid="legend-extra">
          <span><i class="dot extra"></i>Extra</span>
          <span class="mono"
            >{unreported.old.length} old, {unreported.new} new{rendererReady ? `, ${extraDrawn} drawn` : ''}</span
          >
        </div>
      </div>
      {#if !rendererReady}
        <div class="footnote" data-testid="legend-drawing">3D drawing unavailable: nothing is drawn.</div>
      {/if}
      <div class="footnote">Drawn includes changed void outlines; unchanged and unreported voids are hidden.</div>
      <div class="footnote">Extra: unreported geometry. Non-void new geometry is drawn; old geometry is listed below.</div>
      {#if coverage && coverage.undrawn.length > 0}
        <span class="microlabel">not drawn ({coverage.undrawn.length})</span>
        <div class="footnote">Not drawn: containers without meshes, property-only objects, and unchanged voids.</div>
        <div class="undrawn" data-testid="undrawn">
          {#each coverage.undrawn as entity (entity.side + entity.stepId)}
            <button class="classrow" onclick={() => select({ side: entity.side, stepId: entity.stepId })}>
              <span class="cls"><i class="dot {REPORT_KEY_LEGEND[entity.key].dot}"></i>{entity.name ?? `#${entity.stepId}`}</span>
              <span class="nums mono">{entity.klass}</span>
            </button>
          {/each}
        </div>
      {/if}
      {#if unreported.old.length > 0}
        <span class="microlabel">old file, not in report ({unreported.old.length})</span>
        <div class="footnote">Old-file geometry the report has no record of. Not drawn.</div>
        <div class="undrawn" data-testid="unreported-old">
          {#each unreported.old as entity (entity.stepId)}
            <button class="classrow" onclick={() => select({ side: 'old', stepId: entity.stepId })}>
              <span class="cls"><i class="dot extra"></i>#{entity.stepId}</span>
              <span class="nums mono">{entity.klass}</span>
            </button>
          {/each}
        </div>
      {/if}
      {#if cohorts.length > 0 || index.placementPairs.filter(isPlacementOnly).length > 0}
        <div class="footnote">
          Amber arrows trace placement moves (old → new centroid){cohorts.length
            ? '; each group move collapses to one arrow'
            : ''}.
        </div>
      {/if}
    </aside>

    {#if selection}
      <aside class="panel inspector">
        <button class="close" onclick={() => select(null)} aria-label="Close inspector">✕</button>
        <span class="microlabel">entity</span>
        <div class="title">{selectedRecord?.name ?? `#${selection.stepId}`}</div>
        <div>
          <span class="badge {selectedRecord?.section ?? 'extra'}">{selectedRecord?.section ?? 'extra'}</span>
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
            <dd>{selectedPair.match.reason}, score {selectedPair.match.score}</dd>
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
          <p class="footnote">
            Not in the report: the diff says nothing about this entity. {selection.side === 'new'
              ? 'Drawn in the Extra colour.'
              : 'Old-file geometry without a record is not drawn.'}
          </p>
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
      <button class:off={!toggles.extra} onclick={() => (toggles.extra = !toggles.extra)}>
        <i class="dot extra"></i>Extra
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
      <button onclick={startOver}>Start over</button>
    </div>
  {/if}

  {#if phase === 'empty'}
    <div class="center-stage">
      <div class="panel hero" class:armed={dragging}>
        <h1><em>Athar</em> — semantic IFC diff viewer</h1>
        <p>
          Drop two IFC revisions and the Athar report that compares them. Old and new are taken
          from the file hashes the report records. Everything runs in your browser — models never
          leave this machine.
        </p>
        <div class="dropzones">
          <button
            class="dropzone"
            class:filled={drops.ifcs.length === 2}
            onclick={() => ifcInput.click()}
          >
            <span class="slot">Two .ifc files</span>
            <span class="hint">
              {drops.ifcs.length ? drops.ifcs.map((f) => f.name).join(', ') : 'drop or browse'}
            </span>
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
          <button class="primary" onclick={startOver}>Start over</button>
        </div>
      </div>
    </div>
  {:else if phase === 'confirm' && unverified}
    <div class="center-stage">
      <div class="panel hero">
        <h1>Which file is old?</h1>
        <p class="panel banner warn" role="alert" data-testid="unverified-warning">
          These files could not be verified against the report. {unverified.reason} Assign old and
          new yourself; a wrong assignment shows every change reversed.
        </p>
        <div class="assign">
          {#each unverified.files as hashed, i (i)}
            <div class="assignrow">
              <span class="mono" title={hashed.sha256}>{hashed.file.name}</span>
              <label>
                <input
                  type="radio"
                  name="role-{i}"
                  checked={unverified.oldIndex === i}
                  onchange={() => unverified && (unverified.oldIndex = i as 0 | 1)}
                  aria-label="{hashed.file.name} is old"
                />Old
              </label>
              <label>
                <input
                  type="radio"
                  name="role-{i}"
                  checked={unverified.oldIndex === 1 - i}
                  onchange={() => unverified && (unverified.oldIndex = (1 - i) as 0 | 1)}
                  aria-label="{hashed.file.name} is new"
                />New
              </label>
            </div>
          {/each}
        </div>
        <div class="cta">
          <button class="primary" disabled={unverified.oldIndex === null} onclick={confirmUnverified}
            >Start diff</button
          >
          <button onclick={startOver}>Start over</button>
        </div>
      </div>
    </div>
  {/if}

  <input
    type="file"
    accept=".ifc"
    multiple
    bind:this={ifcInput}
    data-testid="input-ifc"
    onchange={() => onPickedFiles(ifcInput)}
  />
  <input
    type="file"
    accept=".json,application/json"
    bind:this={reportInput}
    data-testid="input-report"
    onchange={() => onPickedFiles(reportInput)}
  />
</main>
