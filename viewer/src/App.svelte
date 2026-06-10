<script lang="ts">
  import { onMount } from 'svelte'
  import { publishDebug } from './lib/debug'
  import { tessellate } from './lib/ifc'
  import { buildDiffIndex, parseReport, type DiffIndex } from './lib/report'
  import {
    bucketFor,
    computeAppearances,
    countBucketEntities,
    countMeshless,
    DEFAULT_TOGGLES,
    isPlacementOnly,
    SNAP,
    type Toggles,
  } from './lib/renderState'
  import { DiffScene } from './lib/scene'
  import {
    classifyDrop,
    loadFromFiles,
    loadFromSrc,
    type DroppedFiles,
    type LoadedInputs,
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

  let index = $state<DiffIndex | null>(null)
  let scene: DiffScene | null = null
  let canvasEl: HTMLCanvasElement

  let oldInput: HTMLInputElement
  let newInput: HTMLInputElement
  let reportInput: HTMLInputElement

  const appearances = $derived(computeAppearances(sliderT, toggles))

  $effect(() => {
    if (phase === 'ready' && scene) {
      scene.applyAppearances(appearances, showLines)
      publishDebug({ sliderT, appearances })
    }
  })

  const complete = $derived(Boolean(drops.oldFile && drops.newFile && drops.reportFile))

  $effect(() => {
    if (phase === 'empty' && complete) {
      void loadFromFiles(drops as Required<DroppedFiles>).then(start, fail)
    }
  })

  onMount(() => {
    publishDebug({ phase: 'empty' })
    const src = new URLSearchParams(location.search).get('src')
    if (src) {
      phase = 'loading'
      progress = { stage: `Fetching from ${src}`, count: 0 }
      publishDebug({ phase: 'loading' })
      loadFromSrc(src).then(start, fail)
    }
    return () => scene?.dispose()
  })

  function fail(cause: unknown): void {
    error = cause instanceof Error ? cause.message : String(cause)
    phase = 'error'
    publishDebug({ phase: 'error', error })
  }

  async function start(inputs: LoadedInputs): Promise<void> {
    try {
      phase = 'loading'
      warning = inputs.warning ?? ''
      names = { old: inputs.oldName, new: inputs.newName }
      publishDebug({ phase: 'loading', warning })

      const report = parseReport(inputs.reportJson)
      index = buildDiffIndex(report)

      progress = { stage: `Tessellating ${inputs.oldName}`, count: 0 }
      const oldGeometry = await tessellate(inputs.oldBytes, {
        onProgress: (count) => (progress = { stage: `Tessellating ${inputs.oldName}`, count }),
      })
      progress = { stage: `Tessellating ${inputs.newName}`, count: 0 }
      const newGeometry = await tessellate(inputs.newBytes, {
        sharedRtcOffset: oldGeometry.rtcOffset,
        onProgress: (count) => (progress = { stage: `Tessellating ${inputs.newName}`, count }),
      })

      const diff = index
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
      }

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
      })
    } catch (cause) {
      fail(cause)
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
</script>

<main
  class="app"
  ondragover={(e) => {
    e.preventDefault()
    if (phase === 'empty') dragging = true
  }}
  ondragleave={() => (dragging = false)}
  ondrop={onPageDrop}
>
  <div class="viewport">
    <canvas bind:this={canvasEl}></canvas>
  </div>

  {#if warning && phase !== 'error'}
    <div class="panel banner">{warning}</div>
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
      <div class="panel counts">
        <span><i class="dot add"></i>+{index.report.stats.added}</span>
        <span><i class="dot del"></i>−{index.report.stats.deleted}</span>
        <span><i class="dot mod"></i>~{index.report.stats.modified}</span>
        <span><i class="dot unch"></i>={index.report.stats.unchanged}</span>
      </div>
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
