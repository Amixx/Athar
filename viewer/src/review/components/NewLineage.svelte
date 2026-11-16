<script lang="ts">
  import { untrack } from 'svelte'
  import UploadZone from './UploadZone.svelte'

  const ID_PATTERN = /^[A-Za-z0-9._-]+$/

  let {
    initialProject,
    actor,
    uploading,
    error,
    maxBytes,
    onupload,
    onrefuse,
  }: {
    initialProject: string | null
    actor: string
    uploading: File | null
    error: string | null
    maxBytes: number | null
    onupload: (projectId: string, modelKey: string, file: File) => void
    onrefuse: (message: string) => void
  } = $props()

  let projectId = $state(untrack(() => initialProject) ?? '')
  let modelKey = $state('')

  const problem = $derived(
    !actor.trim()
      ? 'Set your reviewer name first: it is recorded as the person who accepted this baseline.'
      : !ID_PATTERN.test(projectId)
        ? 'Name the project with letters, digits, dot, dash or underscore (for example acme-tower).'
        : !ID_PATTERN.test(modelKey)
          ? 'Name the model with letters, digits, dot, dash or underscore (for example architecture).'
          : null,
  )
</script>

<section class="new" aria-labelledby="new-title">
  <h2 id="new-title">Start a model lineage</h2>
  <p>
    A lineage is one discipline model of a project, for example the architecture model of a tower.
    Its first accepted file becomes the baseline that every later delivery is compared with.
  </p>
  <div class="fields">
    <label>
      <span class="eyebrow">Project</span>
      <input bind:value={projectId} placeholder="acme-tower" disabled={uploading !== null} />
    </label>
    <label>
      <span class="eyebrow">Model</span>
      <input bind:value={modelKey} placeholder="architecture" disabled={uploading !== null} />
    </label>
  </div>
  <UploadZone
    label="Drop the first accepted IFC file to set the baseline"
    busyLabel="Reading the model and storing it as the baseline…"
    busyFile={uploading}
    disabledReason={problem}
    {maxBytes}
    onupload={(file) => onupload(projectId, modelKey, file)}
    {onrefuse}
  />
  {#if error}
    <div class="error" role="alert">{error}</div>
  {/if}
</section>

<style>
  .new {
    display: flex;
    flex-direction: column;
    gap: 12px;
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
    border-radius: 4px;
    padding: 24px;
  }

  h2 {
    margin: 0;
  }

  p {
    margin: 0;
    color: var(--ink-dim);
  }

  .fields {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: 4px;
    flex: 1;
    min-width: 160px;
  }

  input {
    border: 1px solid var(--rule);
    border-radius: 3px;
    padding: 6px 10px;
    background: var(--paper);
  }

  .error {
    padding: 10px 14px;
    border-radius: 4px;
    background: var(--c-del-soft);
    border-left: 3px solid var(--c-del);
    color: #7c2a2d;
    font-size: 13px;
  }
</style>
