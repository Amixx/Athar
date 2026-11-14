<script lang="ts">
  import { formatBytes } from '../lib/format'

  let {
    label,
    busyLabel,
    busyFile,
    disabledReason,
    maxBytes,
    onupload,
    onrefuse,
  }: {
    label: string
    busyLabel: string
    busyFile: File | null
    disabledReason: string | null
    maxBytes: number | null
    onupload: (file: File) => void
    onrefuse: (message: string) => void
  } = $props()

  const uid = $props.id()
  const hintId = `${uid}-hint`
  let dragging = $state(false)
  let input: HTMLInputElement
  const busy = $derived(busyFile !== null)
  const inert = $derived(busy || disabledReason !== null)

  function pick(files: FileList | null | undefined) {
    const file = files?.[0]
    if (!file || inert) return
    if (maxBytes !== null && file.size > maxBytes) {
      onrefuse(
        `${file.name} is ${formatBytes(file.size)}, larger than this server's upload limit of ` +
          `${formatBytes(maxBytes)}. Restart athar review with a higher --max-upload-mb to accept it.`,
      )
      return
    }
    onupload(file)
  }

  function browse() {
    if (!inert) input.click()
  }
</script>

<div
  class="zone"
  class:dragging
  class:busy
  class:inert
  role="button"
  tabindex="0"
  aria-label={label}
  aria-disabled={inert}
  aria-describedby={hintId}
  ondragover={(e) => {
    e.preventDefault()
    dragging = !inert
  }}
  ondragleave={() => (dragging = false)}
  ondrop={(e) => {
    e.preventDefault()
    dragging = false
    pick(e.dataTransfer?.files)
  }}
  onclick={browse}
  onkeydown={(e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      browse()
    }
  }}
>
  {#if busyFile}
    <span class="working" role="status">{busyLabel}</span>
    <span class="hint" id={hintId}>
      {busyFile.name}, {formatBytes(busyFile.size)}. Large models can take a few minutes.
    </span>
  {:else}
    <span>{label}</span>
    <span class="hint" id={hintId}>
      {disabledReason ?? 'Drop an .ifc file here, or press Enter to choose one'}
    </span>
  {/if}
</div>
<input
  bind:this={input}
  type="file"
  accept=".ifc"
  hidden
  onchange={(e) => {
    pick((e.target as HTMLInputElement).files)
    ;(e.target as HTMLInputElement).value = ''
  }}
/>

<style>
  .zone {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 3px;
    padding: 18px;
    border: 1.5px dashed var(--ink-faint);
    border-radius: 4px;
    color: var(--ink-dim);
    background: var(--surface);
    text-align: center;
    cursor: pointer;
  }

  .zone:hover:not(.inert),
  .zone.dragging {
    border-color: var(--stamp);
    color: var(--stamp);
    background: var(--stamp-soft);
  }

  .zone.inert {
    cursor: default;
  }

  .zone.inert:not(.busy) {
    opacity: 0.6;
  }

  .zone.busy {
    border-style: solid;
    border-color: var(--stamp);
  }

  .hint {
    font-size: 12px;
    color: var(--ink-faint);
  }

  .working {
    color: var(--stamp);
    font-weight: 600;
  }
</style>
