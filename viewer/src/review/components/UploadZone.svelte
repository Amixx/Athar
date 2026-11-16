<script lang="ts">
  let {
    label,
    busy,
    onupload,
  }: {
    label: string
    busy: boolean
    onupload: (file: File) => void
  } = $props()

  let dragging = $state(false)
  let input: HTMLInputElement

  function pick(files: FileList | null | undefined) {
    const file = files?.[0]
    if (file) onupload(file)
  }
</script>

<div
  class="zone"
  class:dragging
  class:busy
  role="button"
  tabindex="0"
  aria-label={label}
  ondragover={(e) => {
    e.preventDefault()
    dragging = true
  }}
  ondragleave={() => (dragging = false)}
  ondrop={(e) => {
    e.preventDefault()
    dragging = false
    pick(e.dataTransfer?.files)
  }}
  onclick={() => !busy && input.click()}
  onkeydown={(e) => e.key === 'Enter' && !busy && input.click()}
>
  {#if busy}
    <span class="working">Reviewing candidate — diffing against the baseline…</span>
  {:else}
    <span>{label}</span>
    <span class="hint">drop an .ifc here or click to browse</span>
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
  }

  .zone:hover,
  .zone.dragging {
    border-color: var(--stamp);
    color: var(--stamp);
    background: var(--stamp-soft);
  }

  .zone.busy {
    cursor: default;
    border-style: solid;
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
