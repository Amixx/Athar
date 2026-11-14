<script lang="ts">
  let {
    open,
    current,
    onsave,
    onclose,
  }: {
    open: boolean
    current: string
    onsave: (name: string) => void
    onclose: () => void
  } = $props()

  let dialog: HTMLDialogElement
  let name = $state('')

  $effect(() => {
    if (open && !dialog.open) {
      name = current
      dialog.showModal()
    } else if (!open && dialog.open) {
      dialog.close()
    }
  })

  function submit(event: SubmitEvent): void {
    event.preventDefault()
    if (name.trim()) onsave(name)
  }
</script>

<dialog bind:this={dialog} aria-labelledby="actor-title" onclose={onclose}>
  <form onsubmit={submit}>
    <h3 id="actor-title">Who is reviewing?</h3>
    <p>
      Your name is recorded with every delivery you upload and every decision you make. It is kept
      for this browser session.
    </p>
    <label>
      <span class="eyebrow">Reviewer name</span>
      <!-- svelte-ignore a11y_autofocus -->
      <input bind:value={name} autocomplete="name" autofocus required />
    </label>
    <div class="buttons">
      <button type="button" class="btn" onclick={() => dialog.close()}>Browse without a name</button>
      <button type="submit" class="btn primary" disabled={!name.trim()}>Continue</button>
    </div>
  </form>
</dialog>

<style>
  dialog {
    max-width: 440px;
    border: 1.5px solid var(--rule-strong);
    border-radius: 6px;
    padding: 20px 24px;
    background: var(--surface);
    color: var(--ink);
  }

  dialog::backdrop {
    background: rgba(35, 38, 42, 0.35);
  }

  h3 {
    margin: 0 0 8px;
    font-size: 16px;
  }

  p {
    margin: 0 0 14px;
    color: var(--ink-dim);
    font-size: 13px;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  input {
    border: 1px solid var(--rule);
    border-radius: 3px;
    padding: 6px 10px;
    background: var(--paper);
  }

  .buttons {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 16px;
  }
</style>
