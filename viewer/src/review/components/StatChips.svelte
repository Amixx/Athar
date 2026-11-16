<script lang="ts">
  import { formatCount, isCleanDiff, statSummary } from '../lib/format'

  let { stats }: { stats: Record<string, unknown> | undefined } = $props()

  const summary = $derived(statSummary(stats))
  const clean = $derived(isCleanDiff(stats))
</script>

{#if clean}
  <span class="chip clean">no changes</span>
{:else}
  {#if summary.deleted > 0}
    <span class="chip del" title="deleted elements">−{formatCount(summary.deleted)}</span>
  {/if}
  {#if summary.added > 0}
    <span class="chip add" title="added elements">+{formatCount(summary.added)}</span>
  {/if}
  {#if summary.modified > 0}
    <span class="chip mod" title="modified elements">~{formatCount(summary.modified)}</span>
  {/if}
{/if}
