<script lang="ts">
  import { isCleanDiff, statSummary } from '../lib/format'

  let { stats }: { stats: Record<string, unknown> | undefined } = $props()

  const summary = $derived(statSummary(stats))
  const clean = $derived(isCleanDiff(stats))
</script>

{#if clean}
  <span class="chip clean">no changes</span>
{:else}
  {#if summary.added > 0}<span class="chip add">+{summary.added}</span>{/if}
  {#if summary.deleted > 0}<span class="chip del">−{summary.deleted}</span>{/if}
  {#if summary.modified > 0}<span class="chip mod">~{summary.modified}</span>{/if}
{/if}
