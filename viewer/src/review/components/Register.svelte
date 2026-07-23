<script lang="ts">
  import type { StoreEvent } from '../lib/api'
  import { describeEvent, formatWhen, revisionLetters } from '../lib/format'
  import StatChips from './StatChips.svelte'

  let {
    events,
    pendingIds,
    onopen,
  }: {
    events: StoreEvent[]
    pendingIds: Set<string>
    onopen: (reviewId: string) => void
  } = $props()

  const letters = $derived(revisionLetters(events))
  const newestFirst = $derived([...events].reverse())
</script>

<table aria-label="Review register">
  <thead>
    <tr>
      <th class="rev">Rev</th>
      <th class="what">Record</th>
      <th class="delta">Changes</th>
      <th class="by">By</th>
      <th class="when">Date</th>
      <th class="status">Status</th>
    </tr>
  </thead>
  <tbody>
    {#each newestFirst as event (event.id)}
      <tr
        class:openable={event.event === 'review'}
        onclick={() => event.event === 'review' && onopen(event.id)}
      >
        <td class="rev mono">{letters.get(event.id) ?? ''}</td>
        <td class="what">
          <div>{describeEvent(event)}</div>
          <div class="sub mono">{event.id}</div>
          {#if event.event === 'review' && (event.conflicts?.length ?? 0) > 0}
            <div class="conflict">
              ⚠ overlaps {event.conflicts!.map((c) => c.review_id).join(', ')}
            </div>
          {/if}
          {#if event.note}
            <div class="sub">“{event.note}”</div>
          {/if}
        </td>
        <td class="delta">
          {#if event.event === 'review'}
            <StatChips stats={event.diff?.stats} />
          {/if}
        </td>
        <td class="by">{event.actor}</td>
        <td class="when mono">{formatWhen(event.at)}</td>
        <td class="status">
          {#if event.event === 'approval' || event.event === 'baseline_set'}
            <span class="stamp">Accepted</span>
          {:else if event.event === 'rejection'}
            <span class="stamp rejected">Rejected</span>
          {:else if event.event === 'review' && pendingIds.has(event.id)}
            <span class="chip pend">awaiting decision</span>
          {:else if event.event === 'review' && event.has_policy}
            <span class="chip" class:add={event.verdict === 'pass'} class:del={event.verdict === 'fail'}>
              policy {event.verdict}
            </span>
          {/if}
        </td>
      </tr>
    {:else}
      <tr><td colspan="6" class="empty">No records yet for this model.</td></tr>
    {/each}
  </tbody>
</table>

<style>
  table {
    width: 100%;
    border-collapse: collapse;
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
  }

  th {
    font: 600 10.5px/1.2 var(--mono);
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--ink-dim);
    text-align: left;
    padding: 8px 12px;
    border-bottom: 1.5px solid var(--rule-strong);
  }

  td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--rule);
    vertical-align: top;
  }

  tbody tr:last-child td {
    border-bottom: none;
  }

  tr.openable {
    cursor: pointer;
  }

  tr.openable:hover td {
    background: var(--stamp-soft);
  }

  .rev {
    width: 42px;
    font-weight: 700;
  }

  .delta {
    white-space: nowrap;
  }

  .sub {
    color: var(--ink-faint);
    font-size: 11.5px;
    margin-top: 2px;
  }

  .conflict {
    color: var(--c-pend);
    font-size: 12px;
    margin-top: 3px;
  }

  .when {
    white-space: nowrap;
    color: var(--ink-dim);
  }

  .status {
    white-space: nowrap;
  }

  .empty {
    color: var(--ink-dim);
    text-align: center;
    padding: 28px;
  }
</style>
