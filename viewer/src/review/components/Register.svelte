<script lang="ts">
  import type { StoreEvent } from '../lib/api'
  import type { ReviewState } from '../lib/format'
  import { describeEvent, formatCount, formatWhen, revisionLetters } from '../lib/format'
  import StatChips from './StatChips.svelte'

  const PAGE = 25

  let {
    events,
    states,
    reviewHref,
  }: {
    events: StoreEvent[]
    states: Map<string, ReviewState>
    reviewHref: (reviewId: string) => string
  } = $props()

  let shown = $state(PAGE)
  const letters = $derived(revisionLetters(events))
  const newestFirst = $derived([...events].reverse())
  const visible = $derived(newestFirst.slice(0, shown))
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
    {#each visible as event (event.id)}
      {@const state = states.get(event.id)}
      <tr class:openable={event.event === 'review'}>
        <td class="rev mono">{letters.get(event.id) ?? ''}</td>
        <td class="what">
          {#if event.event === 'review'}
            <a class="open" href={reviewHref(event.id)}>{describeEvent(event)}</a>
          {:else}
            <div>{describeEvent(event)}</div>
          {/if}
          <div class="sub">
            <span class="mono">{event.id}</span>
            {#if event.source_name}<span class="file">{event.source_name}</span>{/if}
          </div>
          {#if event.event === 'review' && (event.conflicts?.length ?? 0) > 0}
            <div class="conflict">
              Overlaps {event.conflicts!.map((c) => c.review_id).join(', ')}
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
          {:else if state?.state === 'pending'}
            <span class="chip pend">awaiting decision</span>
          {:else if state?.state === 'stale'}
            <span class="chip clean" title="The baseline changed after this review">superseded</span>
          {:else if state?.state === 'approved'}
            <span class="chip add">accepted</span>
          {:else if state?.state === 'rejected'}
            <span class="chip del">rejected</span>
          {/if}
          {#if event.event === 'review' && event.has_policy}
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
{#if newestFirst.length > shown}
  <div class="pager">
    <span>Showing the newest {formatCount(shown)} of {formatCount(newestFirst.length)} records.</span>
    <button class="btn" onclick={() => (shown += PAGE)}>Show {PAGE} older</button>
  </div>
{/if}

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

  tr.openable:hover td {
    background: var(--stamp-soft);
  }

  .open {
    color: var(--ink);
    font-weight: 600;
  }

  .open:hover {
    color: var(--stamp);
  }

  .rev {
    width: 42px;
    font-weight: 700;
  }

  .delta {
    white-space: nowrap;
  }

  .sub {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    color: var(--ink-faint);
    font-size: 11.5px;
    margin-top: 2px;
  }

  .file {
    color: var(--ink-dim);
    overflow-wrap: anywhere;
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

  .status .chip + .chip {
    margin-left: 4px;
  }

  .empty {
    color: var(--ink-dim);
    text-align: center;
    padding: 28px;
  }

  .pager {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    color: var(--ink-dim);
    font-size: 13px;
  }
</style>
