<script lang="ts">
  import type { ModifiedEntry, Report, ReportEntity, StoreEvent } from '../lib/api'
  import { viewerUrl } from '../lib/api'
  import {
    entityLabel,
    formatBytes,
    formatWhen,
    groupChanges,
    modifiedAspects,
    placementDeltaMm,
    shortSha,
  } from '../lib/format'
  import StatChips from './StatChips.svelte'

  let {
    projectId,
    review,
    report,
    reportError,
    pending,
    busy,
    onresolve,
    onback,
  }: {
    projectId: string
    review: StoreEvent
    report: Report | null
    reportError: string | null
    pending: boolean
    busy: boolean
    onresolve: (action: 'approve' | 'reject', note: string) => void
    onback: () => void
  } = $props()

  let note = $state('')
  let expanded = $state<Record<string, boolean>>({})
  const GROUP_LIMIT = 30

  const groups = $derived(report ? groupChanges(report) : [])
  const kindGlyph = { added: '+', deleted: '−', modified: '~' } as const

  function entryKey(kind: string, entry: ReportEntity | ModifiedEntry, index: number): string {
    return `${kind}:${index}`
  }

  function asModified(entry: ReportEntity | ModifiedEntry): ModifiedEntry {
    return entry as ModifiedEntry
  }

  function asEntity(entry: ReportEntity | ModifiedEntry): ReportEntity {
    return entry as ReportEntity
  }
</script>

<article>
  <header>
    <button class="back" onclick={onback}>← register</button>
    <div class="head-main">
      <div class="eyebrow">Review record</div>
      <h2 class="mono">{review.id}</h2>
      <div class="meta">
        submitted by <strong>{review.actor}</strong> · {formatWhen(review.at)} ·
        <span class="mono">
          {shortSha(review.baseline_artifact?.sha256)} → {shortSha(review.candidate_artifact?.sha256)}
        </span>
        · {formatBytes(review.candidate_artifact?.size_bytes)}
      </div>
      <div class="chips">
        <StatChips stats={review.diff?.stats} />
        {#if review.has_policy}
          <span class="chip" class:add={review.verdict === 'pass'} class:del={review.verdict === 'fail'}>
            policy {review.verdict}
          </span>
        {/if}
      </div>
    </div>
    <div class="head-actions">
      <a class="btn" href={viewerUrl(projectId, review.id)} target="_blank" rel="noopener">
        Open 3D diff
      </a>
    </div>
  </header>

  {#if (review.conflicts?.length ?? 0) > 0}
    <aside class="banner conflict-banner">
      <strong>Overlapping pending work.</strong>
      {#each review.conflicts! as conflict (conflict.review_id)}
        <div>
          Shares {conflict.overlap_count}
          {conflict.overlap_count === 1 ? 'entity' : 'entities'} with
          <span class="mono">{conflict.review_id}</span>{conflict.actor ? ` by ${conflict.actor}` : ''} —
          accepting one will likely invalidate the other.
        </div>
      {/each}
    </aside>
  {/if}

  {#if review.check && !review.check.ok}
    <aside class="banner policy-banner">
      <strong>Policy gate failed.</strong>
      {#each review.check.violations as violation (violation.code)}
        <div><span class="mono">{violation.code}</span> — {violation.message}</div>
      {/each}
    </aside>
  {/if}

  {#if pending}
    <section class="decision">
      <div class="eyebrow">Decision</div>
      <input placeholder="note (optional)" bind:value={note} disabled={busy} />
      <div class="decision-buttons">
        <button class="btn primary" disabled={busy} onclick={() => onresolve('approve', note)}>
          Accept as new baseline
        </button>
        <button class="btn danger" disabled={busy} onclick={() => onresolve('reject', note)}>
          Reject
        </button>
      </div>
    </section>
  {/if}

  {#if reportError}
    <p class="report-missing">{reportError}</p>
  {:else if !report}
    <p class="report-missing">Loading change report…</p>
  {:else if groups.length === 0}
    <p class="report-missing">No semantic entity changes — the model content is identical.</p>
  {:else}
    {#each groups as group (group.kind + group.className)}
      <section class="group">
        <h3>
          <span class="glyph {group.kind}">{kindGlyph[group.kind]}</span>
          {group.className}
          <span class="count mono">{group.entries.length}</span>
        </h3>
        <ul>
          {#each group.entries.slice(0, GROUP_LIMIT) as entry, index (entryKey(group.kind, entry, index))}
            {#if group.kind === 'modified'}
              {@const modified = asModified(entry)}
              {@const aspects = modifiedAspects(modified)}
              {@const delta = placementDeltaMm(modified)}
              {@const deltas = modified.data_delta ?? []}
              {@const key = entryKey(group.kind, entry, index)}
              <li>
                <button
                  class="entry"
                  class:expandable={deltas.length > 0}
                  onclick={() => (expanded[key] = !expanded[key])}
                >
                  <span class="label">{entityLabel(modified.new ?? modified.old)}</span>
                  <span class="aspects mono">
                    {aspects.join(', ')}{delta !== null ? ` · Δ ${delta.toFixed(1)} mm` : ''}
                    {#if modified.change_scope === 'transitive'}· ripple{/if}
                  </span>
                  {#if deltas.length > 0}
                    <span class="disclosure">{expanded[key] ? '▾' : '▸'} {deltas.length}</span>
                  {/if}
                </button>
                {#if expanded[key] && deltas.length > 0}
                  <table class="deltas">
                    <tbody>
                      {#each deltas as change (change.path)}
                        <tr>
                          <td class="path mono">{change.path}</td>
                          <td class="old mono">{change.old ?? '∅'}</td>
                          <td class="arrow">→</td>
                          <td class="new mono">{change.new ?? '∅'}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                {/if}
              </li>
            {:else}
              {@const entity = asEntity(entry)}
              <li>
                <div class="entry static">
                  <span class="label">{entityLabel(entity)}</span>
                  <span class="aspects mono">{entity.guid ?? `#${entity.step_id}`}</span>
                </div>
              </li>
            {/if}
          {/each}
          {#if group.entries.length > GROUP_LIMIT}
            <li class="more">… and {group.entries.length - GROUP_LIMIT} more</li>
          {/if}
        </ul>
      </section>
    {/each}
  {/if}
</article>

<style>
  article {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  header {
    display: flex;
    gap: 16px;
    align-items: flex-start;
  }

  .back {
    border: none;
    background: none;
    color: var(--ink-dim);
    padding: 4px 0;
  }

  .back:hover {
    color: var(--stamp);
  }

  .head-main {
    flex: 1;
  }

  h2 {
    margin: 2px 0 4px;
    font-size: 20px;
  }

  .meta {
    color: var(--ink-dim);
    font-size: 13px;
  }

  .chips {
    display: flex;
    gap: 6px;
    margin-top: 8px;
  }

  .banner {
    padding: 10px 14px;
    border-radius: 4px;
    font-size: 13px;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  .conflict-banner {
    background: var(--c-pend-soft);
    border-left: 3px solid var(--c-pend);
    color: #6d5023;
  }

  .policy-banner {
    background: var(--c-del-soft);
    border-left: 3px solid var(--c-del);
    color: #7c2a2d;
  }

  .decision {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 14px;
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
    border-radius: 4px;
  }

  .decision input {
    flex: 1;
    border: 1px solid var(--rule);
    border-radius: 3px;
    padding: 6px 10px;
    background: var(--paper);
  }

  .decision-buttons {
    display: flex;
    gap: 8px;
  }

  .report-missing {
    color: var(--ink-dim);
    padding: 20px 0;
  }

  .group h3 {
    display: flex;
    align-items: baseline;
    gap: 8px;
    margin: 0 0 6px;
    font-size: 14px;
  }

  .glyph {
    font: 700 14px/1 var(--mono);
  }

  .glyph.added {
    color: var(--c-add);
  }

  .glyph.deleted {
    color: var(--c-del);
  }

  .glyph.modified {
    color: var(--c-mod);
  }

  .count {
    color: var(--ink-faint);
    font-size: 12px;
  }

  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 4px;
  }

  li + li {
    border-top: 1px solid var(--rule);
  }

  .entry {
    display: flex;
    width: 100%;
    align-items: baseline;
    gap: 12px;
    padding: 7px 12px;
    border: none;
    background: none;
    text-align: left;
    cursor: default;
  }

  .entry.expandable {
    cursor: pointer;
  }

  .entry.expandable:hover {
    background: var(--stamp-soft);
  }

  .label {
    flex: 1;
  }

  .aspects {
    color: var(--ink-dim);
    font-size: 12px;
  }

  .disclosure {
    color: var(--ink-faint);
    font-size: 12px;
    white-space: nowrap;
  }

  .deltas {
    width: 100%;
    border-collapse: collapse;
    background: var(--paper);
    font-size: 12px;
  }

  .deltas td {
    padding: 4px 12px;
    border-top: 1px dashed var(--rule);
    vertical-align: top;
  }

  .path {
    color: var(--ink-dim);
    max-width: 340px;
    overflow-wrap: anywhere;
  }

  .old {
    color: var(--c-del);
    text-decoration: line-through;
    overflow-wrap: anywhere;
  }

  .arrow {
    color: var(--ink-faint);
  }

  .new {
    color: var(--c-add);
    overflow-wrap: anywhere;
  }

  .more {
    padding: 7px 12px;
    color: var(--ink-faint);
    font-size: 12.5px;
  }
</style>
