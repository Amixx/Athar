<script lang="ts">
  import { tick } from 'svelte'
  import type { ApiError, ModifiedEntry, Report, ReportEntity, StoreEvent } from '../lib/api'
  import { viewerUrl } from '../lib/api'
  import type { ReviewState } from '../lib/format'
  import {
    entityLabel,
    formatBytes,
    formatCount,
    formatLengthMm,
    formatVectorMm,
    formatWhen,
    groupChanges,
    modifiedAspects,
    placementCohorts,
    placementDeltaMm,
    propertyRows,
    shortSha,
  } from '../lib/format'
  import StatChips from './StatChips.svelte'

  const PAGE = 50

  let {
    projectId,
    modelKey,
    review,
    report,
    reportError,
    reviewState,
    states,
    actor,
    busy,
    decisionError,
    registerHref,
    reviewHref,
    onresolve,
    onaskname,
    onretryreport,
  }: {
    projectId: string
    modelKey: string
    review: StoreEvent
    report: Report | null
    reportError: ApiError | null
    reviewState: ReviewState
    states: Map<string, ReviewState>
    actor: string
    busy: boolean
    decisionError: string | null
    registerHref: string
    reviewHref: (reviewId: string) => string
    onresolve: (action: 'approve' | 'reject', note: string) => Promise<boolean>
    onaskname: () => void
    onretryreport: () => void
  } = $props()

  let note = $state('')
  let confirming = $state<'approve' | 'reject' | null>(null)
  let dialog: HTMLDialogElement
  let cancelButton = $state<HTMLButtonElement | null>(null)
  let copied = $state<'copied' | 'failed' | null>(null)
  let openCohorts = $state<Record<number, boolean>>({})
  let shown = $state<Record<string, number>>({})

  const deliveryName = $derived(review.source_name || 'Delivery (file name not recorded)')
  const named = $derived(actor.trim().length > 0)
  const decisionReady = $derived(report !== null || reportError?.code === 'report_not_stored')
  const cohortView = $derived(report ? placementCohorts(report) : { cohorts: [], moveOnly: new Set<number>() })
  const cohortOf = $derived(
    new Map(cohortView.cohorts.flatMap((cohort, index) => cohort.members.map((m) => [m.new.step_id, index]))),
  )
  const groups = $derived(report ? groupChanges(report, cohortView.moveOnly) : [])
  const kindGlyph = { added: '+', deleted: '−', modified: '~' } as const
  const kindWord = { added: 'added', deleted: 'deleted', modified: 'modified' } as const

  function limit(key: string): number {
    return shown[key] ?? PAGE
  }

  function more(key: string): void {
    shown[key] = limit(key) + PAGE
  }

  function asModified(entry: ReportEntity | ModifiedEntry): ModifiedEntry {
    return entry as ModifiedEntry
  }

  function asEntity(entry: ReportEntity | ModifiedEntry): ReportEntity {
    return entry as ReportEntity
  }

  function entityId(entity: ReportEntity): string {
    return entity.guid ?? `#${entity.step_id}`
  }

  async function ask(action: 'approve' | 'reject'): Promise<void> {
    confirming = action
    dialog.showModal()
    await tick()
    cancelButton?.focus()
  }

  async function confirm(): Promise<void> {
    const action = confirming
    dialog.close()
    if (action && (await onresolve(action, note.trim()))) note = ''
  }

  async function copySha(): Promise<void> {
    try {
      await navigator.clipboard.writeText(review.candidate_artifact?.sha256 ?? '')
      copied = 'copied'
    } catch {
      copied = 'failed'
    }
  }
</script>

<article aria-labelledby="review-title">
  <header>
    <a class="back" href={registerHref}>← Back to the register</a>
    <div class="eyebrow">Review {review.id} of {projectId}, {modelKey}</div>
    <h2 id="review-title">{deliveryName}</h2>
    <dl class="provenance">
      <div>
        <dt>Submitted by</dt>
        <dd>{review.actor}</dd>
      </div>
      <div>
        <dt>Recorded</dt>
        <dd>{formatWhen(review.at)}</dd>
      </div>
      <div>
        <dt>Delivery sha256</dt>
        <dd>
          <span class="mono" title={review.candidate_artifact?.sha256}>
            {shortSha(review.candidate_artifact?.sha256)}
          </span>
          <button class="copy" onclick={copySha} aria-label="Copy the full sha256 of the delivery">
            {copied === 'copied' ? 'Copied' : 'Copy'}
          </button>
          {#if copied === 'failed'}
            <span class="copy-failed">
              Copy is blocked here; the full hash is
              <span class="mono select">{review.candidate_artifact?.sha256}</span>
            </span>
          {/if}
        </dd>
      </div>
      <div>
        <dt>File</dt>
        <dd>
          {review.candidate_artifact?.schema ?? 'unknown schema'}, {formatBytes(review.candidate_artifact?.size_bytes)}
        </dd>
      </div>
      <div>
        <dt>Compared with baseline</dt>
        <dd class="mono" title={review.baseline_artifact?.sha256}>{shortSha(review.baseline_artifact?.sha256)}</dd>
      </div>
      <div>
        <dt>Produced by</dt>
        <dd>
          {#if review.athar_version}
            Athar {review.athar_version}, canon {review.canon_version ?? 'unknown'}
          {:else}
            Athar version not recorded
          {/if}
        </dd>
      </div>
    </dl>
    <div class="chips">
      <StatChips stats={report?.stats ?? review.diff?.stats} />
      {#if review.has_policy}
        <span class="chip" class:add={review.verdict === 'pass'} class:del={review.verdict === 'fail'}>
          policy {review.verdict}
        </span>
      {/if}
      {#if report}
        <a class="btn small" href={viewerUrl(projectId, review.id)} target="_blank" rel="noopener">
          Open 3D diff
        </a>
      {/if}
    </div>
  </header>

  {#if decisionError}
    <div class="banner refused" role="alert">
      <strong>The decision was not recorded.</strong>
      <span>{decisionError}</span>
    </div>
  {/if}

  {#if reviewState.state === 'approved'}
    <aside class="banner decided">
      <span class="stamp">Accepted</span>
      <span>Accepted as the baseline by {reviewState.actor}, {formatWhen(reviewState.at)}, in {reviewState.eventId}.</span>
    </aside>
  {:else if reviewState.state === 'rejected'}
    <aside class="banner decided">
      <span class="stamp rejected">Rejected</span>
      <span>Rejected by {reviewState.actor}, {formatWhen(reviewState.at)}, in {reviewState.eventId}.</span>
    </aside>
  {:else if reviewState.state === 'stale'}
    <aside class="banner stale">
      <strong>Superseded: this review can no longer be accepted or rejected.</strong>
      <span>
        The baseline changed in {reviewState.eventId} ({reviewState.actor}, {formatWhen(reviewState.at)}) after this
        delivery was compared. To decide on it, upload the same file again from
        <a href={registerHref}>the register</a> to compare it with the current baseline.
      </span>
    </aside>
  {/if}

  {#if (review.conflicts?.length ?? 0) > 0}
    <aside class="banner conflict-banner">
      <strong>Overlapping work in other reviews.</strong>
      {#each review.conflicts! as conflict (conflict.review_id)}
        {@const other = states.get(conflict.review_id)}
        <div>
          Changes {formatCount(conflict.overlap_count)}
          {conflict.overlap_count === 1 ? 'element' : 'elements'} that
          <a href={reviewHref(conflict.review_id)}>{conflict.review_id}</a>{conflict.actor
            ? ` by ${conflict.actor}`
            : ''} also changes.
          {#if other?.state === 'pending'}
            Both await a decision; accepting one supersedes the other.
          {:else if other?.state === 'approved'}
            That review was accepted in {other.eventId}.
          {:else if other?.state === 'rejected'}
            That review was rejected in {other.eventId}.
          {:else if other?.state === 'stale'}
            That review was superseded in {other.eventId}.
          {/if}
        </div>
      {/each}
    </aside>
  {/if}

  {#if review.check && !review.check.ok}
    <aside class="banner policy-banner">
      <strong>Policy gate failed.</strong>
      {#each review.check.violations as violation, index (index)}
        <div><span class="mono">{violation.code}</span>: {violation.message}</div>
      {/each}
    </aside>
  {/if}

  {#if reviewState.state === 'pending'}
    <section class="decision" aria-label="Decision">
      <div class="decision-who">
        <div class="eyebrow">Decision</div>
        {#if named}
          <span>Recorded as <strong>{actor}</strong></span>
        {:else}
          <span class="needs-name">
            Set your reviewer name to accept or reject.
            <button class="link" onclick={onaskname}>Set name</button>
          </span>
        {/if}
      </div>
      <label class="note">
        <span class="visually-hidden">Note for the record (optional)</span>
        <input placeholder="Note for the record (optional)" bind:value={note} disabled={busy} />
      </label>
      <div class="decision-buttons">
        <button class="btn primary" disabled={busy || !named || !decisionReady} onclick={() => ask('approve')}>
          Accept as new baseline
        </button>
        <button class="btn danger" disabled={busy || !named || !decisionReady} onclick={() => ask('reject')}>Reject</button>
      </div>
    </section>
  {/if}

  <dialog
    bind:this={dialog}
    aria-labelledby="confirm-title"
    onclose={() => (confirming = null)}
  >
    {#if confirming}
      <h3 id="confirm-title">
        {confirming === 'approve' ? 'Accept this delivery as the new baseline?' : 'Reject this delivery?'}
      </h3>
      <p>
        <strong>{deliveryName}</strong> (review {review.id})
        {confirming === 'approve'
          ? `becomes the accepted baseline of ${projectId}, ${modelKey}. Later deliveries are compared with it.`
          : `is recorded as rejected. The baseline of ${projectId}, ${modelKey} stays as it is.`}
      </p>
      <p class="dim">
        Recorded as {actor}{note.trim() ? `, with the note “${note.trim()}”` : ', without a note'}.
      </p>
      <div class="dialog-buttons">
        <button class="btn" bind:this={cancelButton} onclick={() => dialog.close()}>Cancel</button>
        <button class="btn {confirming === 'approve' ? 'primary' : 'danger'}" onclick={confirm}>
          {confirming === 'approve' ? `Accept ${deliveryName}` : `Reject ${deliveryName}`}
        </button>
      </div>
    {/if}
  </dialog>

  {#if reportError}
    {#if reportError.code === 'report_not_stored'}
      <p class="report-missing">
        This review was recorded before change reports were kept, so only the summary counts above
        exist.
      </p>
    {:else}
      <p class="report-missing" role="alert">
        The change report could not be loaded: {reportError.message}
        <button class="btn small" onclick={onretryreport}>Retry</button>
      </p>
    {/if}
  {:else if !report}
    <p class="report-missing" role="status">Loading the change report…</p>
  {:else if groups.length === 0 && cohortView.cohorts.length === 0}
    <p class="report-missing">No element changed: the delivery matches the baseline.</p>
  {:else}
    {#if cohortView.cohorts.length > 0}
      <section class="group" aria-labelledby="bulk-moves">
        <h3 id="bulk-moves"><span class="glyph modified">⇢</span> Bulk moves</h3>
        <ul>
          {#each cohortView.cohorts as cohort, index (index)}
            {@const key = `cohort:${index}`}
            <li>
              <button
                class="entry expandable"
                aria-expanded={openCohorts[index] === true}
                onclick={() => (openCohorts[index] = !openCohorts[index])}
              >
                <span class="disclosure" aria-hidden="true">{openCohorts[index] ? '▾' : '▸'}</span>
                <span class="label">
                  {formatCount(cohort.count)} elements moved by {formatVectorMm(cohort.deltaMm)}
                </span>
              </button>
              {#if openCohorts[index]}
                <ul class="members">
                  {#each cohort.members.slice(0, limit(key)) as member (member.new.step_id)}
                    <li class="member">
                      <span class="label">{entityLabel(member.new)}</span>
                      <span class="aspects mono">{entityId(member.new)}</span>
                    </li>
                  {/each}
                  {#if cohort.members.length > limit(key)}
                    <li class="more">
                      <button class="btn small" onclick={() => more(key)}>
                        Show {formatCount(Math.min(PAGE, cohort.members.length - limit(key)))} more of
                        {formatCount(cohort.members.length)}
                      </button>
                    </li>
                  {/if}
                </ul>
              {/if}
            </li>
          {/each}
        </ul>
      </section>
    {/if}

    {#each groups as group (group.kind + group.className)}
      {@const key = `${group.kind}:${group.className}`}
      <section class="group">
        <h3>
          <span class="glyph {group.kind}" aria-hidden="true">{kindGlyph[group.kind]}</span>
          {group.className}
          <span class="count">{formatCount(group.entries.length)} {kindWord[group.kind]}</span>
        </h3>
        <ul>
          {#each group.entries.slice(0, limit(key)) as entry, index (`${key}:${index}`)}
            {#if group.kind === 'modified'}
              {@const modified = asModified(entry)}
              {@const aspects = modifiedAspects(modified)}
              {@const moved = placementDeltaMm(modified)}
              {@const rows = propertyRows(modified)}
              {@const raw = modified.data_delta ?? []}
              <li>
                <div class="entry">
                  <span class="label">{entityLabel(modified.new ?? modified.old)}</span>
                  <span class="aspects">
                    {aspects.join(', ')} changed{#if moved !== null}, moved {formatLengthMm(moved)}{/if}{#if modified.change_scope === 'transitive'}, through a related element{/if}{#if cohortOf.has(modified.new.step_id)}, part of a bulk move{/if}
                  </span>
                  <span class="aspects mono">{entityId(modified.new)}</span>
                </div>
                {#if rows.length > 0}
                  <table class="props">
                    <tbody>
                      {#each rows as row (`${row.kind}:${row.pset}:${row.name}:${row.inherited}`)}
                        <tr class={row.kind}>
                          <th scope="row">
                            {row.pset}, {row.name}{#if row.inherited}<span class="dim"> (from type)</span>{/if}
                          </th>
                          <td class="old">{row.before}</td>
                          <td class="arrow">to</td>
                          <td class="new">{row.after}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                {/if}
                {#if raw.length > 0}
                  <details class="raw">
                    <summary>
                      Data changes ({formatCount(raw.length)})
                    </summary>
                    <table class="deltas">
                      <tbody>
                        {#each raw as change (change.path)}
                          <tr>
                            <td class="path mono">{change.path}</td>
                            <td class="old mono">{change.old ?? 'not set'}</td>
                            <td class="arrow">to</td>
                            <td class="new mono">{change.new ?? 'not set'}</td>
                          </tr>
                        {/each}
                      </tbody>
                    </table>
                  </details>
                {/if}
              </li>
            {:else}
              {@const entity = asEntity(entry)}
              <li>
                <div class="entry">
                  <span class="label">{entityLabel(entity)}</span>
                  <span class="aspects mono">{entityId(entity)}</span>
                </div>
              </li>
            {/if}
          {/each}
          {#if group.entries.length > limit(key)}
            <li class="more">
              <button class="btn small" onclick={() => more(key)}>
                Show {formatCount(Math.min(PAGE, group.entries.length - limit(key)))} more of
                {formatCount(group.entries.length)}
              </button>
            </li>
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
    flex-direction: column;
    gap: 4px;
  }

  .back {
    color: var(--ink-dim);
    font-size: 13px;
    margin-bottom: 6px;
  }

  .back:hover {
    color: var(--stamp);
  }

  h2 {
    margin: 0 0 6px;
    font-size: 20px;
    overflow-wrap: anywhere;
  }

  .provenance {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 8px 20px;
    margin: 0;
    padding: 10px 14px;
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 4px;
  }

  .provenance dt {
    font: 600 10px/1.4 var(--mono);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--ink-dim);
  }

  .provenance dd {
    margin: 0;
    font-size: 13px;
    overflow-wrap: anywhere;
  }

  .copy {
    margin-left: 6px;
    padding: 0 6px;
    border: 1px solid var(--rule);
    border-radius: 3px;
    background: var(--paper);
    font-size: 11.5px;
  }

  .copy-failed {
    display: block;
    color: var(--ink-dim);
    font-size: 12px;
  }

  .select {
    user-select: all;
  }

  .chips {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 8px;
  }

  .btn.small {
    padding: 3px 10px;
    font-size: 12.5px;
    text-decoration: none;
  }

  .chips .btn.small {
    margin-left: auto;
  }

  .banner {
    padding: 10px 14px;
    border-radius: 4px;
    font-size: 13px;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  .banner.decided {
    flex-direction: row;
    align-items: center;
    gap: 14px;
    background: var(--surface);
    border: 1px solid var(--rule);
  }

  .banner.stale {
    background: rgba(0, 0, 0, 0.05);
    border-left: 3px solid var(--ink-dim);
  }

  .banner.refused,
  .policy-banner {
    background: var(--c-del-soft);
    border-left: 3px solid var(--c-del);
    color: #7c2a2d;
  }

  .conflict-banner {
    background: var(--c-pend-soft);
    border-left: 3px solid var(--c-pend);
    color: #6d5023;
  }

  .decision {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
    padding: 12px 14px;
    background: var(--surface);
    border: 1.5px solid var(--rule-strong);
    border-radius: 4px;
  }

  .decision-who {
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 13px;
  }

  .needs-name {
    color: var(--c-pend);
  }

  .note {
    flex: 1;
    min-width: 180px;
  }

  .note input {
    width: 100%;
    border: 1px solid var(--rule);
    border-radius: 3px;
    padding: 6px 10px;
    background: var(--paper);
  }

  .decision-buttons,
  .dialog-buttons {
    display: flex;
    gap: 8px;
  }

  .dialog-buttons {
    justify-content: flex-end;
    margin-top: 16px;
  }

  dialog {
    max-width: 480px;
    border: 1.5px solid var(--rule-strong);
    border-radius: 6px;
    padding: 20px 24px;
    background: var(--surface);
    color: var(--ink);
  }

  dialog::backdrop {
    background: rgba(35, 38, 42, 0.35);
  }

  dialog h3 {
    margin: 0 0 10px;
    font-size: 16px;
  }

  dialog p {
    margin: 0 0 6px;
    overflow-wrap: anywhere;
  }

  .dim {
    color: var(--ink-dim);
  }

  .link {
    border: none;
    background: none;
    padding: 0;
    color: var(--stamp);
    text-decoration: underline;
  }

  .visually-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  .report-missing {
    display: flex;
    align-items: center;
    gap: 12px;
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
    color: var(--ink-dim);
    font-size: 12px;
    font-weight: 400;
  }

  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 4px;
  }

  ul.members {
    border: none;
    border-top: 1px dashed var(--rule);
    border-radius: 0;
    background: var(--paper);
  }

  li + li {
    border-top: 1px solid var(--rule);
  }

  .entry,
  .member {
    display: flex;
    width: 100%;
    align-items: baseline;
    gap: 12px;
    padding: 7px 12px;
    border: none;
    background: none;
    text-align: left;
    color: inherit;
  }

  .member {
    padding-left: 32px;
    font-size: 13px;
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
    color: var(--ink-dim);
    font-size: 12px;
    white-space: nowrap;
  }

  .props,
  .deltas {
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
  }

  .props th,
  .props td,
  .deltas td {
    padding: 4px 12px;
    border-top: 1px dashed var(--rule);
    vertical-align: top;
    text-align: left;
  }

  .props th {
    font-weight: 600;
    width: 45%;
    padding-left: 24px;
  }

  .props .old,
  .props .new {
    width: 26%;
  }

  .props .arrow,
  .deltas .arrow {
    width: 1%;
    white-space: nowrap;
  }

  .raw summary {
    padding: 4px 12px 4px 24px;
    color: var(--ink-dim);
    font-size: 12px;
    cursor: pointer;
  }

  .deltas {
    background: var(--paper);
    font-size: 12px;
  }

  .path {
    color: var(--ink-dim);
    max-width: 340px;
    overflow-wrap: anywhere;
  }

  .old {
    color: var(--c-del);
    overflow-wrap: anywhere;
  }

  tr.changed .old,
  .deltas .old {
    text-decoration: line-through;
  }

  .arrow {
    color: var(--ink-dim);
  }

  .new {
    color: var(--c-add);
    overflow-wrap: anywhere;
  }

  .more {
    padding: 7px 12px;
  }
</style>
