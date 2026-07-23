<script lang="ts">
  import type { ProjectSummary } from '../lib/api'

  let {
    projects,
    selectedProject,
    selectedModel,
    onselect,
  }: {
    projects: ProjectSummary[]
    selectedProject: string | null
    selectedModel: string | null
    onselect: (projectId: string, modelKey: string) => void
  } = $props()
</script>

<nav aria-label="Projects and model lineages">
  {#each projects as project (project.project_id)}
    <section>
      <div class="eyebrow project-name">{project.name}</div>
      {#each project.models as model (model.model_key)}
        <button
          class="model"
          class:active={project.project_id === selectedProject && model.model_key === selectedModel}
          onclick={() => onselect(project.project_id, model.model_key)}
        >
          <span class="key">{model.model_key}</span>
          {#if model.pending_count > 0}
            <span class="chip pend">{model.pending_count} pending</span>
          {/if}
        </button>
      {:else}
        <div class="empty">no model lineages yet</div>
      {/each}
    </section>
  {:else}
    <div class="empty">
      No projects in this store yet. Upload a first accepted model below, or seed one with
      <span class="mono">athar store set-baseline</span>.
    </div>
  {/each}
</nav>

<style>
  nav {
    display: flex;
    flex-direction: column;
    gap: 18px;
  }

  .project-name {
    color: var(--ink);
    padding-bottom: 6px;
    border-bottom: 1.5px solid var(--rule-strong);
    margin-bottom: 6px;
  }

  .model {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    padding: 7px 8px;
    border: none;
    border-left: 2px solid transparent;
    border-radius: 0 3px 3px 0;
    background: none;
    color: var(--ink);
    text-align: left;
  }

  .model:hover {
    background: var(--stamp-soft);
  }

  .model.active {
    border-left-color: var(--stamp);
    background: var(--stamp-soft);
    font-weight: 600;
  }

  .key {
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .empty {
    color: var(--ink-dim);
    font-size: 13px;
    padding: 4px 8px;
  }
</style>
