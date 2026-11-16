<script lang="ts">
  import type { ProjectSummary } from '../lib/api'

  let {
    projects,
    selectedProject,
    selectedModel,
    onselect,
    onnew,
  }: {
    projects: ProjectSummary[]
    selectedProject: string | null
    selectedModel: string | null
    onselect: (projectId: string, modelKey: string) => void
    onnew: (projectId: string | null) => void
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
          aria-current={project.project_id === selectedProject && model.model_key === selectedModel
            ? 'page'
            : undefined}
          onclick={() => onselect(project.project_id, model.model_key)}
        >
          <span class="key">{model.model_key}</span>
          {#if model.pending_count > 0}
            <span class="chip pend">{model.pending_count} pending</span>
          {/if}
        </button>
      {:else}
        <div class="empty">No model has an accepted baseline yet.</div>
        <button class="link" onclick={() => onnew(project.project_id)}>Set a first baseline</button>
      {/each}
    </section>
  {:else}
    <div class="empty">No projects in this store yet.</div>
  {/each}
  <button class="btn new" onclick={() => onnew(selectedProject)}>New model lineage</button>
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

  .link {
    border: none;
    background: none;
    color: var(--stamp);
    padding: 2px 8px;
    text-decoration: underline;
  }

  .new {
    font-size: 13px;
    padding: 6px 12px;
  }
</style>
