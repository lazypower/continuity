<script lang="ts">
  import { categoryColors, type Category } from '../lib/types';

  interface Props {
    uri: string;
    category: string;
    l0_abstract: string;
    l1_overview?: string;
    relevance?: number;
    score?: number;
  }

  let { uri, category, l0_abstract, l1_overview, relevance, score }: Props = $props();

  let expanded = $state(false);

  function toggle() {
    expanded = !expanded;
  }

  function categoryColor(cat: string): string {
    return categoryColors[cat as Category] || 'var(--text-secondary)';
  }

  function slug(u: string): string {
    const parts = u.replace('mem://', '').split('/');
    return parts[parts.length - 1] || u;
  }

  function glowClass(cat: string): string {
    const map: Record<string, string> = {
      profile: 'glow-profile',
      preferences: 'glow-preferences',
      entities: 'glow-entities',
      events: 'glow-events',
      patterns: 'glow-patterns',
      cases: 'glow-cases',
    };
    return map[cat] || '';
  }

  function relevancePercent(r: number | undefined): number {
    if (r == null) return 100;
    return Math.round(r * 100);
  }
</script>

<div
  class="card-container rounded-lg bg-[var(--bg-card)] hover:bg-[var(--bg-card-hover)] cursor-pointer fade-in"
  class:expanded
  style:--cat-color={categoryColor(category)}
  onclick={toggle}
  onkeydown={(e) => e.key === 'Enter' && toggle()}
  role="button"
  tabindex="0"
>
  <!-- Category accent border -->
  <div class="card-accent" style:background-color={categoryColor(category)}></div>

  <div class="card-body p-4">
    <div class="flex items-start justify-between gap-3">
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-2 mb-1.5">
          <span
            class="inline-block px-2 py-0.5 rounded text-xs font-medium"
            style="background-color: color-mix(in srgb, {categoryColor(category)} 20%, transparent); color: {categoryColor(category)};"
          >
            {category}
          </span>
          <span class="text-xs text-[var(--text-secondary)] truncate font-mono opacity-60">{slug(uri)}</span>
          {#if score != null}
            <span class="text-xs font-mono text-[var(--text-secondary)] opacity-60">
              {score.toFixed(2)}
            </span>
          {/if}
        </div>
        <p class="text-sm leading-relaxed">{l0_abstract}</p>
      </div>
      <span class="expand-icon text-[var(--text-secondary)] text-sm shrink-0">
        {expanded ? '\u2212' : '\u002B'}
      </span>
    </div>

    <!-- Relevance bar -->
    {#if relevance != null}
      <div class="mt-2.5 flex items-center gap-2">
        <div class="flex-1 h-[3px] rounded-full bg-[var(--border)] overflow-hidden">
          <div
            class="h-full rounded-full"
            style="width: {relevancePercent(relevance)}%; background: {categoryColor(category)}; opacity: 0.7;"
          ></div>
        </div>
        <span class="text-[10px] font-mono text-[var(--text-secondary)] opacity-50 w-8 text-right">
          {relevancePercent(relevance)}%
        </span>
      </div>
    {/if}

    {#if expanded && l1_overview}
      <div class="mt-3 pt-3 border-t border-[var(--border)] expand-enter">
        <p class="text-sm text-[var(--text-secondary)] whitespace-pre-wrap leading-relaxed">{l1_overview}</p>
      </div>
    {/if}
  </div>
</div>

<style>
  .card-container {
    display: flex;
    border: 1px solid var(--border);
    overflow: hidden;
    position: relative;
  }

  .card-container:hover {
    border-color: var(--border-hover);
  }

  .card-container.expanded:hover,
  .card-container:hover {
    box-shadow: 0 0 16px color-mix(in srgb, var(--cat-color) 12%, transparent);
  }

  .card-accent {
    width: 3px;
    flex-shrink: 0;
    opacity: 0.6;
  }

  .card-container:hover .card-accent {
    opacity: 1;
  }

  .card-body {
    flex: 1;
    min-width: 0;
  }

  .expand-icon {
    transition: transform 150ms ease;
  }

  .card-container.expanded .expand-icon {
    transform: rotate(45deg);
  }
</style>
