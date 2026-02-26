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
</script>

<div
  class="border border-[var(--border)] rounded-lg p-4 bg-[var(--bg-card)] hover:border-[var(--text-secondary)] transition-colors cursor-pointer"
  style:opacity={relevance != null ? Math.max(0.4, relevance) : 1}
  onclick={toggle}
  onkeydown={(e) => e.key === 'Enter' && toggle()}
  role="button"
  tabindex="0"
>
  <div class="flex items-start justify-between gap-3">
    <div class="flex-1 min-w-0">
      <div class="flex items-center gap-2 mb-1">
        <span
          class="inline-block px-2 py-0.5 rounded text-xs font-medium text-white"
          style:background-color={categoryColor(category)}
        >
          {category}
        </span>
        <span class="text-xs text-[var(--text-secondary)] truncate">{slug(uri)}</span>
        {#if score != null}
          <span class="text-xs text-[var(--text-secondary)]">({score.toFixed(2)})</span>
        {/if}
      </div>
      <p class="text-sm leading-relaxed">{l0_abstract}</p>
    </div>
    <span class="text-[var(--text-secondary)] text-xs shrink-0">{expanded ? '−' : '+'}</span>
  </div>

  {#if expanded && l1_overview}
    <div class="mt-3 pt-3 border-t border-[var(--border)]">
      <p class="text-sm text-[var(--text-secondary)] whitespace-pre-wrap">{l1_overview}</p>
    </div>
  {/if}
</div>
