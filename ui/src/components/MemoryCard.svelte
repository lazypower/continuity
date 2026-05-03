<script lang="ts">
  import { onDestroy } from 'svelte';
  import { categoryColors, type Category } from '../lib/types';

  interface Props {
    uri: string;
    category: string;
    l0_abstract: string;
    l1_overview?: string;
    relevance?: number;
    score?: number;
    // When true, the memory has been retracted. The card displays a marker
    // and suppresses content. The API default paths shouldn't deliver
    // retracted nodes here, but this guard makes the component correct
    // regardless of upstream filtering — see issue #12.
    retracted?: boolean;
  }

  let { uri, category, l0_abstract, l1_overview, relevance, score, retracted }: Props = $props();

  let expanded = $state(false);
  let copyState = $state<'idle' | 'copied' | 'failed'>('idle');
  let copyResetTimer: ReturnType<typeof setTimeout> | undefined;

  // If the card unmounts while a reset timer is pending, the setTimeout
  // callback would still fire and try to mutate copyState on a destroyed
  // component. Clear it on teardown.
  onDestroy(() => {
    clearTimeout(copyResetTimer);
  });

  function toggle() {
    expanded = !expanded;
  }

  async function copyBody(event: MouseEvent | KeyboardEvent) {
    event.stopPropagation();
    if (!l1_overview) return;
    try {
      await navigator.clipboard.writeText(l1_overview);
      copyState = 'copied';
    } catch {
      copyState = 'failed';
    }
    clearTimeout(copyResetTimer);
    copyResetTimer = setTimeout(() => {
      copyState = 'idle';
    }, 1500);
  }

  function copyLabel(state: 'idle' | 'copied' | 'failed'): string {
    switch (state) {
      case 'copied':
        return 'Memory body copied to clipboard';
      case 'failed':
        return 'Copy to clipboard failed';
      default:
        return 'Copy memory body to clipboard';
    }
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
          {#if retracted}
            <span
              class="inline-block px-1.5 py-0.5 rounded text-[10px] font-mono uppercase tracking-wide opacity-70"
              style="border: 1px solid var(--text-secondary); color: var(--text-secondary);"
              title="This memory was retracted. Reason and original content are hidden by contract — use `continuity show <uri> --include-retracted` to inspect."
            >
              retracted
            </span>
          {/if}
        </div>
        {#if retracted}
          <p class="text-xs italic text-[var(--text-secondary)] opacity-70 leading-relaxed">
            (retracted — reason and original content hidden by contract)
          </p>
        {:else}
          <p class="text-sm leading-relaxed">{l0_abstract}</p>
        {/if}
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

    {#if expanded && l1_overview && !retracted}
      <div class="mt-3 pt-3 border-t border-[var(--border)] expand-enter">
        <div class="flex items-start justify-between gap-3">
          <!--
            Clicks inside the expanded body must not bubble to the card's
            toggle handler, or text selection / double-click gestures would
            collapse the card mid-drag. Keyboard users still toggle via the
            card's role="button" + Enter handler on the outer container.
          -->
          <p
            class="body-text text-sm text-[var(--text-secondary)] whitespace-pre-wrap leading-relaxed flex-1"
            onclick={(e) => e.stopPropagation()}
            onkeydown={(e) => e.stopPropagation()}
            onmousedown={(e) => e.stopPropagation()}
            role="presentation"
          >{l1_overview}</p>
          <button
            type="button"
            class="copy-btn shrink-0 text-xs font-mono px-2 py-1 rounded border border-[var(--border)] hover:border-[var(--border-hover)] text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
            onclick={copyBody}
            onkeydown={(e) => e.stopPropagation()}
            aria-label={copyLabel(copyState)}
          >
            {copyState === 'copied' ? 'copied' : copyState === 'failed' ? 'failed' : 'copy'}
          </button>
        </div>
        <!--
          Screen-reader-only announcement of copy state. aria-live="polite"
          speaks the transition (copied / failed) without interrupting; the
          visual button text carries the same info for sighted users.
        -->
        <span class="sr-only" role="status" aria-live="polite">
          {copyState === 'copied' ? 'Copied to clipboard' : copyState === 'failed' ? 'Copy failed' : ''}
        </span>
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

  /*
   * Expanded body text must be selectable so operators can copy it natively.
   * The parent card is cursor-pointer; reset to a text caret here so the
   * affordance is legible.
   */
  .body-text {
    user-select: text;
    -webkit-user-select: text;
    cursor: text;
  }

  .copy-btn {
    cursor: pointer;
    background: transparent;
    transition: border-color 150ms ease, color 150ms ease;
  }
</style>
