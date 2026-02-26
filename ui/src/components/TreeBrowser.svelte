<script lang="ts">
  import { fetchTree } from '../lib/api';
  import type { TreeNode } from '../lib/types';
  import { categoryColors, type Category } from '../lib/types';
  import MemoryCard from './MemoryCard.svelte';

  let roots = $state<TreeNode[]>([]);
  let expandedDirs = $state<Record<string, TreeNode[]>>({});
  let loading = $state(true);
  let error = $state('');

  async function loadRoots() {
    loading = true;
    error = '';
    try {
      const data = await fetchTree();
      roots = data.nodes || [];
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  }

  async function toggleDir(uri: string) {
    if (expandedDirs[uri]) {
      const { [uri]: _, ...rest } = expandedDirs;
      expandedDirs = rest;
      return;
    }
    try {
      const data = await fetchTree(uri);
      expandedDirs = { ...expandedDirs, [uri]: data.nodes || [] };
    } catch (e) {
      error = String(e);
    }
  }

  function isExpanded(uri: string): boolean {
    return uri in expandedDirs;
  }

  function categoryColor(cat: string): string {
    return categoryColors[cat as Category] || 'var(--text-secondary)';
  }

  function label(uri: string): string {
    const parts = uri.replace('mem://', '').split('/');
    return parts[parts.length - 1] || uri;
  }

  function leafCount(): number {
    let count = 0;
    for (const r of roots) {
      count += r.children || 0;
    }
    return count;
  }

  loadRoots();
</script>

<div class="p-6 max-w-4xl mx-auto">
  <div class="flex items-center justify-between mb-5">
    <div class="flex items-center gap-3">
      <h2 class="text-lg font-semibold">Memory Tree</h2>
      {#if !loading && roots.length > 0}
        <span class="text-xs font-mono text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-2 py-0.5 rounded-full">
          mem://
        </span>
      {/if}
    </div>
  </div>

  {#if loading}
    <div class="flex items-center gap-3 text-[var(--text-secondary)] py-12 justify-center">
      <span class="loading-pulse" style="color: var(--color-accent)">&#10022;</span>
      <span class="text-sm">Loading memory tree...</span>
    </div>
  {:else if error}
    <p class="text-red-400 text-sm bg-red-400/10 px-4 py-3 rounded-lg">{error}</p>
  {:else if roots.length === 0}
    <div class="empty-state text-center py-16 px-8">
      <div class="text-4xl mb-4 opacity-30" style="color: var(--color-accent)">&#10022;</div>
      <p class="text-[var(--text-secondary)] text-sm mb-2">No memories yet</p>
      <p class="text-[var(--text-secondary)] text-xs opacity-60">
        Use Claude Code to start building memory. It grows with every session.
      </p>
    </div>
  {:else}
    <div class="tree-root space-y-0.5">
      {#each roots as root}
        <button
          class="tree-node flex items-center gap-2.5 w-full text-left px-3 py-2.5 rounded-lg hover:bg-[var(--bg-card)] transition-colors"
          onclick={() => toggleDir(root.uri)}
        >
          <span class="tree-chevron text-[var(--text-secondary)] text-xs" class:rotated={isExpanded(root.uri)}>
            &#9656;
          </span>
          <span class="font-medium text-sm">{root.uri}</span>
          {#if root.children}
            <span class="text-[10px] font-mono text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded-full">
              {root.children}
            </span>
          {/if}
        </button>

        {#if isExpanded(root.uri)}
          <div class="tree-branch ml-3 pl-4 border-l border-[var(--border)] space-y-0.5 expand-enter">
            {#each expandedDirs[root.uri] || [] as child}
              {#if child.node_type === 'dir'}
                <button
                  class="tree-node flex items-center gap-2.5 w-full text-left px-3 py-2 rounded-lg hover:bg-[var(--bg-card)] transition-colors"
                  onclick={() => toggleDir(child.uri)}
                >
                  <span class="tree-chevron text-xs" class:rotated={isExpanded(child.uri)}
                    style:color={categoryColor(child.category)}>
                    &#9656;
                  </span>
                  <span
                    class="text-sm font-medium"
                    style:color={categoryColor(child.category)}
                  >{label(child.uri)}</span>
                  {#if child.children}
                    <span class="text-[10px] font-mono text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded-full">
                      {child.children}
                    </span>
                  {/if}
                </button>

                {#if isExpanded(child.uri)}
                  <div class="tree-branch ml-3 pl-4 border-l space-y-2.5 py-1 expand-enter"
                    style:border-color="color-mix(in srgb, {categoryColor(child.category)} 30%, transparent)">
                    {#each expandedDirs[child.uri] || [] as leaf}
                      <MemoryCard
                        uri={leaf.uri}
                        category={leaf.category}
                        l0_abstract={leaf.l0_abstract || ''}
                        l1_overview={leaf.l1_overview}
                      />
                    {/each}
                  </div>
                {/if}
              {:else}
                <MemoryCard
                  uri={child.uri}
                  category={child.category}
                  l0_abstract={child.l0_abstract || ''}
                  l1_overview={child.l1_overview}
                />
              {/if}
            {/each}
          </div>
        {/if}
      {/each}
    </div>
  {/if}
</div>

<style>
  .tree-chevron {
    transition: transform 200ms ease;
    display: inline-block;
  }
  .tree-chevron.rotated {
    transform: rotate(90deg);
  }

  .loading-pulse {
    animation: pulse 1.5s ease-in-out infinite;
    font-size: 1.25rem;
  }

  @keyframes pulse {
    0%, 100% { opacity: 0.4; }
    50% { opacity: 1; }
  }
</style>
