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

  loadRoots();
</script>

<div class="p-6 max-w-4xl mx-auto">
  <h2 class="text-lg font-semibold mb-4">Memory Tree</h2>

  {#if loading}
    <p class="text-[var(--text-secondary)]">Loading...</p>
  {:else if error}
    <p class="text-red-500 text-sm">{error}</p>
  {:else if roots.length === 0}
    <p class="text-[var(--text-secondary)]">No memories yet. Use Claude Code to build memory.</p>
  {:else}
    <div class="space-y-1">
      {#each roots as root}
        <button
          class="flex items-center gap-2 w-full text-left px-3 py-2 rounded-md hover:bg-[var(--bg-secondary)] transition-colors"
          onclick={() => toggleDir(root.uri)}
        >
          <span class="text-[var(--text-secondary)]">{isExpanded(root.uri) ? '▾' : '▸'}</span>
          <span class="font-medium">{root.uri}</span>
          {#if root.children}
            <span class="text-xs text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded">
              {root.children}
            </span>
          {/if}
        </button>

        {#if isExpanded(root.uri)}
          <div class="ml-6 space-y-1">
            {#each expandedDirs[root.uri] || [] as child}
              {#if child.node_type === 'dir'}
                <button
                  class="flex items-center gap-2 w-full text-left px-3 py-2 rounded-md hover:bg-[var(--bg-secondary)] transition-colors"
                  onclick={() => toggleDir(child.uri)}
                >
                  <span class="text-[var(--text-secondary)]">{isExpanded(child.uri) ? '▾' : '▸'}</span>
                  <span
                    class="text-sm font-medium"
                    style:color={categoryColor(child.category)}
                  >{label(child.uri)}</span>
                  {#if child.children}
                    <span class="text-xs text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded">
                      {child.children}
                    </span>
                  {/if}
                </button>

                {#if isExpanded(child.uri)}
                  <div class="ml-6 space-y-2 py-1">
                    {#each expandedDirs[child.uri] || [] as leaf}
                      <MemoryCard
                        uri={leaf.uri}
                        category={leaf.category}
                        l0_abstract={leaf.l0_abstract || ''}
                      />
                    {/each}
                  </div>
                {/if}
              {:else}
                <MemoryCard
                  uri={child.uri}
                  category={child.category}
                  l0_abstract={child.l0_abstract || ''}
                />
              {/if}
            {/each}
          </div>
        {/if}
      {/each}
    </div>
  {/if}
</div>
