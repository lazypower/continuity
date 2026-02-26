<script lang="ts">
  import { fetchSearch } from '../lib/api';
  import type { SearchResult } from '../lib/types';
  import MemoryCard from './MemoryCard.svelte';

  let query = $state('');
  let mode = $state<'find' | 'search'>('find');
  let category = $state('');
  let limit = $state(10);
  let results = $state<SearchResult[]>([]);
  let resultCount = $state(0);
  let loading = $state(false);
  let error = $state('');
  let searched = $state(false);

  async function doSearch() {
    if (!query.trim()) return;
    loading = true;
    error = '';
    searched = true;
    try {
      const data = await fetchSearch(query, mode, limit, category || undefined);
      results = data.results || [];
      resultCount = data.count;
    } catch (e) {
      error = String(e);
      results = [];
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') doSearch();
  }

  const categories = ['', 'profile', 'preferences', 'entities', 'events', 'patterns', 'cases'];
</script>

<div class="p-6 max-w-4xl mx-auto">
  <h2 class="text-lg font-semibold mb-4">Search Memories</h2>

  <div class="flex gap-2 mb-4">
    <input
      type="text"
      bind:value={query}
      onkeydown={handleKeydown}
      placeholder="Search your memories..."
      class="flex-1 px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
    />
    <select
      bind:value={mode}
      class="px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm"
    >
      <option value="find">Find</option>
      <option value="search">Search</option>
    </select>
    <select
      bind:value={category}
      class="px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm"
    >
      {#each categories as cat}
        <option value={cat}>{cat || 'All categories'}</option>
      {/each}
    </select>
    <button
      onclick={doSearch}
      disabled={loading || !query.trim()}
      class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
    >
      {loading ? '...' : 'Go'}
    </button>
  </div>

  {#if error}
    <p class="text-red-500 text-sm mb-4">{error}</p>
  {/if}

  {#if searched && !loading}
    <p class="text-sm text-[var(--text-secondary)] mb-4">
      {resultCount} result{resultCount !== 1 ? 's' : ''} for "{query}" ({mode})
    </p>
  {/if}

  <div class="space-y-3">
    {#each results as result}
      <MemoryCard
        uri={result.uri}
        category={result.category}
        l0_abstract={result.l0_abstract}
        l1_overview={result.l1_overview}
        relevance={result.relevance}
        score={result.score}
      />
    {/each}
  </div>
</div>
