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
  <h2 class="text-lg font-semibold mb-5">Search Memories</h2>

  <div class="search-bar flex gap-2 mb-5">
    <div class="flex-1 relative">
      <input
        type="text"
        bind:value={query}
        onkeydown={handleKeydown}
        placeholder="What are you looking for?"
        class="w-full px-4 py-2.5 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm focus:outline-none focus:border-[var(--color-accent)] focus:ring-1 focus:ring-[var(--color-accent)] placeholder:text-[var(--text-secondary)] placeholder:opacity-50"
      />
    </div>
    <select
      bind:value={mode}
      class="px-3 py-2.5 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm focus:outline-none focus:border-[var(--color-accent)]"
    >
      <option value="find">Find</option>
      <option value="search">Search</option>
    </select>
    <select
      bind:value={category}
      class="px-3 py-2.5 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-[var(--text-primary)] text-sm focus:outline-none focus:border-[var(--color-accent)]"
    >
      {#each categories as cat}
        <option value={cat}>{cat || 'All categories'}</option>
      {/each}
    </select>
    <button
      onclick={doSearch}
      disabled={loading || !query.trim()}
      class="search-btn px-5 py-2.5 rounded-lg text-sm font-medium disabled:opacity-30 transition-all"
    >
      {#if loading}
        <span class="loading-dots">...</span>
      {:else}
        Search
      {/if}
    </button>
  </div>

  {#if error}
    <p class="text-red-400 text-sm bg-red-400/10 px-4 py-3 rounded-lg mb-4">{error}</p>
  {/if}

  {#if searched && !loading}
    <div class="flex items-center gap-2 mb-4">
      <span class="text-sm text-[var(--text-secondary)]">
        {resultCount} result{resultCount !== 1 ? 's' : ''}
      </span>
      <span class="text-xs font-mono text-[var(--text-secondary)] opacity-40">
        "{query}" via {mode}
      </span>
    </div>
  {/if}

  {#if searched && !loading && results.length === 0}
    <div class="text-center py-12">
      <p class="text-[var(--text-secondary)] text-sm">No memories match that query.</p>
    </div>
  {/if}

  <div class="space-y-3">
    {#each results as result, i}
      <div class="fade-in" style="animation-delay: {i * 50}ms">
        <MemoryCard
          uri={result.uri}
          category={result.category}
          l0_abstract={result.l0_abstract}
          l1_overview={result.l1_overview}
          relevance={result.relevance}
          score={result.score}
        />
      </div>
    {/each}
  </div>
</div>

<style>
  .search-btn {
    background: var(--color-accent);
    color: #0a0f1a;
  }
  .search-btn:hover:not(:disabled) {
    background: var(--color-accent-dim);
    box-shadow: 0 0 16px rgba(212, 168, 67, 0.2);
  }

  .loading-dots {
    animation: blink 1s ease-in-out infinite;
  }
  @keyframes blink {
    0%, 100% { opacity: 0.3; }
    50% { opacity: 1; }
  }
</style>
