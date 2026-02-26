<script lang="ts">
  import { fetchProfile } from '../lib/api';
  import type { ProfileNode } from '../lib/types';
  import MemoryCard from './MemoryCard.svelte';

  let relationalProfile = $state('');
  let nodes = $state<ProfileNode[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function load() {
    loading = true;
    error = '';
    try {
      const data = await fetchProfile();
      relationalProfile = data.relational_profile || '';
      nodes = data.nodes || [];
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  }

  load();
</script>

<div class="p-6 max-w-4xl mx-auto">
  <h2 class="text-lg font-semibold mb-4">Relational Profile</h2>

  {#if loading}
    <p class="text-[var(--text-secondary)]">Loading...</p>
  {:else if error}
    <p class="text-red-500 text-sm">{error}</p>
  {:else}
    {#if relationalProfile}
      <div class="border border-[var(--border)] rounded-lg p-5 bg-[var(--bg-card)] mb-6">
        <h3 class="text-sm font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-3">Working With You</h3>
        <div class="text-sm leading-relaxed whitespace-pre-wrap">{relationalProfile}</div>
      </div>
    {:else}
      <div class="border border-[var(--border)] border-dashed rounded-lg p-5 bg-[var(--bg-card)] mb-6">
        <p class="text-[var(--text-secondary)] text-sm">No relational profile yet. It builds over sessions.</p>
      </div>
    {/if}

    {#if nodes.length > 0}
      <h3 class="text-sm font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-3">Profile &amp; Preferences</h3>
      <div class="space-y-3">
        {#each nodes as node}
          <MemoryCard
            uri={node.uri}
            category={node.category}
            l0_abstract={node.l0_abstract}
            relevance={node.relevance}
          />
        {/each}
      </div>
    {:else}
      <p class="text-[var(--text-secondary)] text-sm">No profile or preference nodes yet.</p>
    {/if}
  {/if}
</div>
