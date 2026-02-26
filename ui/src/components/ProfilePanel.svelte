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

  function formatProfile(text: string): string {
    // Simple markdown-ish rendering for section headers
    return text
      .replace(/^## (\d+)\. (.+)$/gm, '<h4 class="profile-section-header">$2</h4>')
      .replace(/^- (.+)$/gm, '<div class="profile-bullet">$1</div>')
      .replace(/\n\n/g, '<br/>');
  }

  load();
</script>

<div class="p-6 max-w-4xl mx-auto">
  <h2 class="text-lg font-semibold mb-5">Relational Profile</h2>

  {#if loading}
    <div class="flex items-center gap-3 text-[var(--text-secondary)] py-12 justify-center">
      <span class="loading-pulse" style="color: var(--color-accent)">&#10022;</span>
      <span class="text-sm">Loading profile...</span>
    </div>
  {:else if error}
    <p class="text-red-400 text-sm bg-red-400/10 px-4 py-3 rounded-lg">{error}</p>
  {:else}
    {#if relationalProfile}
      <div class="profile-card rounded-lg p-5 mb-6 fade-in">
        <div class="flex items-center gap-2 mb-4">
          <span class="text-xs" style="color: var(--color-accent)">&#9830;</span>
          <h3 class="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-widest">
            Working With You
          </h3>
        </div>
        <div class="profile-content text-sm leading-relaxed">
          {@html formatProfile(relationalProfile)}
        </div>
      </div>
    {:else}
      <div class="empty-profile rounded-lg p-8 mb-6 text-center fade-in">
        <div class="text-3xl mb-3 opacity-20" style="color: var(--color-accent)">&#9830;</div>
        <p class="text-[var(--text-secondary)] text-sm mb-1">No relational profile yet</p>
        <p class="text-[var(--text-secondary)] text-xs opacity-50">
          It builds over sessions as Continuity learns how you work.
        </p>
      </div>
    {/if}

    {#if nodes.length > 0}
      <div class="flex items-center gap-2 mb-3">
        <h3 class="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-widest">
          Profile &amp; Preferences
        </h3>
        <span class="text-[10px] font-mono text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded-full">
          {nodes.length}
        </span>
      </div>
      <div class="space-y-3">
        {#each nodes as node, i}
          <div class="fade-in" style="animation-delay: {i * 50}ms">
            <MemoryCard
              uri={node.uri}
              category={node.category}
              l0_abstract={node.l0_abstract}
              l1_overview={node.l1_overview}
              relevance={node.relevance}
            />
          </div>
        {/each}
      </div>
    {:else if relationalProfile}
      <p class="text-[var(--text-secondary)] text-sm opacity-60">No additional profile or preference nodes yet.</p>
    {/if}
  {/if}
</div>

<style>
  .profile-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    position: relative;
    overflow: hidden;
  }

  .profile-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: linear-gradient(90deg,
      var(--color-profile),
      var(--color-preferences),
      var(--color-entities),
      var(--color-events),
      var(--color-patterns),
      var(--color-cases)
    );
    opacity: 0.6;
  }

  .empty-profile {
    background: var(--bg-card);
    border: 1px dashed var(--border);
  }

  :global(.profile-section-header) {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--color-accent);
    margin-top: 1rem;
    margin-bottom: 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }

  :global(.profile-section-header:first-child) {
    margin-top: 0;
  }

  :global(.profile-bullet) {
    font-size: 0.875rem;
    color: var(--text-secondary);
    padding-left: 1rem;
    position: relative;
    margin-bottom: 0.25rem;
  }

  :global(.profile-bullet::before) {
    content: '\2022';
    position: absolute;
    left: 0;
    color: var(--color-accent);
    opacity: 0.5;
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
