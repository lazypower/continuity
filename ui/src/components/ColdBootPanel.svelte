<script lang="ts">
  import { fetchColdBootContext, fetchPinned } from '../lib/api';
  import type { PinnedMemory } from '../lib/types';
  import MemoryCard from './MemoryCard.svelte';

  let context = $state('');
  let pins = $state<PinnedMemory[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function load() {
    loading = true;
    error = '';
    try {
      const [ctx, pinned] = await Promise.all([fetchColdBootContext(), fetchPinned()]);
      context = ctx.context || '';
      pins = pinned.pins || [];
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  }

  // A pin toggled from this view (unpin) changes both the pin list and the
  // verbatim context, so reload the whole picture.
  function onPinChange() {
    load();
  }

  function approxTokens(s: string): number {
    // Rough heuristic — ~4 chars/token. Enough to gauge tray weight.
    return Math.round(s.length / 4);
  }

  load();
</script>

<div class="p-6 max-w-4xl mx-auto">
  <div class="mb-5">
    <h2 class="text-lg font-semibold">Cold Boot</h2>
    <p class="text-sm text-[var(--text-secondary)] mt-1">
      Exactly what an agent wakes up with at <span class="font-mono">SessionStart</span> — the surgeon's
      tray, verbatim. Pins are the declared contract: what you've chosen to put on the tray before
      knowing the operation.
    </p>
  </div>

  {#if loading}
    <div class="flex items-center gap-3 text-[var(--text-secondary)] py-12 justify-center">
      <span class="loading-pulse" style="color: var(--color-accent)">&#10022;</span>
      <span class="text-sm">Loading cold-boot window...</span>
    </div>
  {:else if error}
    <p class="text-red-400 text-sm bg-red-400/10 px-4 py-3 rounded-lg">{error}</p>
  {:else}
    <!-- Pinned memories — the declared contract, with drill-down + unpin. -->
    <section class="mb-8">
      <div class="flex items-center gap-2 mb-3">
        <h3 class="text-sm font-semibold uppercase tracking-wide text-[var(--text-secondary)]">
          Pinned
        </h3>
        <span class="text-[10px] font-mono text-[var(--text-secondary)] bg-[var(--bg-secondary)] px-1.5 py-0.5 rounded-full">
          {pins.length}
        </span>
      </div>
      {#if pins.length === 0}
        <p class="text-sm text-[var(--text-secondary)] opacity-70">
          No pins. Browse the <span class="font-mono">Tree</span> and click the 📍 on any memory to pin it
          to the cold-boot window.
        </p>
      {:else}
        <div class="space-y-2.5">
          {#each pins as pin}
            <MemoryCard
              uri={pin.uri}
              category={pin.category}
              l0_abstract={pin.l0_abstract}
              l1_overview={pin.l1_overview}
              relevance={pin.relevance}
              showPin={true}
              pinned={true}
              onpinchange={onPinChange}
            />
          {/each}
        </div>
      {/if}
    </section>

    <!-- The verbatim injection — the honesty instrument. -->
    <section>
      <div class="flex items-center justify-between mb-3">
        <h3 class="text-sm font-semibold uppercase tracking-wide text-[var(--text-secondary)]">
          Injected verbatim
        </h3>
        <span class="text-[10px] font-mono text-[var(--text-secondary)]">
          {context.length} chars · ~{approxTokens(context)} tokens
        </span>
      </div>
      <pre class="context-block text-xs font-mono whitespace-pre-wrap leading-relaxed">{context}</pre>
    </section>
  {/if}
</div>

<style>
  .loading-pulse {
    animation: pulse 1.5s ease-in-out infinite;
    font-size: 1.25rem;
  }

  @keyframes pulse {
    0%, 100% { opacity: 0.4; }
    50% { opacity: 1; }
  }

  .context-block {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 0.5rem;
    padding: 1rem;
    color: var(--text-secondary);
    max-height: 60vh;
    overflow: auto;
  }
</style>
