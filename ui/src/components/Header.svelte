<script lang="ts">
  import { activeTab, type Tab } from '../lib/stores';
  import ThemeToggle from './ThemeToggle.svelte';

  const tabs: { id: Tab; label: string }[] = [
    { id: 'tree', label: 'Tree' },
    { id: 'search', label: 'Search' },
    { id: 'profile', label: 'Profile' },
  ];

  function setTab(tab: Tab) {
    activeTab.set(tab);
  }
</script>

<header class="border-b border-[var(--border)] px-6 py-3 flex items-center justify-between bg-[var(--bg-secondary)]">
  <div class="flex items-center gap-4">
    <h1 class="text-lg font-semibold tracking-tight">Continuity</h1>
    <nav class="flex gap-1">
      {#each tabs as tab}
        <button
          onclick={() => setTab(tab.id)}
          class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors {$activeTab === tab.id
            ? 'bg-[var(--bg-primary)] text-[var(--text-primary)] shadow-sm'
            : 'text-[var(--text-secondary)] hover:text-[var(--text-primary)]'}"
        >
          {tab.label}
        </button>
      {/each}
    </nav>
  </div>
  <ThemeToggle />
</header>
