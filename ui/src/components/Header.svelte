<script lang="ts">
  import { activeTab, type Tab } from '../lib/stores';
  import ThemeToggle from './ThemeToggle.svelte';

  const tabs: { id: Tab; label: string; icon: string }[] = [
    { id: 'tree', label: 'Tree', icon: '&#9672;' },
    { id: 'search', label: 'Search', icon: '&#9906;' },
    { id: 'profile', label: 'Profile', icon: '&#9830;' },
  ];

  function setTab(tab: Tab) {
    activeTab.set(tab);
  }
</script>

<header class="border-b border-[var(--border)] px-6 py-3 flex items-center justify-between bg-[var(--bg-secondary)]">
  <div class="flex items-center gap-5">
    <div class="flex items-center gap-2.5">
      <!-- Sparkle icon echoing the hero image's star -->
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M12 2L13.5 8.5L20 7L14.5 11L18 17L12 13.5L6 17L9.5 11L4 7L10.5 8.5L12 2Z"
          fill="var(--color-accent)" opacity="0.9"/>
        <path d="M12 2L13.5 8.5L20 7L14.5 11L18 17L12 13.5L6 17L9.5 11L4 7L10.5 8.5L12 2Z"
          fill="none" stroke="var(--color-accent)" stroke-width="0.5" opacity="0.5"/>
      </svg>
      <h1 class="text-lg font-semibold tracking-tight" style="color: var(--color-accent)">
        Continuity
      </h1>
    </div>
    <nav class="flex gap-0.5">
      {#each tabs as tab}
        <button
          onclick={() => setTab(tab.id)}
          class="px-3.5 py-1.5 rounded-md text-sm font-medium flex items-center gap-1.5 {$activeTab === tab.id
            ? 'active-tab'
            : 'text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-card)]'}"
        >
          <span class="text-xs" style={$activeTab === tab.id ? 'color: var(--color-accent)' : ''}>
            {@html tab.icon}
          </span>
          {tab.label}
        </button>
      {/each}
    </nav>
  </div>
  <ThemeToggle />
</header>

<style>
  .active-tab {
    background: var(--bg-card);
    color: var(--text-primary);
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.2), inset 0 1px 0 rgba(212, 168, 67, 0.1);
  }
</style>
