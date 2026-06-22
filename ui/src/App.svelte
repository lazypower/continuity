<script lang="ts">
  import { activeTab } from './lib/stores';
  import Header from './components/Header.svelte';
  import HealthPanel from './components/HealthPanel.svelte';
  import TreeBrowser from './components/TreeBrowser.svelte';
  import SearchPanel from './components/SearchPanel.svelte';
  import ProfilePanel from './components/ProfilePanel.svelte';
  import ColdBootPanel from './components/ColdBootPanel.svelte';
</script>

<div class="min-h-screen">
  <Header />
  <main>
    {#if $activeTab === 'health'}
      <HealthPanel />
    {:else if $activeTab === 'tree'}
      <TreeBrowser />
    {:else if $activeTab === 'search'}
      <SearchPanel />
    {:else if $activeTab === 'profile'}
      <ProfilePanel />
    {:else if $activeTab === 'coldboot'}
      <ColdBootPanel />
    {/if}
  </main>
</div>
