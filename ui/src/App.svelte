<script lang="ts">
  import { activeTab } from './lib/stores';
  import Header from './components/Header.svelte';
  import TreeBrowser from './components/TreeBrowser.svelte';
  import SearchPanel from './components/SearchPanel.svelte';
  import ProfilePanel from './components/ProfilePanel.svelte';
</script>

<div class="min-h-screen">
  <Header />
  <main>
    {#if $activeTab === 'tree'}
      <TreeBrowser />
    {:else if $activeTab === 'search'}
      <SearchPanel />
    {:else if $activeTab === 'profile'}
      <ProfilePanel />
    {/if}
  </main>
</div>
