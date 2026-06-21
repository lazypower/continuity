<script lang="ts">
  import { fetchMetrics } from '../lib/api';
  import type { MetricsResponse, MetricNode } from '../lib/types';
  import { categoryColors, bandColor, type Category } from '../lib/types';
  import DecayHistogram from './DecayHistogram.svelte';
  import EffectivenessSparkline from './EffectivenessSparkline.svelte';

  let data = $state<MetricsResponse | null>(null);
  let loading = $state(true);
  let error = $state('');

  async function load() {
    loading = true;
    error = '';
    try {
      data = await fetchMetrics();
    } catch (e) {
      error = String(e);
      data = null;
    } finally {
      loading = false;
    }
  }
  load();

  function catColor(cat: string): string {
    return categoryColors[cat as Category] || 'var(--text-secondary)';
  }
  function slug(u: string): string {
    const p = u.replace('mem://', '').split('/');
    return p[p.length - 1] || u;
  }
  function pct(n: number): number {
    return Math.round(n * 100);
  }
  function ageLabel(days: number): string {
    if (days < 1) return 'today';
    if (days === 1) return '1d';
    if (days < 30) return `${days}d`;
    if (days < 365) return `${Math.round(days / 30)}mo`;
    return `${(days / 365).toFixed(1)}y`;
  }

  // Current-state verdict (no trend history yet — that arrives with metrics_daily).
  const verdict = $derived.by(() => {
    if (!data) return '';
    const s = data.summary;
    if (s.active_total === 0) return 'No memories yet.';
    const staleFrac = s.stale / s.active_total;
    const freshFrac = s.fresh / s.active_total;
    if (staleFrac > 0.4) return 'Drifting stale — a lot of memory is fading out of reach.';
    if (freshFrac > 0.6) return 'Healthy — most memory is fresh and reachable.';
    return 'Stable — a healthy core with some fading at the edges.';
  });

  function nn<T>(xs: T[] | null): T[] {
    return xs ?? [];
  }
</script>

<div class="p-6 max-w-5xl mx-auto">
  <div class="flex items-baseline justify-between mb-1">
    <h2 class="text-lg font-semibold">Memory Health</h2>
    {#if data}
      <span class="text-xs font-mono text-[var(--text-secondary)] opacity-50">
        {data.summary.active_total} active · {data.summary.retracted_total} retracted
      </span>
    {/if}
  </div>

  {#if error}
    <p class="text-red-400 text-sm bg-red-400/10 px-4 py-3 rounded-lg mt-4">{error}</p>
  {:else if loading}
    <div class="text-center py-16">
      <span class="loading-pulse text-[var(--color-accent)] text-2xl">♥</span>
      <p class="text-[var(--text-secondary)] text-sm mt-2">Reading your memory…</p>
    </div>
  {:else if data}
    <p class="text-sm text-[var(--text-secondary)] mb-5">{verdict}</p>

    <!-- Stat tiles -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
      <div class="tile">
        <span class="tile-label">Active</span>
        <span class="tile-num">{data.summary.active_total}</span>
        <span class="tile-sub">{data.summary.never_retrieved} never retrieved</span>
      </div>

      <div class="tile">
        <span class="tile-label">Freshness</span>
        <div class="seg mt-1.5">
          {#each [['fresh', data.summary.fresh, 'var(--color-preferences)'], ['fading', data.summary.fading, 'var(--color-events)'], ['stale', data.summary.stale, 'var(--color-cases)']] as [, n, c]}
            {#if (n as number) > 0}
              <div class="seg-part" style="flex: {n}; background: {c}" title="{n}"></div>
            {/if}
          {/each}
        </div>
        <div class="flex gap-2.5 mt-1.5 text-[10px] font-mono text-[var(--text-secondary)]">
          <span style="color: var(--color-preferences)">{data.summary.fresh} fresh</span>
          <span style="color: var(--color-events)">{data.summary.fading} fading</span>
          <span style="color: var(--color-cases)">{data.summary.stale} stale</span>
        </div>
      </div>

      <div class="tile">
        <span class="tile-label">Retraction rate</span>
        <span class="tile-num">{pct(data.summary.retraction_rate)}<span class="text-base opacity-50">%</span></span>
        <span class="tile-sub">{data.summary.recent_retractions} in last 30d</span>
      </div>

      <div class="tile">
        <span class="tile-label">Categories</span>
        <span class="tile-num">{data.categories.length}</span>
        <span class="tile-sub">
          {#if data.categories.length}top: {data.categories[0].category} ({pct(data.categories[0].share)}%){/if}
        </span>
      </div>
    </div>

    <!-- Hero: decay distribution -->
    <div class="card mb-5">
      <div class="flex items-baseline justify-between mb-2">
        <h3 class="card-title">Decay distribution</h3>
        <span class="text-[10px] font-mono text-[var(--text-secondary)] opacity-50">
          where {data.summary.active_total} active memories sit on the relevance spectrum
        </span>
      </div>
      <DecayHistogram bins={data.histogram} />
    </div>

    <!-- Category breakdown -->
    <div class="card mb-5">
      <h3 class="card-title mb-3">By category</h3>
      <div class="space-y-2">
        {#each data.categories as c}
          <div class="flex items-center gap-3">
            <span class="w-24 text-xs font-mono shrink-0" style="color: {catColor(c.category)}">{c.category}</span>
            <div class="flex-1 h-2 rounded-full bg-[var(--border)] overflow-hidden">
              <div class="h-full rounded-full" style="width: {pct(c.share)}%; background: {catColor(c.category)}; opacity: 0.75"></div>
            </div>
            <span class="w-8 text-right text-xs font-mono text-[var(--text-secondary)]">{c.count}</span>
          </div>
        {/each}
      </div>
    </div>

    <!-- Needs attention -->
    {#snippet attn(title: string, hint: string, items: MetricNode[], metric: (n: MetricNode) => string)}
      <div class="card">
        <h3 class="card-title">{title}</h3>
        <p class="text-[10px] text-[var(--text-secondary)] opacity-60 mb-2.5">{hint}</p>
        {#if items.length === 0}
          <p class="text-xs text-[var(--text-secondary)] opacity-50 italic py-2">nothing here — clean</p>
        {:else}
          <div class="space-y-1.5">
            {#each items as n}
              <div class="attn-row">
                <span class="dot" style="background: {catColor(n.category)}"></span>
                <span class="attn-slug" title={n.uri}>{slug(n.uri)}</span>
                <span class="attn-l0">{n.l0_abstract}</span>
                <span class="attn-metric">{metric(n)}</span>
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/snippet}

    <h3 class="text-sm font-semibold text-[var(--text-secondary)] uppercase tracking-wide mb-3 mt-7">Needs attention</h3>
    <div class="grid md:grid-cols-2 gap-3 mb-5">
      {@render attn(
        'Load-bearing & decaying',
        'retrieved often but no longer fresh — refresh these first',
        nn(data.needs_attention.stale_high_retrieval),
        (n) => `${n.access_count}× · ${pct(n.relevance)}%`,
      )}
      {@render attn(
        'Never retrieved & old',
        'captured but never pulled into a session — noise or buried value',
        nn(data.needs_attention.never_retrieved_old),
        (n) => ageLabel(n.age_days),
      )}
      {@render attn(
        'Near the decay cliff',
        'approaching stale — act before they fade out of reach',
        nn(data.needs_attention.near_decay_cliff),
        (n) => `${pct(n.relevance)}%`,
      )}
      {@render attn(
        'Orphaned tombstones',
        'retracted without a successor — untidy supersession',
        nn(data.needs_attention.orphaned_tombstones),
        (n) => ageLabel(n.age_days),
      )}
    </div>

    <!-- Critical -->
    <div class="card mb-5">
      <h3 class="card-title mb-1">Critical memories</h3>
      <p class="text-[10px] text-[var(--text-secondary)] opacity-60 mb-3">most retrieved — your load-bearing core</p>
      {#if nn(data.critical).length === 0}
        <p class="text-xs text-[var(--text-secondary)] opacity-50 italic">nothing retrieved yet</p>
      {:else}
        <div class="space-y-1.5">
          {#each nn(data.critical) as n, i}
            <div class="crit-row">
              <span class="crit-rank">{i + 1}</span>
              <span class="dot" style="background: {catColor(n.category)}"></span>
              <span class="attn-slug" title={n.uri}>{slug(n.uri)}</span>
              <span class="attn-l0">{n.l0_abstract}</span>
              <div class="crit-relbar"><div style="width: {pct(n.relevance)}%; background: {bandColor(n.relevance)}"></div></div>
              <span class="crit-count">{n.access_count}×</span>
            </div>
          {/each}
        </div>
      {/if}
    </div>

    <!-- Footer: memory effectiveness over time -->
    <div class="card">
      <h3 class="card-title">Memory effectiveness</h3>
      <p class="text-[10px] text-[var(--text-secondary)] opacity-60 mb-2">
        captures vs retrievals per day — capture spikes above retrievals read as repeating yourself; a sustained retrieval dip reads as endemic drift, a one-off as gremlins
      </p>
      {#if nn(data.daily).length}
        <EffectivenessSparkline daily={nn(data.daily)} />
      {:else}
        <p class="text-xs text-[var(--text-secondary)] opacity-50 italic py-2">no history yet</p>
      {/if}
    </div>
  {/if}
</div>

<style>
  .tile {
    display: flex;
    flex-direction: column;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 14px;
  }
  .tile-label {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-secondary);
    opacity: 0.7;
  }
  .tile-num {
    font-size: 28px;
    font-weight: 600;
    line-height: 1.1;
    margin-top: 2px;
    color: var(--text-primary);
  }
  .tile-sub {
    font-size: 11px;
    font-family: ui-monospace, monospace;
    color: var(--text-secondary);
    opacity: 0.6;
    margin-top: 2px;
  }
  .seg {
    display: flex;
    height: 8px;
    border-radius: 4px;
    overflow: hidden;
    gap: 2px;
    background: var(--border);
  }
  .seg-part {
    height: 100%;
  }
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px 18px;
  }
  .card-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--text-primary);
  }
  .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .attn-row,
  .crit-row {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 6px;
    border-radius: 6px;
    font-size: 12px;
  }
  .attn-row:hover,
  .crit-row:hover {
    background: var(--bg-card-hover);
  }
  .attn-slug {
    font-family: ui-monospace, monospace;
    font-size: 11px;
    color: var(--text-primary);
    flex-shrink: 0;
    max-width: 30%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .attn-l0 {
    flex: 1;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .attn-metric,
  .crit-count {
    font-family: ui-monospace, monospace;
    font-size: 11px;
    color: var(--text-secondary);
    flex-shrink: 0;
    text-align: right;
  }
  .crit-rank {
    font-family: ui-monospace, monospace;
    font-size: 10px;
    color: var(--text-secondary);
    opacity: 0.5;
    width: 14px;
    flex-shrink: 0;
  }
  .crit-relbar {
    width: 56px;
    height: 3px;
    border-radius: 2px;
    background: var(--border);
    overflow: hidden;
    flex-shrink: 0;
  }
  .crit-relbar > div {
    height: 100%;
    border-radius: 2px;
    opacity: 0.8;
  }
</style>
