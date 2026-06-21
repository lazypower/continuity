<script lang="ts">
  import { scaleLinear } from 'd3-scale';
  import { area, line, curveCatmullRom } from 'd3-shape';
  import { max } from 'd3-array';
  import type { HistBin } from '../lib/types';
  import { FRESH_THRESHOLD, STALE_THRESHOLD } from '../lib/types';

  interface Props {
    bins: HistBin[];
  }
  let { bins }: Props = $props();

  // Fixed drawing space; the SVG scales to its container via viewBox.
  const W = 760;
  const H = 240;
  const M = { top: 16, right: 16, bottom: 30, left: 32 };
  const iw = W - M.left - M.right;
  const ih = H - M.top - M.bottom;

  const lo = $derived(bins.length ? bins[0].lo : 0.1);
  const hi = $derived(bins.length ? bins[bins.length - 1].hi : 1.0);
  const maxCount = $derived(Math.max(1, max(bins, (b) => b.count) ?? 1));

  const x = $derived(scaleLinear().domain([lo, hi]).range([M.left, M.left + iw]));
  const y = $derived(scaleLinear().domain([0, maxCount]).range([M.top + ih, M.top]));

  // Smooth ridge over bin centers, anchored to the baseline at both edges so the
  // area reads as a single shape rather than disconnected bars.
  type Pt = { v: number; c: number };
  const pts = $derived<Pt[]>([
    { v: lo, c: 0 },
    ...bins.map((b) => ({ v: (b.lo + b.hi) / 2, c: b.count })),
    { v: hi, c: 0 },
  ]);

  const areaPath = $derived(
    area<Pt>()
      .x((p) => x(p.v))
      .y0(M.top + ih)
      .y1((p) => y(p.c))
      .curve(curveCatmullRom.alpha(0.5))(pts) ?? '',
  );
  const linePath = $derived(
    line<Pt>()
      .x((p) => x(p.v))
      .y((p) => y(p.c))
      .curve(curveCatmullRom.alpha(0.5))(pts) ?? '',
  );

  // Gradient stop offsets (%) for the band thresholds, positioned by relevance.
  const span = $derived(hi - lo || 1);
  const staleOff = $derived(((STALE_THRESHOLD - lo) / span) * 100);
  const freshOff = $derived(((FRESH_THRESHOLD - lo) / span) * 100);

  // Hover: snap to nearest bin.
  let hover = $state<{ bin: HistBin; cx: number } | null>(null);
  function onMove(e: PointerEvent) {
    const svg = e.currentTarget as SVGSVGElement;
    const r = svg.getBoundingClientRect();
    const px = ((e.clientX - r.left) / r.width) * W; // back to viewBox space
    const rel = x.invert(px);
    let best: HistBin | null = null;
    let bestD = Infinity;
    for (const b of bins) {
      const c = (b.lo + b.hi) / 2;
      const d = Math.abs(c - rel);
      if (d < bestD) {
        bestD = d;
        best = b;
      }
    }
    if (best) hover = { bin: best, cx: x((best.lo + best.hi) / 2) };
  }
  function onLeave() {
    hover = null;
  }

  const ticks = [0.1, 0.4, 0.7, 1.0];
</script>

<div class="relative">
  <svg
    viewBox="0 0 {W} {H}"
    class="w-full"
    style="height: auto"
    role="img"
    aria-label="Relevance distribution of active memories"
    onpointermove={onMove}
    onpointerleave={onLeave}
  >
    <defs>
      <linearGradient id="decay-grad" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%" stop-color="var(--color-cases)" />
        <stop offset="{staleOff}%" stop-color="var(--color-cases)" />
        <stop offset="{staleOff}%" stop-color="var(--color-events)" />
        <stop offset="{freshOff}%" stop-color="var(--color-events)" />
        <stop offset="{freshOff}%" stop-color="var(--color-preferences)" />
        <stop offset="100%" stop-color="var(--color-preferences)" />
      </linearGradient>
    </defs>

    <!-- Faint band regions behind the ridge -->
    <rect x={x(lo)} y={M.top} width={x(STALE_THRESHOLD) - x(lo)} height={ih}
      fill="var(--color-cases)" opacity="0.05" />
    <rect x={x(STALE_THRESHOLD)} y={M.top} width={x(FRESH_THRESHOLD) - x(STALE_THRESHOLD)} height={ih}
      fill="var(--color-events)" opacity="0.05" />
    <rect x={x(FRESH_THRESHOLD)} y={M.top} width={x(hi) - x(FRESH_THRESHOLD)} height={ih}
      fill="var(--color-preferences)" opacity="0.05" />

    <!-- baseline -->
    <line x1={M.left} y1={M.top + ih} x2={M.left + iw} y2={M.top + ih}
      stroke="var(--border)" stroke-width="1" />

    <!-- the ridge -->
    <path d={areaPath} fill="url(#decay-grad)" opacity="0.32" />
    <path d={linePath} fill="none" stroke="url(#decay-grad)" stroke-width="2" />

    <!-- threshold guides -->
    {#each [STALE_THRESHOLD, FRESH_THRESHOLD] as t}
      <line x1={x(t)} y1={M.top} x2={x(t)} y2={M.top + ih}
        stroke="var(--border-hover)" stroke-width="1" stroke-dasharray="3 3" opacity="0.6" />
    {/each}

    <!-- hover marker -->
    {#if hover}
      <line x1={hover.cx} y1={M.top} x2={hover.cx} y2={M.top + ih}
        stroke="var(--color-accent)" stroke-width="1" opacity="0.7" />
      <circle cx={hover.cx} cy={y(hover.bin.count)} r="3.5"
        fill="var(--color-accent)" />
    {/if}

    <!-- x ticks -->
    {#each ticks as t}
      <text x={x(t)} y={M.top + ih + 18} text-anchor="middle"
        class="axis-label">{t.toFixed(1)}</text>
    {/each}
    <!-- y max label -->
    <text x={M.left - 6} y={M.top + 8} text-anchor="end" class="axis-label">{maxCount}</text>

    <!-- band captions -->
    <text x={(x(lo) + x(STALE_THRESHOLD)) / 2} y={M.top + 12} text-anchor="middle"
      class="band-label" fill="var(--color-cases)">stale</text>
    <text x={(x(STALE_THRESHOLD) + x(FRESH_THRESHOLD)) / 2} y={M.top + 12} text-anchor="middle"
      class="band-label" fill="var(--color-events)">fading</text>
    <text x={(x(FRESH_THRESHOLD) + x(hi)) / 2} y={M.top + 12} text-anchor="middle"
      class="band-label" fill="var(--color-preferences)">fresh</text>
  </svg>

  {#if hover}
    <div class="tooltip" style="left: {(hover.cx / W) * 100}%">
      <span class="tip-count">{hover.bin.count}</span>
      <span class="tip-range">relevance {hover.bin.lo.toFixed(2)}–{hover.bin.hi.toFixed(2)}</span>
    </div>
  {/if}
</div>

<style>
  .axis-label {
    font-size: 10px;
    font-family: ui-monospace, monospace;
    fill: var(--text-secondary);
    opacity: 0.55;
  }
  .band-label {
    font-size: 9px;
    font-family: ui-monospace, monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    opacity: 0.7;
  }
  .tooltip {
    position: absolute;
    top: -4px;
    transform: translateX(-50%);
    pointer-events: none;
    background: var(--bg-card-hover);
    border: 1px solid var(--border-hover);
    border-radius: 6px;
    padding: 4px 8px;
    display: flex;
    flex-direction: column;
    align-items: center;
    white-space: nowrap;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
  }
  .tip-count {
    font-size: 13px;
    font-weight: 600;
    color: var(--text-primary);
  }
  .tip-range {
    font-size: 10px;
    font-family: ui-monospace, monospace;
    color: var(--text-secondary);
  }
</style>
