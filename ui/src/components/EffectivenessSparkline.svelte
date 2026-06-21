<script lang="ts">
  import { scaleLinear } from 'd3-scale';
  import { area, line, curveMonotoneX } from 'd3-shape';
  import { max } from 'd3-array';
  import type { DailyPoint } from '../lib/types';

  interface Props {
    daily: DailyPoint[];
  }
  let { daily }: Props = $props();

  const W = 760;
  const H = 110;
  const M = { top: 14, right: 12, bottom: 18, left: 28 };
  const iw = W - M.left - M.right;
  const ih = H - M.top - M.bottom;

  const totalCaptures = $derived(daily.reduce((s, d) => s + d.captures, 0));
  const totalRetrievals = $derived(daily.reduce((s, d) => s + d.retrievals, 0));
  const hasRetrievals = $derived(daily.some((d) => d.has_snapshot && d.retrievals > 0));

  // Shared y-axis: comparing capture vs retrieval volume is the whole point.
  const yMax = $derived(Math.max(1, max(daily, (d) => Math.max(d.captures, d.retrievals)) ?? 1));
  const x = $derived(
    scaleLinear().domain([0, Math.max(1, daily.length - 1)]).range([M.left, M.left + iw]),
  );
  const y = $derived(scaleLinear().domain([0, yMax]).range([M.top + ih, M.top]));

  const capArea = $derived(
    area<DailyPoint>()
      .x((_, i) => x(i))
      .y0(M.top + ih)
      .y1((d) => y(d.captures))
      .curve(curveMonotoneX)(daily) ?? '',
  );
  const capLine = $derived(
    line<DailyPoint>()
      .x((_, i) => x(i))
      .y((d) => y(d.captures))
      .curve(curveMonotoneX)(daily) ?? '',
  );
  const retLine = $derived(
    line<DailyPoint>()
      .x((_, i) => x(i))
      .y((d) => y(d.retrievals))
      .curve(curveMonotoneX)(daily) ?? '',
  );

  let hover = $state<{ d: DailyPoint; cx: number } | null>(null);
  function onMove(e: PointerEvent) {
    const svg = e.currentTarget as SVGSVGElement;
    const r = svg.getBoundingClientRect();
    const px = ((e.clientX - r.left) / r.width) * W;
    let i = Math.round(x.invert(px));
    if (i < 0) i = 0;
    if (i > daily.length - 1) i = daily.length - 1;
    if (daily[i]) hover = { d: daily[i], cx: x(i) };
  }
  function onLeave() {
    hover = null;
  }

  const firstDate = $derived(daily.length ? daily[0].date.slice(5) : '');
  const lastDate = $derived(daily.length ? daily[daily.length - 1].date.slice(5) : '');
</script>

<div class="relative">
  <div class="flex items-center gap-4 mb-1 text-[10px] font-mono">
    <span style="color: var(--color-patterns)">● {totalCaptures} captures</span>
    <span style="color: var(--color-accent)" class:dim={!hasRetrievals}>
      ● {totalRetrievals} retrievals{hasRetrievals ? '' : ' (accruing)'}
    </span>
    <span class="ml-auto text-[var(--text-secondary)] opacity-50">last {daily.length}d</span>
  </div>

  <svg
    viewBox="0 0 {W} {H}"
    class="w-full"
    style="height: auto"
    role="img"
    aria-label="Captures and retrievals per day"
    onpointermove={onMove}
    onpointerleave={onLeave}
  >
    <line x1={M.left} y1={M.top + ih} x2={M.left + iw} y2={M.top + ih}
      stroke="var(--border)" stroke-width="1" />

    <path d={capArea} fill="var(--color-patterns)" opacity="0.12" />
    <path d={capLine} fill="none" stroke="var(--color-patterns)" stroke-width="2" />
    <path d={retLine} fill="none" stroke="var(--color-accent)" stroke-width="1.5"
      stroke-dasharray={hasRetrievals ? 'none' : '3 3'} opacity={hasRetrievals ? 0.95 : 0.4} />

    {#if hover}
      <line x1={hover.cx} y1={M.top} x2={hover.cx} y2={M.top + ih}
        stroke="var(--color-accent)" stroke-width="1" opacity="0.5" />
      <circle cx={hover.cx} cy={y(hover.d.captures)} r="3" fill="var(--color-patterns)" />
      <circle cx={hover.cx} cy={y(hover.d.retrievals)} r="3" fill="var(--color-accent)" />
    {/if}

    <text x={M.left} y={H - 4} class="axis-label" text-anchor="start">{firstDate}</text>
    <text x={M.left + iw} y={H - 4} class="axis-label" text-anchor="end">{lastDate}</text>
    <text x={M.left - 6} y={M.top + 8} class="axis-label" text-anchor="end">{yMax}</text>
  </svg>

  {#if hover}
    <div class="tooltip" style="left: {(hover.cx / W) * 100}%">
      <span class="tip-date">{hover.d.date}</span>
      <span style="color: var(--color-patterns)">{hover.d.captures} captured</span>
      <span style="color: var(--color-accent)">
        {hover.d.has_snapshot ? `${hover.d.retrievals} retrieved` : 'no snapshot'}
      </span>
    </div>
  {/if}
</div>

<style>
  .axis-label {
    font-size: 9px;
    font-family: ui-monospace, monospace;
    fill: var(--text-secondary);
    opacity: 0.5;
  }
  .dim {
    opacity: 0.55;
  }
  .tooltip {
    position: absolute;
    top: 18px;
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
    font-size: 10px;
    font-family: ui-monospace, monospace;
  }
  .tip-date {
    color: var(--text-secondary);
    margin-bottom: 1px;
  }
</style>
