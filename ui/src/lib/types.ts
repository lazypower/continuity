// `retracted` is set true by the API when a memory has been tombstoned via
// `continuity retract`. Default-read API paths (search, tree default,
// context injection) suppress retracted nodes server-side, so this field
// arriving as `true` indicates either an explicit `?include_retracted=true`
// inspection request, or a server-side change in default behavior. Renderers
// MUST handle this field defensively — a retracted node should never display
// as if it were live, even if upstream filtering changes. See issue #12.
export interface TreeNode {
  uri: string;
  node_type: string;
  category: string;
  l0_abstract?: string;
  l1_overview?: string;
  children?: number;
  retracted?: boolean;
  pinned?: boolean;
}

// A live operator pin (GET /api/memories/pinned). Pins are the declared half of
// the operating contract — memories that ride every cold SessionStart. The list
// excludes retracted nodes server-side, so anything here is genuinely injected.
export interface PinnedMemory {
  uri: string;
  category: string;
  l0_abstract: string;
  l1_overview?: string;
  relevance: number;
  pinned_at: number;
}

export interface PinnedResponse {
  count: number;
  pins: PinnedMemory[] | null;
}

// The verbatim cold-boot injection (GET /api/context with no session). This is
// exactly what an agent wakes up with — the honesty instrument for the tray.
export interface ContextResponse {
  context: string;
}

export interface TreeResponse {
  uri: string;
  nodes: TreeNode[] | null;
}

export interface SearchResult {
  uri: string;
  category: string;
  l0_abstract: string;
  l1_overview?: string;
  score: number;
  similarity: number;
  relevance: number;
  retracted?: boolean;
}

export interface SearchResponse {
  query: string;
  mode: string;
  count: number;
  results: SearchResult[] | null;
}

export interface ProfileNode {
  uri: string;
  category: string;
  l0_abstract: string;
  l1_overview?: string;
  relevance: number;
  retracted?: boolean;
}

export interface ProfileResponse {
  relational_profile: string;
  nodes: ProfileNode[] | null;
}

export interface HealthResponse {
  status: string;
  version: string;
  uptime: number;
  db: boolean;
}

export type Category =
  | 'profile' | 'preferences' | 'feedback' | 'entities' | 'events'
  | 'patterns' | 'cases' | 'reference' | 'moments' | 'session';

export const categoryColors: Record<Category, string> = {
  profile: 'var(--color-profile)',
  preferences: 'var(--color-preferences)',
  feedback: 'var(--color-feedback)',
  entities: 'var(--color-entities)',
  events: 'var(--color-events)',
  patterns: 'var(--color-patterns)',
  cases: 'var(--color-cases)',
  reference: 'var(--color-reference)',
  moments: 'var(--color-accent)',
  session: 'var(--text-secondary)',
};

// ── Memory Health metrics (GET /api/metrics) ─────────────────────────────────

export interface MetricNode {
  uri: string;
  category: string;
  l0_abstract: string;
  relevance: number;
  access_count: number;
  last_access?: number;
  created_at: number;
  age_days: number;
}

export interface CategoryShare {
  category: string;
  count: number;
  share: number;
}

export interface HistBin {
  lo: number;
  hi: number;
  count: number;
}

export interface DailyPoint {
  date: string; // 'YYYY-MM-DD'
  captures: number;
  retrievals: number;
  active_total: number;
  fresh: number;
  fading: number;
  stale: number;
  has_snapshot: boolean;
}

export interface MetricsResponse {
  generated_at: number;
  summary: {
    active_total: number;
    retracted_total: number;
    fresh: number;
    fading: number;
    stale: number;
    never_retrieved: number;
    retraction_rate: number;
    recent_retractions: number;
  };
  categories: CategoryShare[];
  histogram: HistBin[];
  needs_attention: {
    stale_high_retrieval: MetricNode[] | null;
    never_retrieved_old: MetricNode[] | null;
    near_decay_cliff: MetricNode[] | null;
    orphaned_tombstones: MetricNode[] | null;
  };
  critical: MetricNode[] | null;
  daily: DailyPoint[] | null;
}

// Freshness band thresholds — must match internal/store/metrics.go.
export const FRESH_THRESHOLD = 0.7;
export const STALE_THRESHOLD = 0.4;

export function bandColor(relevance: number): string {
  if (relevance >= FRESH_THRESHOLD) return 'var(--color-preferences)'; // emerald = healthy
  if (relevance < STALE_THRESHOLD) return 'var(--color-cases)'; // rose = stale
  return 'var(--color-events)'; // amber = fading
}

export function bandLabel(relevance: number): string {
  if (relevance >= FRESH_THRESHOLD) return 'fresh';
  if (relevance < STALE_THRESHOLD) return 'stale';
  return 'fading';
}
