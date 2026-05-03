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

export type Category = 'profile' | 'preferences' | 'entities' | 'events' | 'patterns' | 'cases';

export const categoryColors: Record<Category, string> = {
  profile: 'var(--color-profile)',
  preferences: 'var(--color-preferences)',
  entities: 'var(--color-entities)',
  events: 'var(--color-events)',
  patterns: 'var(--color-patterns)',
  cases: 'var(--color-cases)',
};
