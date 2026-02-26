export interface TreeNode {
  uri: string;
  node_type: string;
  category: string;
  l0_abstract?: string;
  children?: number;
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
  relevance: number;
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
