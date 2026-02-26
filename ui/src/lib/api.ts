import type { TreeResponse, SearchResponse, ProfileResponse, HealthResponse } from './types';

const BASE = '/api';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

export function fetchHealth(): Promise<HealthResponse> {
  return get('/health');
}

export function fetchTree(uri?: string): Promise<TreeResponse> {
  const params = uri ? `?uri=${encodeURIComponent(uri)}` : '';
  return get(`/tree${params}`);
}

export function fetchSearch(
  query: string,
  mode: 'find' | 'search' = 'find',
  limit: number = 10,
  category?: string,
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, mode, limit: String(limit) });
  if (category) params.set('category', category);
  return get(`/search?${params}`);
}

export function fetchProfile(): Promise<ProfileResponse> {
  return get('/profile');
}
