import type {
  TreeResponse, SearchResponse, ProfileResponse, HealthResponse, MetricsResponse,
  PinnedResponse, ContextResponse,
} from './types';

const BASE = '/api';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    // Error responses carry a JSON {error} message; surface it verbatim.
    let msg = `${res.status}`;
    try {
      const j = await res.json();
      if (j && j.error) msg = `${res.status}: ${j.error}`;
    } catch {
      // non-JSON body; fall back to status
    }
    throw new Error(msg);
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

export function fetchMetrics(): Promise<MetricsResponse> {
  return get('/metrics');
}

export function fetchPinned(): Promise<PinnedResponse> {
  return get('/memories/pinned');
}

// The verbatim cold-boot injection. preview=true renders exactly what a fresh
// SessionStart receives WITHOUT advancing moment rotation — previewing the tray
// must not consume the rotation it shows.
export function fetchColdBootContext(): Promise<ContextResponse> {
  return get('/context?preview=true');
}

export function pinMemory(uri: string): Promise<{ status: string; uri: string }> {
  return post('/memories/pin', { uri });
}

export function unpinMemory(uri: string): Promise<{ status: string; uri: string }> {
  return post('/memories/unpin', { uri });
}
