import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

// The binary under test, built with the UI embedded. Defaults to the
// repository-root build output of `go build -o continuity ./cmd/continuity`.
export const bin = resolve(process.env.CONTINUITY_BIN ?? '../continuity');

export const port = Number(process.env.CONTINUITY_E2E_PORT ?? 47811);
export const baseURL = `http://127.0.0.1:${port}`;

// One isolated data dir per run. Playwright loads the config in the runner and
// again in each worker; workers inherit the runner's environment, so the dir
// is created once and shared through CONTINUITY_E2E_DIR.
function ensureDataDir(): string {
  const existing = process.env.CONTINUITY_E2E_DIR;
  if (existing) return existing;
  const dir = mkdtempSync(join(tmpdir(), 'continuity-e2e-'));
  // HOME isolates config.toml. Pointing the LLM at a closed local port means
  // nothing in the server can reach a real model, even by accident.
  mkdirSync(join(dir, 'home', '.continuity'), { recursive: true });
  writeFileSync(
    join(dir, 'home', '.continuity', 'config.toml'),
    '[llm]\nprovider = "ollama"\nollama_url = "http://127.0.0.1:9"\n',
  );
  process.env.CONTINUITY_E2E_DIR = dir;
  return dir;
}

export const dataDir = ensureDataDir();

// Environment shared by the server and the CLI that seeds it. Every path and
// address points into dataDir or at the test port, never at a real install.
export const continuityEnv: Record<string, string> = {
  HOME: join(dataDir, 'home'),
  CONTINUITY_DB: join(dataDir, 'continuity.db'),
  CONTINUITY_BIND: '127.0.0.1',
  CONTINUITY_PORT: String(port),
  CONTINUITY_URL: baseURL,
  CONTINUITY_EMBEDDER: 'tfidf',
  CONTINUITY_EXTRACTION_AUTO: 'false',
  CONTINUITY_RELATIONAL_AUTO: 'false',
  CONTINUITY_OBSERVATION_RETENTION_DAYS: 'off',
  ANTHROPIC_API_KEY: '',
  CLAUDE_CODE_SESSION_ID: '',
};
