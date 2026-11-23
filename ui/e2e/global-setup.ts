import { execFileSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { baseURL, bin, continuityEnv, dataDir } from './env';
import { pinnedSeeds, seeds } from './seed';

// Runs after Playwright's webServer reports the server healthy. Writes the
// fixture memories through the real CLI and pins through the HTTP API, the
// same paths an agent and the UI use.
export default async function globalSetup(): Promise<() => void> {
  const env = { ...process.env, ...continuityEnv };
  for (const m of Object.values(seeds)) {
    execFileSync(bin, ['remember', '-c', m.category, '-n', m.name, '-s', m.summary, '-b', m.body], {
      env,
      stdio: 'pipe',
    });
  }
  for (const m of pinnedSeeds) {
    const res = await fetch(`${baseURL}/api/memories/pin`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uri: m.uri }),
    });
    if (!res.ok) throw new Error(`pin ${m.uri}: ${res.status} ${await res.text()}`);
  }
  return () => rmSync(dataDir, { recursive: true, force: true });
}
