import { defineConfig, devices } from '@playwright/test';
import { baseURL, bin, continuityEnv } from './e2e/env';

// End-to-end tests drive the embedded UI served by a real `continuity serve`
// with an isolated data dir, not the Vite dev server. Build the binary with
// the UI embedded first:
//
//   npm run build && rm -rf ../cmd/continuity/ui && cp -r dist ../cmd/continuity/ui
//   (cd .. && go build -o continuity ./cmd/continuity)
//   npm run test:e2e
//
// CONTINUITY_BIN overrides the binary path, CONTINUITY_E2E_PORT the port.
export default defineConfig({
  testDir: './e2e',
  globalSetup: './e2e/global-setup.ts',
  // Tests share one server and database; run them in order in one worker.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  // The suite runs in seconds against a local server; tight limits make a
  // broken view fail fast instead of waiting out the 30s default.
  timeout: 15_000,
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL,
    actionTimeout: 5_000,
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: `"${bin}" serve`,
    url: `${baseURL}/api/health`,
    env: continuityEnv,
    reuseExistingServer: false,
    timeout: 15_000,
    stdout: 'ignore',
    stderr: 'pipe',
  },
});
