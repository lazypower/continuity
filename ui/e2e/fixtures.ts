import { test as base, expect } from '@playwright/test';

// Every test fails if the page logs console.error, throws an uncaught
// exception, drops a request, or receives an HTTP error response.
export const test = base.extend<{ browserErrors: void }>({
  browserErrors: [
    async ({ page }, use) => {
      const errors: string[] = [];
      page.on('console', (m) => {
        if (m.type() === 'error') errors.push(`console.error: ${m.text()}`);
      });
      page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
      page.on('requestfailed', (r) => errors.push(`request failed: ${r.url()} ${r.failure()?.errorText ?? ''}`));
      page.on('response', (r) => {
        if (r.status() >= 400) errors.push(`HTTP ${r.status()}: ${r.url()}`);
      });
      await use();
      expect(errors, 'browser errors during test').toEqual([]);
    },
    { auto: true },
  ],
});

export { expect };
