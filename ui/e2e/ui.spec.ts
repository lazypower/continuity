import type { Page } from '@playwright/test';
import { expect, test } from './fixtures';
import { seedCount, seeds } from './seed';

const views = [
  { tab: 'Health', heading: 'Memory Health' },
  { tab: 'Tree', heading: 'Memory Tree' },
  { tab: 'Search', heading: 'Search Memories' },
  { tab: 'Profile', heading: 'Relational Profile' },
  { tab: 'Cold Boot', heading: 'Cold Boot' },
];

async function openView(page: Page, tab: string) {
  await page.getByRole('navigation').getByRole('button', { name: tab }).click();
}

// Expands a tree root (e.g. mem://user) and one category directory under it.
async function openTreeDir(page: Page, root: string, dir: string) {
  await page.goto('/');
  await openView(page, 'Tree');
  await page.getByRole('button', { name: new RegExp(`^\\W*${root} \\d+$`) }).click();
  await page.getByRole('button', { name: new RegExp(`^\\W*${dir} \\d+$`) }).click();
}

// The Cold Boot view's list of pinned memories.
function pinnedSection(page: Page) {
  return page.locator('section').filter({ has: page.getByRole('heading', { name: 'Pinned' }) });
}

test('app shell loads with navigation for every view', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveTitle(/Continuity/);
  await expect(page.getByRole('heading', { level: 1, name: 'Continuity' })).toBeVisible();
  const nav = page.getByRole('navigation');
  for (const v of views) {
    await expect(nav.getByRole('button', { name: v.tab })).toBeVisible();
  }
  // Health is the landing view.
  await expect(page.getByRole('heading', { level: 2, name: 'Memory Health' })).toBeVisible();
});

test('each nav view renders its heading', async ({ page }) => {
  await page.goto('/');
  for (const v of views) {
    await openView(page, v.tab);
    await expect(page.getByRole('heading', { level: 2, name: v.heading })).toBeVisible();
  }
});

test('theme toggle switches between dark and light', async ({ page }) => {
  await page.goto('/');
  const html = page.locator('html');
  await expect(html).toHaveClass(/\bdark\b/);
  await page.getByRole('button', { name: 'Toggle theme' }).click();
  await expect(html).toHaveClass(/\blight\b/);
  await page.getByRole('button', { name: 'Toggle theme' }).click();
  await expect(html).toHaveClass(/\bdark\b/);
});

test('health view summarizes seeded memories and draws its charts', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText(`${seedCount} active · 0 retracted`)).toBeVisible();
  await expect(page.getByText('Healthy — most memory is fresh and reachable.')).toBeVisible();

  const byCategory = page.locator('.card', { has: page.getByRole('heading', { name: 'By category' }) });
  for (const m of Object.values(seeds)) {
    await expect(byCategory.getByText(m.category, { exact: true })).toBeVisible();
  }

  // Decay histogram (d3): a drawn ridge, and every seed in the fresh band so
  // the y-axis max equals the seed count.
  const histogram = page.getByRole('img', { name: 'Relevance distribution of active memories' });
  await expect(histogram).toBeVisible();
  await expect(histogram.locator('path').first()).toHaveAttribute('d', /^M[\d.]/);
  await expect(histogram.getByText(String(seedCount), { exact: true })).toBeVisible();

  // Effectiveness sparkline (d3): today's captures are the seeds.
  const sparkline = page.getByRole('img', { name: 'Captures and retrievals per day' });
  await expect(sparkline).toBeVisible();
  await expect(sparkline.locator('path').first()).toHaveAttribute('d', /^M[\d.]/);
  await expect(page.getByText(`${seedCount} captures`)).toBeVisible();
});

test('tree browses to a seeded memory and expands its body', async ({ page }) => {
  await openTreeDir(page, 'mem://user', 'preferences');
  await page.getByText(seeds.devbox.summary).click();
  await expect(page.getByText(seeds.devbox.body)).toBeVisible();
  await expect(page.getByRole('button', { name: 'Copy memory body to clipboard', exact: true })).toBeVisible();

  await page.getByRole('button', { name: /^\W*mem:\/\/agent \d+$/ }).click();
  await page.getByRole('button', { name: /^\W*patterns \d+$/ }).click();
  await expect(page.getByText(seeds.zebra.summary)).toBeVisible();
});

test('search finds a seeded memory and honors the category filter', async ({ page }) => {
  await page.goto('/');
  await openView(page, 'Search');
  const query = page.getByPlaceholder('What are you looking for?');
  await query.fill('zebra heron');
  await query.press('Enter');
  await expect(page.getByText('1 result', { exact: true })).toBeVisible();
  await expect(page.getByText(seeds.zebra.summary)).toBeVisible();

  await page.getByRole('combobox').nth(1).selectOption('preferences');
  await page.getByRole('button', { name: 'Search', exact: true }).click();
  await expect(page.getByText('0 results', { exact: true })).toBeVisible();
  await expect(page.getByText('No memories match that query.')).toBeVisible();
});

test('profile view lists preference memories', async ({ page }) => {
  await page.goto('/');
  await openView(page, 'Profile');
  await expect(page.getByText('No relational profile yet')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Profile & Preferences' })).toBeVisible();
  await expect(page.getByText(seeds.devbox.summary)).toBeVisible();
});

test('cold boot view shows pins and the verbatim injection', async ({ page }) => {
  await page.goto('/');
  await openView(page, 'Cold Boot');
  const pinned = pinnedSection(page);
  await expect(pinned.getByText(seeds.runbook.summary)).toBeVisible();
  await expect(pinned.getByRole('button', { name: `Unpin ${seeds.runbook.uri}`, exact: true })).toBeVisible();
  // The injection is what SessionStart receives: the index and the pinned memory.
  const injected = page.locator('pre');
  await expect(injected).toContainText('Memory Index');
  await expect(injected).toContainText(`${seedCount} memories`);
  await expect(injected).toContainText(seeds.runbook.summary);
});

test('pinning from the tree reaches the cold boot view and unpins there', async ({ page }) => {
  const { uri, summary } = seeds.zebra;
  await openTreeDir(page, 'mem://agent', 'patterns');
  const pin = page.getByRole('button', { name: `Pin ${uri}`, exact: true });
  await pin.click();
  const unpinInTree = page.getByRole('button', { name: `Unpin ${uri}`, exact: true });
  await expect(unpinInTree).toHaveAttribute('aria-pressed', 'true');

  await openView(page, 'Cold Boot');
  const pinned = pinnedSection(page);
  await expect(pinned.getByText(summary)).toBeVisible();
  await expect(page.locator('pre')).toContainText(summary);
  await pinned.getByRole('button', { name: `Unpin ${uri}`, exact: true }).click();
  await expect(pinned.getByText(summary)).toBeHidden();
  await expect(page.locator('pre')).not.toContainText(summary);
  // The seeded pin is untouched.
  await expect(pinned.getByText(seeds.runbook.summary)).toBeVisible();
});
