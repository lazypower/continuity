// Memories the global setup writes through the real CLI. Tests assert against
// these values, so this is the single source for fixture content.
export interface SeedMemory {
  category: string;
  name: string;
  summary: string;
  body: string;
  uri: string;
}

export const seeds = {
  devbox: {
    category: 'preferences',
    name: 'e2e-devbox',
    summary: 'Always use devbox for development tooling',
    body: 'Devbox provides the Go and Node toolchains for this project.',
    uri: 'mem://user/preferences/e2e-devbox',
  },
  zebra: {
    category: 'patterns',
    name: 'zebra-heron',
    summary: 'Zebra heron pattern keeps fixtures deterministic',
    body: 'Seed fixtures through the real CLI so the UI renders server data.',
    uri: 'mem://agent/patterns/zebra-heron',
  },
  runbook: {
    category: 'reference',
    name: 'pinned-runbook',
    summary: 'Release runbook lives in the docs folder',
    body: 'Pinned by the e2e setup so the cold-boot view has a pin to render.',
    uri: 'mem://user/reference/pinned-runbook',
  },
  terse: {
    category: 'feedback',
    name: 'terse-summaries',
    summary: 'Keep status updates terse',
    body: 'Rule: terse updates. Why: the diff carries the detail. How to apply: skip recaps.',
    uri: 'mem://user/feedback/terse-summaries',
  },
} satisfies Record<string, SeedMemory>;

export const seedCount = Object.keys(seeds).length;

// Seeds the setup pins through the HTTP API.
export const pinnedSeeds: SeedMemory[] = [seeds.runbook];
