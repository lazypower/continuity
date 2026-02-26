import { writable } from 'svelte/store';

export type Tab = 'tree' | 'search' | 'profile';

export const activeTab = writable<Tab>('tree');

export const darkMode = writable<boolean>(
  typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches
);

// Apply dark mode class to document root
if (typeof window !== 'undefined') {
  darkMode.subscribe((dark) => {
    document.documentElement.classList.toggle('dark', dark);
  });
}
