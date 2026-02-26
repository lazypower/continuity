import { writable } from 'svelte/store';

export type Tab = 'tree' | 'search' | 'profile';

export const activeTab = writable<Tab>('tree');

// Default to dark mode (matches the brand), respect saved preference
function getInitialDarkMode(): boolean {
  if (typeof window === 'undefined') return true;
  const saved = localStorage.getItem('continuity-dark-mode');
  if (saved !== null) return saved === 'true';
  return true; // dark-first
}

export const darkMode = writable<boolean>(getInitialDarkMode());

// Apply dark/light class to document root and persist preference
if (typeof window !== 'undefined') {
  darkMode.subscribe((dark) => {
    document.documentElement.classList.toggle('dark', dark);
    document.documentElement.classList.toggle('light', !dark);
    localStorage.setItem('continuity-dark-mode', String(dark));
  });
}
