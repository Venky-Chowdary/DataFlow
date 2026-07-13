import type { Screen } from "./types";

const SCREENS: Screen[] = [
  "dashboard",
  "transfer",
  "pilot",
  "connectors",
  "schedules",
  "jobs",
  "mcp",
  "settings",
  "docs",
];

export function screenFromHash(hash: string): Screen | null {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  if (!raw || raw === "landing") return null;
  return SCREENS.includes(raw as Screen) ? (raw as Screen) : null;
}

export function hashForScreen(screen: Screen): string {
  return `#/${screen}`;
}

export function readAppHash(): Screen | null {
  if (typeof window === "undefined") return null;
  return screenFromHash(window.location.hash);
}

export function writeAppHash(screen: Screen, replace = false) {
  if (typeof window === "undefined") return;
  const next = hashForScreen(screen);
  if (window.location.hash === next) return;
  if (replace) {
    window.history.replaceState(null, "", next);
  } else {
    window.location.hash = next;
  }
}
