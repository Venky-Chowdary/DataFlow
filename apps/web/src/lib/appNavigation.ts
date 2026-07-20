import type { Screen } from "./types";

const SCREENS: Screen[] = [
  "dashboard",
  "transfer",
  "pilot",
  "query",
  "connectors",
  "contracts",
  "schedules",
  "jobs",
  "mcp",
  "settings",
  "docs",
  "benchmarks",
];

export type AppHashFocus = {
  screen: Screen;
  jobId?: string;
  panel?: string;
};

export function screenFromHash(hash: string): Screen | null {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  if (!raw || raw === "landing") return null;
  return SCREENS.includes(raw as Screen) ? (raw as Screen) : null;
}

/** Parse `#/jobs?jobId=…&panel=mapping-proof` style deep-links. */
export function focusFromHash(hash: string): AppHashFocus | null {
  const screen = screenFromHash(hash);
  if (!screen) return null;
  const qIdx = hash.indexOf("?");
  if (qIdx < 0) return { screen };
  const params = new URLSearchParams(hash.slice(qIdx + 1));
  const jobId = (params.get("jobId") || params.get("job") || "").trim() || undefined;
  const panel = (params.get("panel") || "").trim() || undefined;
  return { screen, jobId, panel };
}

export function hashForScreen(screen: Screen, focus?: { jobId?: string; panel?: string }): string {
  const base = `#/${screen}`;
  if (!focus?.jobId && !focus?.panel) return base;
  const params = new URLSearchParams();
  if (focus.jobId) params.set("jobId", focus.jobId);
  if (focus.panel) params.set("panel", focus.panel);
  const qs = params.toString();
  return qs ? `${base}?${qs}` : base;
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

/** Write jobs deep-link without wiping other screens' keep-alive state. */
export function writeJobsDeepLink(jobId: string, panel?: string) {
  if (typeof window === "undefined") return;
  window.location.hash = hashForScreen("jobs", { jobId, panel });
}
