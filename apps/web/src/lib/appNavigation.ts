import { helpDocFromSlug, isHelpDocRoute, type HelpDocId } from "./helpDocs";
import type { Screen } from "./types";

const SCREENS: Screen[] = [
  "dashboard",
  "transfer",
  "pilot",
  "query",
  "connectors",
  "contracts",
  "schedules",
  "transforms",
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

/** Friendly URL aliases → Screen ids. `pipelines` stays readable so links
 * shared before the workspace settled on “Schedules” keep resolving.
 * Do not alias `home` — that path is the public marketing route. */
const HASH_ALIASES: Record<string, Screen> = {
  pipelines: "schedules",
  pipeline: "schedules",
  overview: "dashboard",
  studio: "transfer",
  theater: "jobs",
  proofs: "benchmarks",
  proof: "benchmarks",
};

export function screenFromHash(hash: string): Screen | null {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  if (!raw || raw === "landing") return null;
  if (HASH_ALIASES[raw]) return HASH_ALIASES[raw];
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

/** Prefer operator-facing path segments when writing the hash — the segment
 * must match the label the operator sees, so `schedules` writes itself. */
const HASH_WRITE: Partial<Record<Screen, string>> = {
  dashboard: "overview",
};

export function hashForScreen(screen: Screen, focus?: { jobId?: string; panel?: string }): string {
  const segment = HASH_WRITE[screen] || screen;
  const base = `#/${segment}`;
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

/**
 * Help hashes (`#/help`, `#/help/<slug>`, aliases) are public marketing when
 * signed out. Signed-in operators must stay in the workspace Help screen —
 * dumping them to MarketingSite was P2-5.
 */
export function isSignedInHelpHash(hash: string): boolean {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  if (!raw) return false;
  if (raw === "help" || raw === "guide" || raw === "documentation") return true;
  if (raw.startsWith("help/")) return true;
  if (raw.startsWith("help-")) return true;
  return false;
}

/** Screen to open when a stored session exists. Help public hashes map to docs. */
export function signedInScreenFromHash(hash: string): Screen | null {
  const screen = screenFromHash(hash);
  if (screen) return screen;
  if (isSignedInHelpHash(hash)) return "docs";
  return null;
}

/**
 * Which operator-guide article a signed-in help hash should open.
 * `#/help` is the space home; `#/help/<slug>` and `#/help-<id>` are articles.
 * Returns null when the hash is not a help route (sidebar `#/docs` stays the walkthrough).
 */
export function signedInHelpArticleFromHash(hash: string): HelpDocId | "help" | null {
  if (!isSignedInHelpHash(hash)) return null;
  const raw = hash.replace(/^#\/?/, "").split("?")[0].split("#")[0].trim().toLowerCase();
  const helpMatch = raw.match(/^help\/([a-z0-9-]+)$/);
  if (helpMatch) return helpDocFromSlug(helpMatch[1]) ?? "help";
  if (isHelpDocRoute(raw)) return raw;
  return "help";
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
