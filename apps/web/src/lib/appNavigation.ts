import { helpDocFromSlug, isHelpDocRoute, type HelpDocId } from "./helpDocs";
import type { Screen } from "./types";

const SCREENS: Screen[] = [
  "dashboard",
  "transfer",
  "pilot",
  "query",
  "connectors",
  "contracts",
  "exit",
  "schedules",
  "transforms",
  "transform-library",
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
  library: "transform-library",
  "exit-readiness": "exit",
  readiness: "exit",
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
 * Emailed password-reset links land on `/reset-password?token=…`; after the
 * page strips the token the route lives on as `#/reset-password`. Both forms
 * belong to the reset-link stage — the generic hash router must not bounce
 * them back to landing/app.
 */
export function isResetLinkRoute(pathname: string, hash: string): boolean {
  return pathname === "/reset-password"
    || hash.split("?")[0] === "#/reset-password";
}

/**
 * The emailed link is `/reset-password#token=…` — the token rides the URL
 * fragment so it can never reach a server/CDN/proxy access log or a Referer
 * header. Returns "" for any other route.
 */
export function resetLinkToken(pathname: string, hash: string): string {
  return emailedLinkToken("/reset-password", pathname, hash);
}

/** Workspace invitation links: `/accept-invite#token=…`, then `#/accept-invite`. */
export function isInviteLinkRoute(pathname: string, hash: string): boolean {
  return pathname === "/accept-invite"
    || hash.split("?")[0] === "#/accept-invite";
}

export function inviteLinkToken(pathname: string, hash: string): string {
  return emailedLinkToken("/accept-invite", pathname, hash);
}

/** Token from an emailed `<route>#token=…` link; "" on any other path. */
function emailedLinkToken(route: string, pathname: string, hash: string): string {
  if (pathname !== route) return "";
  return new URLSearchParams(hash.replace(/^#/, "")).get("token") ?? "";
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

/**
 * Hash to normalise to for ``screen``. A replace (boot / sign-in normalisation)
 * keeps a ``?jobId=…&panel=…`` focus already on the same screen, so a fresh
 * ``#/jobs?jobId=<id>`` load is not rewritten to ``#/jobs`` before it is read.
 */
export function normalizedAppHash(screen: Screen, currentHash: string, replace: boolean): string {
  const focus = replace ? focusFromHash(currentHash) : null;
  if (focus?.screen === screen) return hashForScreen(screen, focus);
  return hashForScreen(screen);
}

export function writeAppHash(screen: Screen, replace = false) {
  if (typeof window === "undefined") return;
  const next = normalizedAppHash(screen, window.location.hash, replace);
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

/* ── Exit readiness deep links ─────────────────────────────── */

export interface ExitFocus {
  system?: string;
  rehearsal?: string;
}

/** `#/exit?system=…&rehearsal=…` (the copilot's link) → the ids to open. */
export function exitFocusFromHash(hash: string): ExitFocus | null {
  const raw = hash.replace(/^#\/?/, "");
  const [path, query = ""] = raw.split("?");
  if (path.trim().toLowerCase() !== "exit") return null;
  const params = new URLSearchParams(query);
  const system = (params.get("system") || "").trim() || undefined;
  const rehearsal = (params.get("rehearsal") || "").trim() || undefined;
  return system || rehearsal ? { system, rehearsal } : {};
}

export function exitHash(focus: ExitFocus = {}): string {
  const params = new URLSearchParams();
  if (focus.system) params.set("system", focus.system);
  if (focus.rehearsal) params.set("rehearsal", focus.rehearsal);
  const qs = params.toString();
  return qs ? `#/exit?${qs}` : "#/exit";
}
