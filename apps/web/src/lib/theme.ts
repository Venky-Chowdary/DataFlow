/**
 * Workspace appearance — one preference, one token remap.
 *
 * Persistence is this browser (`df2.theme`). The CSS owner is tokens.css:
 * `[data-theme="dark"]` remaps `--df-*` primitives. Do not add a dark
 * stylesheet or `*-theme-dark` component rules.
 *
 * Applied on `<html>` so :root aliases re-resolve. Scoped in CSS with
 * `:has(.df2-app)` so marketing/login keep their own surfaces.
 */

export type ThemePreference = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

export const THEME_STORAGE_KEY = "df2.theme";
export const THEME_CHANGED_EVENT = "df2-theme-changed";

const PREFS = new Set<ThemePreference>(["light", "dark", "system"]);

export function isThemePreference(value: string | null | undefined): value is ThemePreference {
  return !!value && PREFS.has(value as ThemePreference);
}

export function readThemePreference(): ThemePreference {
  try {
    const raw = localStorage.getItem(THEME_STORAGE_KEY);
    if (isThemePreference(raw)) return raw;
  } catch {
    /* private mode */
  }
  return "light";
}

export function systemPrefersDark(): boolean {
  try {
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  } catch {
    return false;
  }
}

export function resolveTheme(preference: ThemePreference = readThemePreference()): ResolvedTheme {
  if (preference === "system") return systemPrefersDark() ? "dark" : "light";
  return preference;
}

function writePreference(preference: ThemePreference) {
  try {
    localStorage.setItem(THEME_STORAGE_KEY, preference);
  } catch {
    /* private mode — still apply for this tab */
  }
}

function paintTheme(resolved: ResolvedTheme) {
  const root = document.documentElement;
  if (resolved === "dark") {
    root.setAttribute("data-theme", "dark");
    root.classList.add("df2-theme-dark");
  } else {
    root.removeAttribute("data-theme");
    root.classList.remove("df2-theme-dark");
  }
  const meta = document.querySelector('meta[name="color-scheme"]');
  if (meta) {
    meta.setAttribute?.("content", resolved);
    (meta as { content?: string }).content = resolved;
  }
}

/** Apply preference (and persist). Safe to call before React mounts. */
export function applyTheme(preference: ThemePreference): ResolvedTheme {
  writePreference(preference);
  const resolved = resolveTheme(preference);
  paintTheme(resolved);
  try {
    window.dispatchEvent(new CustomEvent(THEME_CHANGED_EVENT, { detail: { preference, resolved } }));
  } catch {
    /* node tests without Event */
  }
  return resolved;
}

export function bootTheme(): ResolvedTheme {
  return applyTheme(readThemePreference());
}
