/**
 * Run: npx --yes tsx --test apps/web/src/lib/theme.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

type Handler = (ev: { type: string; detail?: unknown }) => void;

function installDom() {
  const store = new Map<string, string>();
  const attrs = new Map<string, string>();
  const classes = new Set<string>();
  const metas = new Map<string, { content: string }>();
  const listeners = new Map<string, Set<Handler>>();

  const storage = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => {
      store.set(k, v);
    },
    removeItem: (k: string) => {
      store.delete(k);
    },
  };

  const root = {
    setAttribute: (k: string, v: string) => {
      attrs.set(k, v);
    },
    removeAttribute: (k: string) => {
      attrs.delete(k);
    },
    classList: {
      add: (c: string) => {
        classes.add(c);
      },
      remove: (c: string) => {
        classes.delete(c);
      },
    },
  };

  const win = {
    matchMedia: (q: string) => ({
      matches: q.includes("dark") ? false : false,
      addEventListener: () => {},
      removeEventListener: () => {},
    }),
    addEventListener: (type: string, fn: Handler) => {
      const set = listeners.get(type) ?? new Set();
      set.add(fn);
      listeners.set(type, set);
    },
    removeEventListener: (type: string, fn: Handler) => {
      listeners.get(type)?.delete(fn);
    },
    dispatchEvent: (ev: { type: string; detail?: unknown }) => {
      listeners.get(ev.type)?.forEach((fn) => fn(ev));
      return true;
    },
  };

  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage });
  Object.defineProperty(globalThis, "window", { configurable: true, value: win });
  Object.defineProperty(globalThis, "document", {
    configurable: true,
    value: {
      documentElement: root,
      querySelector: (sel: string) => {
        if (sel === 'meta[name="color-scheme"]') {
          const row = metas.get("color-scheme");
          if (!row) return null;
          return {
            content: row.content,
            setAttribute: (k: string, v: string) => {
              if (k === "content") row.content = v;
            },
          };
        }
        return null;
      },
    },
  });
  (globalThis as { CustomEvent?: unknown }).CustomEvent = class CustomEvent {
    type: string;
    detail: unknown;
    constructor(type: string, init?: { detail?: unknown }) {
      this.type = type;
      this.detail = init?.detail;
    }
  };

  return { store, attrs, classes, metas, storage };
}

const { attrs, classes, metas, store } = installDom();
metas.set("color-scheme", { content: "light" });

const {
  THEME_STORAGE_KEY,
  applyTheme,
  isThemePreference,
  readThemePreference,
  resolveTheme,
} = await import("./theme.ts");

describe("workspace theme preference", () => {
  it("rejects unknown storage values and defaults to light", () => {
    store.delete(THEME_STORAGE_KEY);
    assert.equal(readThemePreference(), "light");
    assert.equal(isThemePreference("midnight"), false);
    assert.equal(isThemePreference("dark"), true);
  });

  it("paints data-theme=dark on html and persists the preference", () => {
    const resolved = applyTheme("dark");
    assert.equal(resolved, "dark");
    assert.equal(store.get(THEME_STORAGE_KEY), "dark");
    assert.equal(attrs.get("data-theme"), "dark");
    assert.equal(classes.has("df2-theme-dark"), true);
    assert.equal(metas.get("color-scheme")?.content, "dark");
  });

  it("clears data-theme for light so :root tokens win (no sibling sheet)", () => {
    applyTheme("light");
    assert.equal(resolveTheme("light"), "light");
    assert.equal(attrs.has("data-theme"), false);
    assert.equal(classes.has("df2-theme-dark"), false);
    assert.equal(metas.get("color-scheme")?.content, "light");
  });
});
