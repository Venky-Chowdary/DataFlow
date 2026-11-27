/**
 * Run: npx --yes tsx --test apps/web/src/lib/lazyPage.test.ts
 */
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import {
  STALE_CHUNK_RELOAD_KEY,
  alreadyReloadedForStaleChunk,
  clearStaleChunkReloadGuard,
  expireAfterLoad,
  isStaleChunkError,
  lazyNamed,
  pageErrorCopy,
  reloadOnceForStaleChunk,
  shouldAutoReloadStaleChunk,
} from "./lazyPage.js";

const memory = new Map<string, string>();

const storage: Storage = {
  get length() {
    return memory.size;
  },
  clear() {
    memory.clear();
  },
  getItem(key: string) {
    return memory.has(key) ? memory.get(key)! : null;
  },
  key(index: number) {
    return [...memory.keys()][index] ?? null;
  },
  removeItem(key: string) {
    memory.delete(key);
  },
  setItem(key: string, value: string) {
    memory.set(key, value);
  },
};

function installBrowserStubs() {
  memory.clear();
  Object.defineProperty(globalThis, "sessionStorage", {
    configurable: true,
    value: storage,
  });
  let reloads = 0;
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: {
      location: {
        reload() {
          reloads += 1;
        },
      },
    },
  });
  return {
    reloads: () => reloads,
  };
}

afterEach(() => {
  clearStaleChunkReloadGuard();
  memory.clear();
});

describe("isStaleChunkError", () => {
  it("matches the production Vite Overview failure", () => {
    const err = new Error(
      "Failed to fetch dynamically imported module: https://www.datawrap.io/assets/DashboardPage-9rFp2JXv.js",
    );
    assert.equal(isStaleChunkError(err), true);
  });

  it("matches Firefox and webpack wordings", () => {
    assert.equal(isStaleChunkError(new Error("error loading dynamically imported module")), true);
    assert.equal(isStaleChunkError(new Error("Importing a module script failed.")), true);
    assert.equal(isStaleChunkError(new Error("Loading chunk 7 failed")), true);
    const named = new Error("chunk");
    named.name = "ChunkLoadError";
    assert.equal(isStaleChunkError(named), true);
  });

  it("does not treat render bugs as stale chunks", () => {
    assert.equal(isStaleChunkError(new Error("Cannot read properties of undefined")), false);
    assert.equal(isStaleChunkError(null), false);
  });
});

describe("pageErrorCopy", () => {
  it("never surfaces the Vite asset URL", () => {
    const copy = pageErrorCopy(
      "Overview",
      new Error(
        "Failed to fetch dynamically imported module: https://www.datawrap.io/assets/DashboardPage-9rFp2JXv.js",
      ),
    );
    assert.equal(copy.reload, true);
    assert.equal(copy.title, "Overview needs a refresh");
    assert.equal(copy.description.includes("datawrap.io"), false);
    assert.equal(copy.description.includes("DashboardPage"), false);
    assert.equal(copy.description.includes("Failed to fetch"), false);
  });

  it("keeps render failures generic", () => {
    const copy = pageErrorCopy("Transfer Studio", new Error("boom at line 12"));
    assert.equal(copy.reload, false);
    assert.equal(copy.title, "Transfer Studio hit an unexpected error");
    assert.equal(copy.description.includes("boom"), false);
  });
});

describe("reloadOnceForStaleChunk", () => {
  it("reloads once, then refuses a loop", () => {
    const browser = installBrowserStubs();
    const err = new Error("Failed to fetch dynamically imported module: /assets/x.js");
    assert.equal(shouldAutoReloadStaleChunk(err), true);
    assert.equal(reloadOnceForStaleChunk(err), true);
    assert.equal(browser.reloads(), 1);
    assert.equal(alreadyReloadedForStaleChunk(), true);
    assert.equal(shouldAutoReloadStaleChunk(err), false);
    assert.equal(reloadOnceForStaleChunk(err), true);
    assert.equal(browser.reloads(), 1);
    const guard = memory.get(STALE_CHUNK_RELOAD_KEY);
    clearStaleChunkReloadGuard();
    if (guard) memory.set(STALE_CHUNK_RELOAD_KEY, guard);
    assert.equal(alreadyReloadedForStaleChunk(), true);
    assert.equal(reloadOnceForStaleChunk(err), false);
    assert.equal(browser.reloads(), 1);
  });

  it("ignores non-chunk errors", () => {
    const browser = installBrowserStubs();
    assert.equal(reloadOnceForStaleChunk(new Error("render")), false);
    assert.equal(browser.reloads(), 0);
  });
});

describe("expireAfterLoad (deep link survives a slow screen chunk)", () => {
  it("does not expire before the screen's code has loaded", async () => {
    let resolve!: () => void;
    const loaded = new Promise<void>((r) => { resolve = r; });
    let expired = false;
    expireAfterLoad(loaded, 5, () => { expired = true; });
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(expired, false, "a 30 ms chunk load outlived a 5 ms timer started at the click");
    resolve();
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(expired, true);
  });

  it("still expires when the chunk fails to load, and cancel stops it", async () => {
    let fired = 0;
    expireAfterLoad(Promise.reject(new Error("chunk 404")), 1, () => { fired += 1; });
    const cancel = expireAfterLoad(Promise.resolve(), 20, () => { fired += 10; });
    cancel();
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(fired, 1);
  });

  it("lazyNamed exposes whenLoaded, which never rejects", async () => {
    const ok = lazyNamed(async () => ({ Page: () => null }), "Page");
    const bad = lazyNamed(async (): Promise<{ Page: () => null }> => { throw new Error("offline"); }, "Page");
    await ok.whenLoaded();
    await bad.whenLoaded();
  });
});
