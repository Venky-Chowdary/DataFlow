/**
 * Run: npx --yes tsx --test apps/web/src/lib/workspace.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

type Handler = (ev: { type: string; detail?: unknown }) => void;

function installMemoryStorage() {
  const store = new Map<string, string>();
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
  const win = {
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
  (globalThis as { CustomEvent?: unknown }).CustomEvent = class CustomEvent {
    type: string;
    detail: unknown;
    constructor(type: string, init?: { detail?: unknown }) {
      this.type = type;
      this.detail = init?.detail;
    }
  };
  (globalThis as { Event?: unknown }).Event = class Event {
    type: string;
    constructor(type: string) {
      this.type = type;
    }
  };
}

installMemoryStorage();

const {
  WORKSPACE_CHANGED_EVENT,
  WORKSPACE_DIRECTORY_EVENT,
  clearActiveWorkspaceId,
  getActiveWorkspaceId,
  notifyWorkspaceDirectory,
  setActiveWorkspaceId,
} = await import("./workspace.ts");

describe("active workspace SSOT", () => {
  it("stores the named workspace and no-ops when the id is unchanged", () => {
    clearActiveWorkspaceId();
    assert.equal(getActiveWorkspaceId(), "");
    const seen: string[] = [];
    const onChange = (ev: { type: string; detail?: unknown }) => {
      seen.push(String((ev.detail as { workspaceId?: string } | undefined)?.workspaceId ?? ""));
    };
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onChange);
    setActiveWorkspaceId("ws-ops");
    setActiveWorkspaceId("ws-ops");
    setActiveWorkspaceId("ws-finance");
    window.removeEventListener(WORKSPACE_CHANGED_EVENT, onChange);
    assert.equal(getActiveWorkspaceId(), "ws-finance");
    assert.deepEqual(seen, ["ws-ops", "ws-finance"]);
  });

  it("directory notify is a distinct event from switching workspace", () => {
    let directory = 0;
    let changed = 0;
    const onDir = () => {
      directory += 1;
    };
    const onChange = () => {
      changed += 1;
    };
    window.addEventListener(WORKSPACE_DIRECTORY_EVENT, onDir);
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onChange);
    notifyWorkspaceDirectory();
    window.removeEventListener(WORKSPACE_DIRECTORY_EVENT, onDir);
    window.removeEventListener(WORKSPACE_CHANGED_EVENT, onChange);
    assert.equal(directory, 1);
    assert.equal(changed, 0);
  });
});
