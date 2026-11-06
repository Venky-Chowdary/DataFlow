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
  ALL_WORKSPACES_ID,
  UNASSIGNED_WORKSPACE_ID,
  WORKSPACE_CHANGED_EVENT,
  WORKSPACE_DIRECTORY_EVENT,
  clearActiveWorkspaceId,
  concreteWorkspaceId,
  getActiveWorkspaceId,
  isKnownWorkspaceChoice,
  landingWorkspaceId,
  notifyWorkspaceDirectory,
  setActiveWorkspaceId,
  workspaceChoices,
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

describe("landing workspace (D218)", () => {
  const rows = [{ id: "ws-oldest" }, { id: "ws-own" }, { id: "ws-last-used" }];

  it("lands where the API says, not in the oldest (first) row", () => {
    assert.equal(landingWorkspaceId(rows, "ws-last-used"), "ws-last-used");
    assert.equal(landingWorkspaceId(rows, "ws-own"), "ws-own");
  });

  it("falls back to the first row only when the API names nothing usable", () => {
    assert.equal(landingWorkspaceId(rows, ""), "ws-oldest");
    assert.equal(landingWorkspaceId(rows, "  "), "ws-oldest");
    assert.equal(landingWorkspaceId(rows, "ws-gone"), "ws-oldest");
    assert.equal(landingWorkspaceId([], "ws-own"), "");
  });
});

describe("platform admin views: All workspaces / Unassigned (D218)", () => {
  const rows = [
    { id: "ws-test", name: "Test", member_count: 2 },
    { id: "ws-venkatesh", name: "Venkatesh Boyapati's workspace", member_count: 1 },
  ];
  const views = [
    { id: ALL_WORKSPACES_ID, name: "All workspaces", kind: "all" },
    { id: UNASSIGNED_WORKSPACE_ID, name: "Unassigned (legacy data)", kind: "unassigned", legacy_jobs: 4577 },
  ];

  it("the API's view ids are the ones the client names", () => {
    assert.equal(ALL_WORKSPACES_ID, "__all__");
    assert.equal(UNASSIGNED_WORKSPACE_ID, "__unassigned__");
  });

  it("lists the views first, the legacy entry with its job count, then every workspace", () => {
    const choices = workspaceChoices(rows, views);
    assert.deepEqual(
      choices.map((c) => [c.id, c.label, c.view]),
      [
        ["__all__", "All workspaces", true],
        ["__unassigned__", "Unassigned (legacy data) · 4577 jobs", true],
        ["ws-test", "Test · 2", false],
        ["ws-venkatesh", "Venkatesh Boyapati's workspace · 1", false],
      ],
    );
    const one = workspaceChoices([], [{ ...views[1], legacy_jobs: 1 }]);
    assert.equal(one[0].label, "Unassigned (legacy data) · 1 job");
    const unknownCount = workspaceChoices([], [{ ...views[1], legacy_jobs: null }]);
    assert.equal(unknownCount[0].label, "Unassigned (legacy data)");
  });

  it("a member without views sees only workspaces", () => {
    assert.deepEqual(workspaceChoices(rows).map((c) => c.id), ["ws-test", "ws-venkatesh"]);
  });

  it("a stored view survives a reload for a platform admin and is dropped for anyone else", () => {
    assert.equal(isKnownWorkspaceChoice(UNASSIGNED_WORKSPACE_ID, rows, views), true);
    assert.equal(isKnownWorkspaceChoice(ALL_WORKSPACES_ID, rows, views), true);
    assert.equal(isKnownWorkspaceChoice(UNASSIGNED_WORKSPACE_ID, rows, []), false);
    assert.equal(isKnownWorkspaceChoice("ws-test", rows, []), true);
    assert.equal(isKnownWorkspaceChoice("", rows, views), false);
    assert.equal(isKnownWorkspaceChoice("  ", rows, views), false);
  });

  it("lands on the API's view, never the oldest workspace", () => {
    assert.equal(landingWorkspaceId(rows, ALL_WORKSPACES_ID, views), ALL_WORKSPACES_ID);
    assert.equal(landingWorkspaceId(rows, UNASSIGNED_WORKSPACE_ID, views), UNASSIGNED_WORKSPACE_ID);
    // A view the caller may not use is not honoured.
    assert.equal(landingWorkspaceId(rows, ALL_WORKSPACES_ID, []), "ws-test");
    // No workspace exists yet: the admin still lands somewhere real.
    assert.equal(landingWorkspaceId([], "", views), ALL_WORKSPACES_ID);
  });

  it("per-workspace screens read no workspace while a view is active", () => {
    assert.equal(concreteWorkspaceId(ALL_WORKSPACES_ID), "");
    assert.equal(concreteWorkspaceId(UNASSIGNED_WORKSPACE_ID), "");
    assert.equal(concreteWorkspaceId(" ws-test "), "ws-test");
    assert.equal(concreteWorkspaceId(""), "");
  });

  it("a view is named on requests like a workspace", () => {
    setActiveWorkspaceId(UNASSIGNED_WORKSPACE_ID);
    assert.equal(getActiveWorkspaceId(), UNASSIGNED_WORKSPACE_ID);
    clearActiveWorkspaceId();
  });
});
