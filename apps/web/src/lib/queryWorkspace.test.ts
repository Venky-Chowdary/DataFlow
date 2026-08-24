/**
 * Run: npx --yes tsx --test apps/web/src/lib/queryWorkspace.test.ts
 */
import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";
import {
  DEFAULT_LAYOUT,
  clearHistory,
  closeTab,
  createTab,
  deriveTabTitle,
  duplicateTab,
  filterHistory,
  formatDuration,
  formatRelativeTime,
  loadHistory,
  loadLayout,
  loadTabs,
  pushHistory,
  redactParams,
  retitleTab,
  saveHistory,
  saveLayout,
  saveTabs,
  type QueryHistoryEntry,
} from "./queryWorkspace.js";

/** Minimal localStorage stand-in — the module is browser-targeted. */
class MemoryStorage {
  private map = new Map<string, string>();
  getItem(k: string) {
    return this.map.has(k) ? (this.map.get(k) as string) : null;
  }
  setItem(k: string, v: string) {
    this.map.set(k, String(v));
  }
  removeItem(k: string) {
    this.map.delete(k);
  }
  clear() {
    this.map.clear();
  }
}

const store = new MemoryStorage();
(globalThis as unknown as { localStorage: MemoryStorage }).localStorage = store;

beforeEach(() => store.clear());

describe("redactParams", () => {
  it("drops credential-shaped parameter names", () => {
    const out = redactParams({
      region: "eu-west-1",
      password: "hunter2",
      api_token: "abc",
      SECRET_key: "x",
      customer_id: "42",
    });
    assert.deepEqual(out, { region: "eu-west-1", customer_id: "42" });
  });

  it("keeps ordinary values and tolerates an empty map", () => {
    assert.deepEqual(redactParams({}), {});
    assert.deepEqual(redactParams({ a: "1" }), { a: "1" });
  });
});

describe("tabs", () => {
  it("returns one empty tab when nothing is stored", () => {
    const { tabs, activeId } = loadTabs();
    assert.equal(tabs.length, 1);
    assert.equal(activeId, tabs[0].id);
  });

  it("round-trips tabs and the active id", () => {
    const a = createTab({ title: "A", query: "SELECT 1" });
    const b = createTab({ title: "B", query: "SELECT 2" });
    saveTabs([a, b], b.id);
    const loaded = loadTabs();
    assert.deepEqual(loaded.tabs.map((t) => t.title), ["A", "B"]);
    assert.equal(loaded.activeId, b.id);
  });

  it("never persists credential-shaped parameter values", () => {
    const t = createTab({ params: { token: "sk-live-123", region: "us" } });
    saveTabs([t], t.id);
    assert.doesNotMatch(JSON.stringify(store.getItem("df2.query.tabs.v1")), /sk-live-123/);
    assert.deepEqual(loadTabs().tabs[0].params, { region: "us" });
  });

  it("falls back to the first tab when the stored active id is gone", () => {
    const a = createTab({ title: "A" });
    saveTabs([a], "missing-id");
    assert.equal(loadTabs().activeId, a.id);
  });

  it("survives corrupt stored JSON", () => {
    store.setItem("df2.query.tabs.v1", "{not json");
    assert.equal(loadTabs().tabs.length, 1);
  });
});

describe("closeTab", () => {
  const a = createTab({ title: "A" });
  const b = createTab({ title: "B" });
  const c = createTab({ title: "C" });

  it("activates the right-hand neighbour", () => {
    const r = closeTab([a, b, c], b.id, b.id);
    assert.deepEqual(r.tabs.map((t) => t.title), ["A", "C"]);
    assert.equal(r.activeId, c.id);
  });

  it("activates the left neighbour when closing the last tab", () => {
    const r = closeTab([a, b, c], c.id, c.id);
    assert.equal(r.activeId, b.id);
  });

  it("keeps the active tab when closing a different one", () => {
    const r = closeTab([a, b, c], a.id, c.id);
    assert.equal(r.activeId, a.id);
  });

  it("replaces the last tab with a fresh empty one", () => {
    const r = closeTab([a], a.id, a.id);
    assert.equal(r.tabs.length, 1);
    assert.notEqual(r.tabs[0].id, a.id);
    assert.equal(r.tabs[0].query, "");
  });

  it("is a no-op for an unknown id", () => {
    const r = closeTab([a, b], a.id, "nope");
    assert.equal(r.tabs.length, 2);
    assert.equal(r.activeId, a.id);
  });
});

describe("duplicateTab", () => {
  it("inserts the copy after the original and activates it", () => {
    const a = createTab({ title: "A", query: "SELECT 1" });
    const b = createTab({ title: "B" });
    const r = duplicateTab([a, b], a.id);
    assert.deepEqual(r.tabs.map((t) => t.title), ["A", "A copy", "B"]);
    assert.equal(r.tabs[1].query, "SELECT 1");
    assert.equal(r.activeId, r.tabs[1].id);
    assert.notEqual(r.tabs[1].id, a.id);
  });
});

describe("deriveTabTitle", () => {
  it("uses the first table reference", () => {
    assert.equal(deriveTabTitle("SELECT * FROM users u JOIN orders o ON 1=1"), "users");
  });

  it("unquotes and drops the schema", () => {
    assert.equal(deriveTabTitle('SELECT * FROM "public"."customers"'), "customers");
    assert.equal(deriveTabTitle("SELECT * FROM `app`.`events`"), "events");
  });

  it("uses the first field of a Mongo filter", () => {
    assert.equal(deriveTabTitle('{ "status": "active" }'), "status");
  });

  it("falls back to the leading keyword", () => {
    assert.equal(deriveTabTitle("EXPLAIN ANALYZE something"), "EXPLAIN");
  });

  it("uses the fallback for an empty query", () => {
    assert.equal(deriveTabTitle("   ", "Untitled"), "Untitled");
  });
});

describe("retitleTab", () => {
  it("auto-titles an unpinned tab", () => {
    const t = createTab({ query: "SELECT * FROM orders", title: "Untitled query" });
    assert.equal(retitleTab(t).title, "orders");
  });

  it("leaves a pinned title alone", () => {
    const t = createTab({ query: "SELECT * FROM orders", title: "My report", titlePinned: true });
    assert.equal(retitleTab(t).title, "My report");
  });
});

describe("history", () => {
  const base = { connectorId: "c1", ok: true } as Omit<QueryHistoryEntry, "id" | "at">;

  it("prepends newest first", () => {
    let h = pushHistory([], { ...base, query: "SELECT 1" });
    h = pushHistory(h, { ...base, query: "SELECT 2" });
    assert.deepEqual(h.map((e) => e.query), ["SELECT 2", "SELECT 1"]);
  });

  it("replaces an identical query on the same connector", () => {
    let h = pushHistory([], { ...base, query: "SELECT 1", rowCount: 1 });
    h = pushHistory(h, { ...base, query: "SELECT 2" });
    h = pushHistory(h, { ...base, query: "SELECT 1", rowCount: 9 });
    assert.equal(h.length, 2);
    assert.equal(h[0].query, "SELECT 1");
    assert.equal(h[0].rowCount, 9);
  });

  it("keeps the same query separately per connector", () => {
    let h = pushHistory([], { ...base, query: "SELECT 1" });
    h = pushHistory(h, { ...base, connectorId: "c2", query: "SELECT 1" });
    assert.equal(h.length, 2);
  });

  it("records failures with their error", () => {
    const h = pushHistory([], { ...base, query: "SELECT bad", ok: false, error: "boom" });
    assert.equal(h[0].ok, false);
    assert.equal(h[0].error, "boom");
  });

  it("round-trips through storage and clears", () => {
    saveHistory(pushHistory([], { ...base, query: "SELECT 1" }));
    assert.equal(loadHistory().length, 1);
    clearHistory();
    assert.deepEqual(loadHistory(), []);
  });

  it("caps stored entries", () => {
    let h: QueryHistoryEntry[] = [];
    for (let i = 0; i < 260; i += 1) h = pushHistory(h, { ...base, query: `SELECT ${i}` });
    assert.equal(h.length, 200);
    assert.equal(h[0].query, "SELECT 259");
  });
});

describe("filterHistory", () => {
  const h = [
    { id: "1", query: "SELECT * FROM users", connectorId: "c1", connectorLabel: "Prod PG", ok: true, at: 1 },
    { id: "2", query: "SELECT * FROM orders", connectorId: "c2", connectorLabel: "Staging", ok: true, at: 2 },
  ] as QueryHistoryEntry[];

  it("matches query text case-insensitively", () => {
    assert.deepEqual(filterHistory(h, "ORDERS").map((e) => e.id), ["2"]);
  });

  it("matches the connector label", () => {
    assert.deepEqual(filterHistory(h, "prod").map((e) => e.id), ["1"]);
  });

  it("returns everything for an empty term", () => {
    assert.equal(filterHistory(h, "  ").length, 2);
  });
});

describe("layout", () => {
  it("returns defaults when nothing is stored", () => {
    assert.deepEqual(loadLayout(), DEFAULT_LAYOUT);
  });

  it("round-trips and clamps the editor height", () => {
    saveLayout({ schemaOpen: false, historyOpen: true, editorHeight: 5000 });
    const l = loadLayout();
    assert.equal(l.schemaOpen, false);
    assert.equal(l.historyOpen, true);
    assert.equal(l.editorHeight, 900);
    saveLayout({ ...l, editorHeight: 10 });
    assert.equal(loadLayout().editorHeight, 120);
  });
});

describe("formatDuration", () => {
  it("formats each magnitude", () => {
    assert.equal(formatDuration(0.4), "<1 ms");
    assert.equal(formatDuration(250), "250 ms");
    assert.equal(formatDuration(1500), "1.50 s");
    assert.equal(formatDuration(125_000), "2m 5s");
  });

  it("renders unknown values as a dash", () => {
    assert.equal(formatDuration(undefined), "—");
    assert.equal(formatDuration(-1), "—");
    assert.equal(formatDuration(NaN), "—");
  });
});

describe("formatRelativeTime", () => {
  const now = 1_700_000_000_000;
  it("formats recent buckets", () => {
    assert.equal(formatRelativeTime(now - 1_000, now), "just now");
    assert.equal(formatRelativeTime(now - 5 * 60_000, now), "5m ago");
    assert.equal(formatRelativeTime(now - 3 * 3_600_000, now), "3h ago");
    assert.equal(formatRelativeTime(now - 2 * 86_400_000, now), "2d ago");
  });

  it("falls back to a date for old entries", () => {
    assert.match(formatRelativeTime(now - 400 * 86_400_000, now), /\d/);
  });
});
