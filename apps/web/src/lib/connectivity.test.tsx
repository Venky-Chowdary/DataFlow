/**
 * UX-17: with the API offline, Connectors kept saying "Test passed", no banner
 * appeared, and Jobs showed the browser's raw "Failed to fetch".
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { after, beforeEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  HTMLElement: dom.window.HTMLElement,
  Event: dom.window.Event,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });

let mode: "down" | "up" = "down";
const realFetch = globalThis.fetch;
globalThis.fetch = (async () => {
  if (mode === "down") throw new TypeError("Failed to fetch");
  return new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } });
}) as typeof fetch;

const { apiFetch } = await import("./api.js");
const { connectivity, resetConnectivityForTests, staleDataLabel, API_UNREACHABLE_MESSAGE, ApiUnreachableError } = await import("./connectivity.js");
const { createElement } = await import("react");
const { renderToStaticMarkup } = await import("react-dom/server");
const { ConnectorCard } = await import("../components/ui/ConnectorCard.js");

after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});
beforeEach(() => resetConnectivityForTests(true));

describe("connectivity", () => {
  it("names a refused request in words, never 'Failed to fetch', and stays a TypeError", async () => {
    mode = "down";
    await assert.rejects(() => apiFetch("/api/v1/connectors/saved"), (err: unknown) => {
      assert.ok(err instanceof ApiUnreachableError);
      assert.ok(err instanceof TypeError, "callers that treat TypeError as client-side still do");
      assert.equal((err as Error).message, API_UNREACHABLE_MESSAGE);
      assert.doesNotMatch((err as Error).message, /Failed to fetch/);
      return true;
    });
  });

  it("goes offline after two refused requests and back online on the next answer", async () => {
    mode = "down";
    await apiFetch("/a").catch(() => undefined);
    assert.equal(connectivity().online, true, "one blip is not an outage");
    await apiFetch("/a").catch(() => undefined);
    assert.equal(connectivity().online, false);
    assert.equal(connectivity().reason, "api-unreachable");
    mode = "up";
    await apiFetch("/a");
    assert.equal(connectivity().online, true);
    assert.ok(connectivity().lastSyncedAt > 0);
  });

  it("follows the browser's own offline signal at once", () => {
    window.dispatchEvent(new dom.window.Event("offline"));
    assert.deepEqual([connectivity().online, connectivity().reason], [false, "browser-offline"]);
    window.dispatchEvent(new dom.window.Event("online"));
    assert.equal(connectivity().online, true);
  });

  it("labels how old the screen is", () => {
    const at = new Date(2026, 10, 14, 9, 5).getTime();
    assert.match(staleDataLabel(at, new Date(2026, 10, 14, 10, 0)), /^Everything on screen is from .*09.05.* and may be out of date\.$/);
    assert.equal(staleDataLabel(0), "Nothing has loaded yet in this session.");
  });
});

describe("connector rows while offline", () => {
  const connector = { id: "c1", name: "Orders PG", type: "postgresql", last_test_ok: true } as never;
  const row = () =>
    renderToStaticMarkup(createElement(ConnectorCard, { connector, compact: true, onTest: () => {}, onSelect: () => {} }));

  it("shows current health online", () => {
    assert.match(row(), /Test passed/);
    assert.match(row(), /df2-connector-row-signal ok/);
  });

  it("shows the last known result, not current health, offline", () => {
    window.dispatchEvent(new dom.window.Event("offline"));
    const html = row();
    assert.match(html, /Last known: passed/);
    assert.doesNotMatch(html, /df2-connector-row-signal ok|df2-health-dot ok/);
    window.dispatchEvent(new dom.window.Event("online"));
  });
});

describe("shell", () => {
  it("shows the offline banner with the stale label and a retry", () => {
    const app = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "DataTransferApp.tsx"), "utf8");
    assert.match(app, /\(!apiOnline \|\| !net\.online\) && \(/);
    assert.match(app, /staleDataLabel\(net\.lastSyncedAt\)/);
    assert.match(app, /Retry now/);
    assert.match(app, /data-stale=\{!net\.online \|\| !apiOnline \? "true" : undefined\}/);
  });
});
