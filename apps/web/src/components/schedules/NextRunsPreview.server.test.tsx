/**
 * Re-review F: the scheduler's own next runs replace the browser's.
 * A browser's time-zone data can differ from the server's (Africa/Casablanca:
 * ICU +01, zoneinfo +00), so the times shown are the ones POST
 * /schedules/preview returns; the browser's are only an instant placeholder.
 * Run: npx tsx --test apps/web/src/components/schedules/NextRunsPreview.server.test.tsx
 */
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  sessionStorage: dom.window.sessionStorage,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
let answer: unknown = null;
let status = 200;
const bodies: unknown[] = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  if (String(input).includes("/schedules/preview")) {
    bodies.push(JSON.parse(String(init?.body || "{}")));
    return new Response(JSON.stringify(answer), { status, headers: { "Content-Type": "application/json" } });
  }
  return new Response("{}", { status: 404 });
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { NextRunsPreview, PREVIEW_DEBOUNCE_MS } = await import("./NextRunsPreview.js");

after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

const now = new Date("2026-11-21T10:00:00Z");
async function mount(props: Record<string, unknown>) {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  await act(async () => root.render(createElement(NextRunsPreview, { mode: "cron", preset: "daily", cron: "30 1 * * *", timeZone: "Africa/Casablanca", now, ...props })));
  await act(async () => { await new Promise((r) => setTimeout(r, PREVIEW_DEBOUNCE_MS + 40)); });
  return { host, root };
}
const times = (host: HTMLElement) => [...host.querySelectorAll("time")].map((t) => t.getAttribute("datetime"));

describe("NextRunsPreview asks the scheduler", () => {
  it("an answer that is not a preview (an older API) is treated as no answer", async () => {
    answer = { id: "sched-1", name: "not a preview" };
    const { host, root } = await mount({ timeZone: "UTC" });
    assert.equal(host.querySelectorAll("time").length, 5);
    assert.match(host.textContent || "", /could not be reached/);
    await act(async () => root.unmount());
  });

  it("shows the scheduler's times, not the browser's, when they differ", async () => {
    const server = ["2026-11-22T01:30:00+00:00", "2026-11-23T01:30:00+00:00", "2026-11-24T01:30:00+00:00", "2026-11-25T01:30:00+00:00", "2026-11-26T01:30:00+00:00"];
    answer = { kind: "clock", timezone: "Africa/Casablanca", runs: server };
    const { host, root } = await mount({});
    assert.deepEqual(times(host), server.map((r) => new Date(r).toISOString()));
    assert.deepEqual(bodies.at(-1), { interval: "cron", cron: "30 1 * * *", timezone: "Africa/Casablanca", stored_next_run_at: null, cadence_changed: false, enabled: true, count: 5 });
    assert.doesNotMatch(host.textContent || "", /could not be reached/);
    await act(async () => root.unmount());
  });

  it("says when the times are only the browser's because the scheduler did not answer", async () => {
    status = 503;
    const { host, root } = await mount({ timeZone: "UTC" });
    assert.equal(times(host).length, 5);
    assert.match(host.textContent || "", /the scheduler could not be reached to confirm these times/);
    status = 200;
    await act(async () => root.unmount());
  });

  it("the scheduler decides paused and refused cadences", async () => {
    answer = { kind: "invalid", runs: [], message: "Unknown timezone: Mars/Olympus" };
    const { host, root } = await mount({ timeZone: "UTC", cron: "0 9 * * *" });
    assert.match(host.textContent || "", /Unknown timezone|cannot run/);
    assert.equal(times(host).length, 0);
    await act(async () => root.unmount());
  });
});

describe("NextRunsPreview says where its times come from (review 8)", () => {
  it("labels browser times while the scheduler is asked, then the scheduler's", async () => {
    answer = { kind: "clock", timezone: "UTC", runs: ["2026-11-22T01:30:00+00:00", "2026-11-23T01:30:00+00:00", "2026-11-24T01:30:00+00:00", "2026-11-25T01:30:00+00:00", "2026-11-26T01:30:00+00:00"] };
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    await act(async () => root.render(createElement(NextRunsPreview, { mode: "cron", preset: "daily", cron: "30 1 * * *", timeZone: "UTC", now })));
    assert.equal(host.querySelector(".df2-next-runs-source")?.getAttribute("data-source"), "browser");
    assert.match(host.textContent || "", /Worked out in this browser while the scheduler is asked/);
    await act(async () => { await new Promise((r) => setTimeout(r, PREVIEW_DEBOUNCE_MS + 40)); });
    assert.equal(host.querySelector(".df2-next-runs-source")?.getAttribute("data-source"), "scheduler");
    await act(async () => root.unmount());
  });

  it("an overdue schedule's first run reads Due now, as the beat will run it", async () => {
    answer = { kind: "rolling", every_minutes: 60, from_stored: true, due_now: true, runs: ["2026-11-21T10:00:00+00:00", "2026-11-21T11:00:00+00:00", "2026-11-21T12:00:00+00:00", "2026-11-21T13:00:00+00:00", "2026-11-21T14:00:00+00:00"] };
    const { host, root } = await mount({ mode: "preset", preset: "hourly", timeZone: "UTC", storedNextRunAt: "2026-11-21T07:20:00Z" });
    assert.match(host.querySelector("li")?.textContent || "", /^Due now/);
    await act(async () => root.unmount());
  });
});
