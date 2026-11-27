/**
 * Review UI2 #2: a Trust Report change updates the grid chip without a reload.
 * Run: npx tsx --test apps/web/src/components/jobs/useJobVerdicts.test.tsx
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
let calls = 0;
let fail = false;
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL) => {
  const url = String(input);
  if (url.includes("/parity/verdicts")) {
    calls += 1;
    if (fail) return new Response("{}", { status: 503, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ verdicts: { "job-a": { verdict: "proven", unexplained: 0, method: "full" } } }), { status: 200, headers: { "Content-Type": "application/json" } });
  }
  return new Response("{}", { status: 404 });
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { useJobVerdicts } = await import("./useJobVerdicts.js");
const { publishReportVerdict } = await import("../../lib/reportVerdicts.js");
const { jobVerdict } = await import("../../lib/jobGrid.js");

after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

function Harness({ jobs }: { jobs: never[] }) {
  const rows = useJobVerdicts(jobs);
  return createElement("ul", null, rows.map((r: { _id: string }) => createElement("li", { key: r._id, id: r._id }, jobVerdict(r as never))));
}

const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 30)); });

describe("useJobVerdicts", () => {
  it("follows the Trust Report: a newer report verdict replaces the row's word at once", async () => {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    const jobs = [{ _id: "job-a", status: "completed", updated_at: "t1", verification_summary: { passed: true } }] as never[];
    await act(async () => root.render(createElement(Harness, { jobs })));
    await settle();
    assert.equal(document.getElementById("job-a")?.textContent, "proven");
    await act(async () => publishReportVerdict({ job_id: "job-a", verdict: "unexplained", parity: { unexplained: 3 } }));
    assert.equal(document.getElementById("job-a")?.textContent, "unexplained", "no reload, no re-fetch");
    await act(async () => root.unmount());
  });

  it("never caches a failed read: the next state change asks again", async () => {
    fail = true;
    calls = 0;
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    const jobs = [{ _id: "job-b", status: "completed", updated_at: "t1", verification_summary: { passed: true } }] as never[];
    await act(async () => root.render(createElement(Harness, { jobs })));
    await settle();
    assert.equal(document.getElementById("job-b")?.textContent, "not_loaded");
    fail = false;
    const later = [{ _id: "job-b", status: "completed", updated_at: "t2", verification_summary: { passed: true } }] as never[];
    await act(async () => root.render(createElement(Harness, { jobs: later })));
    await settle();
    assert.ok(calls >= 2);
    await act(async () => root.unmount());
  });
});
