/**
 * Jobs page load states (P0-3, P1-13): a failed first read is an error with
 * Retry, never "No transfer jobs yet"; an unanswered read shows a skeleton and
 * no invented zeros; an answer with zero rows is the empty state.
 *
 * Run: npx tsx --test src/pages/JobsPage.loadState.test.tsx
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { JobHistory, JobHistoryQuery } from "../lib/jobHistory.js";
import type { TransferJob } from "../lib/types.js";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  sessionStorage: dom.window.sessionStorage,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  CustomEvent: dom.window.CustomEvent,
  MutationObserver: dom.window.MutationObserver,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.HTMLElement.prototype.scrollIntoView = () => {};
dom.window.requestAnimationFrame = (callback) => {
  callback(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};
dom.window.matchMedia = (query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener() {},
  removeListener() {},
  addEventListener() {},
  removeEventListener() {},
  dispatchEvent: () => true,
}) as MediaQueryList;
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => new Response("{}", { status: 404 });

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { JobsPage } = await import("./JobsPage.js");
const { ToastProvider } = await import("../components/Toast.js");
const { ConfirmProvider } = await import("../components/ui/ConfirmDialog.js");

const EMPTY: JobHistory = { jobs: [], total: 0, filteredTotal: 0, statusCounts: {}, attentionCount: 0 };
const ONE: JobHistory = {
  jobs: [{
    _id: "load-alpha", name: "Synthetic alpha", status: "running", source_type: "postgresql",
    destination_type: "mysql", records_processed: 10, created_at: "2026-11-07T00:00:00Z",
  } as TransferJob],
  total: 1,
  filteredTotal: 1,
  statusCounts: { running: 1 },
  attentionCount: 0,
};

let root: ReturnType<typeof createRoot> | undefined;
let host: HTMLDivElement;

async function render(onLoadPage: (q: JobHistoryQuery) => Promise<JobHistory>) {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => {
    root!.render(createElement(ToastProvider, null,
      createElement(ConfirmProvider, null,
        createElement(JobsPage, { jobs: [], onLoadPage, onRefresh: () => {} })),
    ));
  });
  await settle();
}

async function settle(ms = 10) {
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, ms)); });
}

const text = () => host.textContent || "";

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  host?.remove();
});

after(() => {
  globalThis.fetch = originalFetch;
});

describe("Jobs list load states", () => {
  it("a 503 on the first read is an error with Retry, not the empty state", async () => {
    let calls = 0;
    let fail = true;
    await render(async () => {
      calls += 1;
      if (fail) throw new Error("Service unavailable. Reference: ref_7c41d2e9");
      return ONE;
    });
    assert.doesNotMatch(text(), /No transfer jobs yet/);
    assert.match(text(), /Couldn't load job history/);
    assert.match(text(), /Service unavailable/);
    assert.ok(host.querySelector('[role="alert"]'), "the failure is announced");
    assert.match(text(), /ref_7c41d2e9/, "the reference is its own chip");
    // Counts the read never returned are not printed as zeros.
    assert.doesNotMatch(text(), /All \(0\)|Running \(0\)/);
    // No KPI tiles of dashes: the error explains the page.
    assert.doesNotMatch(text(), /Running now/);

    fail = false;
    const retry = Array.from(host.querySelectorAll("button")).find((b) => b.textContent?.trim() === "Retry");
    assert.ok(retry, "Retry button");
    await act(async () => retry!.click());
    await settle(20);
    assert.ok(calls >= 2, "Retry reads the page again");
    assert.doesNotMatch(text(), /Couldn't load job history/);
    assert.match(text(), /Synthetic alpha/);
    assert.match(text(), /Running \(1\)/);
  });

  it("while the first read is in flight: a skeleton and no zero counts", async () => {
    let resolve: (h: JobHistory) => void = () => {};
    await render(() => new Promise<JobHistory>((r) => { resolve = r; }));
    assert.ok(host.querySelector('.dt-skeleton-table[aria-busy="true"]'), "skeleton rows");
    assert.doesNotMatch(text(), /No transfer jobs yet/);
    assert.doesNotMatch(text(), /\(0\)/);
    assert.ok(host.querySelector('.dt-skeleton-tiles[aria-busy="true"]'), "KPI tiles load as skeletons");
    assert.doesNotMatch(text(), /Running now/);
    await act(async () => resolve(EMPTY));
    await settle();
    assert.match(text(), /No transfer jobs yet/);
  });

  it("is empty only when the server answered with zero rows", async () => {
    await render(async () => EMPTY);
    assert.match(text(), /No transfer jobs yet/);
    assert.doesNotMatch(text(), /Couldn't load job history/);
  });
});
