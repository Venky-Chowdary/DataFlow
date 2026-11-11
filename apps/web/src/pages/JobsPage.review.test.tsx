/**
 * Review findings on the Jobs grid, exercised in the app shell the page really
 * runs in (.df2-app > .df2-screen-keep): navigation closes the detail drawer,
 * live rows never yank the open drawer, actions need an explicit selection,
 * and an older API degrades instead of erroring.
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { TransferJob } from "../lib/types.js";
import type { JobHistory, JobHistoryQuery } from "../lib/jobHistory.js";

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
  matches: false, media: query, onchange: null, addListener() {}, removeListener() {},
  addEventListener() {}, removeEventListener() {}, dispatchEvent: () => true,
}) as MediaQueryList;
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => new Response("{}", { status: 404 });

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { JobsPage } = await import("./JobsPage.js");
const { ToastProvider } = await import("../components/Toast.js");
const { ConfirmProvider } = await import("../components/ui/ConfirmDialog.js");

const base = {
  source_type: "postgresql", source_name: "s", destination_type: "mysql", destination_database: "wh",
  records_processed: 10, created_at: "2026-11-07T00:00:00Z", updated_at: "2026-11-07T00:00:00Z",
};
const runA = { ...base, _id: "run-a", name: "Run A", status: "running", destination_collection: "a" } as TransferJob;
const runB = { ...base, _id: "run-b", name: "Run B", status: "running", destination_collection: "b" } as TransferJob;
const failed = { ...base, _id: "run-f", name: "Run F", status: "failed", destination_collection: "f" } as TransferJob;

let root: ReturnType<typeof createRoot> | undefined;
let shell: HTMLDivElement;
let screen: HTMLDivElement;

type PageProps = Parameters<typeof JobsPage>[0];

async function render(props: Partial<PageProps>) {
  if (!root) {
    shell = document.createElement("div");
    shell.className = "df2-app";
    screen = document.createElement("div");
    screen.className = "df2-screen-keep";
    shell.appendChild(screen);
    document.body.appendChild(shell);
    root = createRoot(screen);
  }
  await act(async () => {
    root!.render(createElement(ToastProvider, null, createElement(ConfirmProvider, null,
      createElement(JobsPage, { jobs: [], onRefresh: () => {}, ...props } as PageProps))));
  });
  await settle();
}

async function settle(ms = 20) {
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, ms)); });
}

async function click(element: Element | null | undefined) {
  assert.ok(element instanceof dom.window.HTMLElement, "clickable element");
  element.focus();
  await act(async () => element.click());
  await settle();
}

const drawer = () => document.querySelector<HTMLElement>('[role="dialog"][aria-label="Job detail"]');
const row = (id: string) => document.getElementById(`job-item-${id}`);
const buttonByText = (text: string | RegExp, scope: ParentNode = document) =>
  Array.from(scope.querySelectorAll<HTMLButtonElement>("button,[role=tab]"))
    .find((b) => (typeof text === "string" ? b.textContent?.trim() === text : text.test(b.textContent || "")));

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  localStorage.clear();
});

after(() => {
  globalThis.fetch = originalFetch;
  dom.window.close();
});

describe("finding 1: the drawer belongs to the Jobs screen", () => {
  it("mounts inside the screen wrapper and closes when the screen is hidden", async () => {
    await render({ jobs: [runA, runB] });
    await click(row("run-a"));
    assert.ok(drawer());
    assert.ok(screen.contains(drawer()), "a hidden screen hides its drawer with it");
    await act(async () => { screen.hidden = true; });
    await settle();
    assert.equal(drawer(), null, "switching screens closes the drawer, its overlay and scroll lock");
    assert.equal(document.body.style.overflow || "", "", "no scroll lock left on the next screen");
  });

  it("closes before handing over to Transfer Studio", async () => {
    const opened: unknown[] = [];
    await render({ jobs: [failed], onStartTransfer: (intent) => opened.push(intent) });
    await click(row("run-f"));
    assert.ok(drawer());
    await click(buttonByText("Open Validate in Studio", drawer()!));
    assert.equal(opened.length, 1);
    assert.equal(drawer(), null, "Studio never opens under the Jobs drawer");
  });
});

describe("finding 2: live rows never move the open drawer", () => {
  it("keeps the opened run, refetches the page and does not re-filter rows", async () => {
    const queries: JobHistoryQuery[] = [];
    let serverRows = [runA, runB];
    const loadPage = async (query: JobHistoryQuery): Promise<JobHistory> => {
      queries.push(query);
      return { jobs: serverRows, total: 2, filteredTotal: serverRows.length, statusCounts: { running: serverRows.length }, attentionCount: 0 };
    };
    await render({ jobs: [runA, runB], onLoadPage: loadPage });
    await click(buttonByText(/^Running/));
    await click(row("run-a"));
    const title = () => drawer()?.querySelector(".df2-drawer-title")?.textContent;
    assert.equal(title(), "Run A");
    const before = queries.length;

    // A finishes: the polled feed carries the newer copy.
    await render({ jobs: [{ ...runA, status: "completed", updated_at: "2026-11-07T00:05:00Z" } as TransferJob, runB], onLoadPage: loadPage });
    assert.equal(title(), "Run A", "the drawer keeps the run the operator opened");
    assert.deepEqual(
      Array.from(document.querySelectorAll('[role="grid"] tbody tr')).map((r) => r.id),
      ["job-item-run-a", "job-item-run-b"],
      "the server page is not re-filtered in the browser",
    );
    assert.ok(queries.length > before, "a status change asks the server for a fresh page");

    // The server catches up: the fresh Running page no longer holds A.
    serverRows = [runB];
    await click(buttonByText("Refresh"));
    await settle(30);
    assert.equal(title(), "Run A", "still A, even after it left the page");
    assert.match(document.querySelector(".df2-jobs-pagination-range")?.textContent || "", /1–1 of 1/);
  });
});

describe("finding 9: explicit selection, engine options, older APIs", () => {
  it("offers Re-run / Cancel only for a row the operator picked", async () => {
    await render({ jobs: [runA, failed] });
    const toolbar = () => Array.from(document.querySelectorAll(".df2-jobs-toolbar button")).map((b) => b.textContent?.trim());
    assert.equal(row("run-a")?.getAttribute("aria-selected"), "false", "nothing is selected on arrival");
    assert.equal(toolbar().includes("Cancel"), false);
    assert.equal(toolbar().includes("Re-run"), false);
    await click(row("run-f"));
    await act(async () => {
      document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    });
    await settle();
    assert.equal(row("run-f")?.getAttribute("aria-selected"), "true");
    assert.ok(toolbar().includes("Re-run"));
  });

  it("says where engine options come from", async () => {
    await render({ jobs: [runA, failed] });
    await click(buttonByText("Source: Any"));
    const groups = Array.from(document.querySelectorAll('[role="menu"] [role="group"]')).map((g) => g.getAttribute("aria-label"));
    assert.ok(groups.includes("From loaded jobs"));
  });

  it("hides Needs attention and the server facets when the API predates them", async () => {
    const oldApi = async (): Promise<JobHistory> => ({ jobs: [runA], total: 1, filteredTotal: 1, statusCounts: { running: 1 } });
    await render({ jobs: [runA], onLoadPage: oldApi });
    assert.equal(buttonByText(/^Needs attention/), undefined);
    const chips = Array.from(document.querySelectorAll(".df2-jobs-filter-chips .df2-chip")).map((c) => c.textContent?.trim());
    assert.deepEqual(chips, ["Date: Any"], "facets the server would ignore are not offered");
  });

  it("falls back to All with a clear message when the attention filter is refused", async () => {
    const queries: JobHistoryQuery[] = [];
    const strict = async (query: JobHistoryQuery): Promise<JobHistory> => {
      queries.push(query);
      if (query.status === "attention") throw new Error("Request failed with 422");
      return { jobs: [runA], total: 1, filteredTotal: 1, statusCounts: { running: 1 }, attentionCount: 0 };
    };
    await render({ jobs: [runA], onLoadPage: strict });
    await click(buttonByText(/^Needs attention/));
    await settle(30);
    assert.equal(queries.at(-1)?.status, "all");
    assert.match(document.body.textContent || "", /Needs attention is not available on this server/);
    assert.equal(document.querySelector('[role="alert"].df2-jobs-page-error'), null, "not reported as a failed query");
  });
});
