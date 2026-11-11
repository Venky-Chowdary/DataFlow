/**
 * Jobs page operations grid, with the real page and synthetic jobs: status
 * pivots and counts, server facets, the detail drawer (open, Escape, focus
 * return), CSV export and the per-user column / density layout.
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
const { jobsGridPrefsKey } = await import("../lib/jobGrid.js");
const { compactCount, needsAttentionMetric, sevenDayWindowStart, successRateMetric } = await import(
  "../components/jobs/useJobsKpis.js"
);

const base = {
  source_type: "postgresql",
  source_name: "synthetic_source",
  destination_type: "mysql",
  destination_database: "wh",
  records_processed: 10,
  created_at: "2026-11-07T00:00:00Z",
};
const jobs: TransferJob[] = [
  { ...base, _id: "grid-alpha", name: "Synthetic alpha", status: "completed", destination_collection: "alpha" },
  { ...base, _id: "grid-beta", name: "Synthetic beta", status: "failed", destination_collection: "beta", source_type: "mysql" },
  { ...base, _id: "grid-gamma", name: "Synthetic gamma", status: "completed", destination_collection: "gamma",
    verification_summary: { passed: false, message: "Synthetic difference" } },
] as TransferJob[];

let root: ReturnType<typeof createRoot> | undefined;
let host: HTMLDivElement;
let queries: JobHistoryQuery[] = [];

const loadPage = async (query: JobHistoryQuery): Promise<JobHistory> => {
  queries.push(query);
  const rows = query.status === "attention"
    ? jobs.filter((j) => j.status === "failed" || j.verification_summary?.passed === false)
    : jobs;
  return {
    jobs: rows,
    total: 30,
    filteredTotal: rows.length,
    statusCounts: { completed: 24, failed: 5, running: 1 },
    attentionCount: 7,
  };
};

async function render(withPaging = true) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => {
    root!.render(createElement(ToastProvider, null,
      createElement(ConfirmProvider, null,
        createElement(JobsPage, { jobs, onLoadPage: withPaging ? loadPage : undefined, onRefresh: () => {} })),
    ));
  });
  await settle();
}

async function settle(ms = 10) {
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, ms)); });
}

function button(label: string | RegExp, scope: ParentNode = document) {
  const match = Array.from(scope.querySelectorAll<HTMLButtonElement>("button"))
    .find((b) => (typeof label === "string" ? b.textContent?.trim() === label : label.test(b.textContent || "")));
  assert.ok(match, `button ${label}`);
  return match;
}

async function click(element: HTMLElement) {
  element.focus();
  await act(async () => element.click());
}

async function key(element: Element, k: string) {
  await act(async () => {
    element.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));
  });
}

function drawer() {
  return document.querySelector<HTMLElement>('[role="dialog"][aria-label="Job detail"]');
}

function row(id: string) {
  const el = document.getElementById(`job-item-${id}`);
  assert.ok(el, id);
  return el;
}

function headers() {
  return Array.from(host.querySelectorAll('[role="grid"] th[aria-sort]')).map((th) => th.textContent);
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  localStorage.clear();
  queries = [];
});

after(() => {
  globalThis.fetch = originalFetch;
  dom.window.close();
});

describe("Jobs grid page", () => {
  it("counts every status pivot, including Needs attention, and sends it to the server", async () => {
    await render();
    const tabs = Array.from(host.querySelectorAll('[role="tablist"][aria-label="Filter jobs by status"] [role="tab"]'))
      .map((t) => t.textContent);
    assert.deepEqual(tabs, ["All (30)", "Running (1)", "Needs attention (7)", "Completed (24)", "Failed (5)"]);
    await click(button("Needs attention (7)"));
    await settle();
    assert.equal(queries.at(-1)?.status, "attention");
    assert.equal(queries.at(-1)?.offset, 0);
    assert.deepEqual(
      Array.from(host.querySelectorAll('[role="grid"] tbody tr')).map((tr) => tr.id),
      ["job-item-grid-beta", "job-item-grid-gamma"],
    );
    const kpis = host.querySelector('[aria-label="Key metrics"]')?.textContent || "";
    assert.match(kpis, /Need attention7/);
    assert.match(kpis, /5 failed · 2 failed verification/);
  });

  it("sends engine, mode and verification facets to the server and clears them", async () => {
    await render();
    const select = host.querySelector<HTMLSelectElement>('select[aria-label="Source engine"]');
    assert.ok(select);
    assert.deepEqual(Array.from(select.options).map((o) => o.value), ["", "mysql", "postgresql"]);
    await act(async () => {
      select.value = "mysql";
      select.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    const verification = host.querySelector<HTMLSelectElement>('select[aria-label="Verification"]')!;
    await act(async () => {
      verification.value = "failed";
      verification.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    await settle();
    assert.equal(queries.at(-1)?.sourceType, "mysql");
    assert.equal(queries.at(-1)?.verification, "failed");
    await click(button("Clear filters"));
    await settle();
    assert.equal(queries.at(-1)?.sourceType, undefined);
    assert.equal(queries.at(-1)?.verification, undefined);
  });

  it("opens the detail drawer from the keyboard, closes it with Escape and returns focus", async () => {
    await render();
    assert.equal(drawer(), null, "the grid opens with no drawer over it");
    row("grid-alpha").focus();
    await key(row("grid-alpha"), "ArrowDown");
    assert.equal(document.activeElement, row("grid-beta"));
    assert.equal(row("grid-beta").getAttribute("aria-selected"), "true");
    assert.equal(drawer(), null, "moving the cursor does not open the detail");
    await key(row("grid-beta"), "Enter");
    const open = drawer();
    assert.ok(open);
    assert.match(open.textContent || "", /Job detail/);
    assert.match(open.textContent || "", /MySQL → MySQL/);
    await act(async () => {
      document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    });
    assert.equal(drawer(), null);
    assert.equal(document.activeElement, row("grid-beta"), "focus returns to the row that opened it");
  });

  it("exports the rows on screen as CSV", async () => {
    let saved: Blob | null = null;
    const originalCreate = URL.createObjectURL;
    const originalRevoke = URL.revokeObjectURL;
    URL.createObjectURL = (blob: Blob) => { saved = blob; return "blob:synthetic"; };
    URL.revokeObjectURL = () => {};
    const anchorClick = dom.window.HTMLAnchorElement.prototype.click;
    let downloaded = "";
    dom.window.HTMLAnchorElement.prototype.click = function click(this: HTMLAnchorElement) { downloaded = this.download; };
    try {
      await render();
      await click(button("Export CSV"));
      assert.ok(saved, "a CSV blob was built");
      assert.match(downloaded, /^datawrap-jobs-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}\.csv$/);
      const text = await (saved as Blob).text();
      const lines = text.trim().split("\r\n");
      assert.equal(lines.length, 4);
      assert.ok(lines[0].startsWith("Job ID,Job,Status,"));
      assert.ok(lines[1].startsWith("grid-alpha,Synthetic alpha,Completed,"));
      assert.match(lines[3], /grid-gamma,Synthetic gamma,Completed,.*,Failed,/);
    } finally {
      URL.createObjectURL = originalCreate;
      URL.revokeObjectURL = originalRevoke;
      dom.window.HTMLAnchorElement.prototype.click = anchorClick;
    }
  });

  it("remembers hidden columns and density per user across visits", async () => {
    localStorage.setItem("df2.session", JSON.stringify({ email: "Operator@Example.com", name: "Op", role: "admin", token: "t", expires_at: Date.now() / 1000 + 3600, signed_in_at: Date.now() }));
    await render();
    assert.ok(headers().includes("Mode"));
    await click(button("Columns"));
    const mode = Array.from(document.querySelectorAll<HTMLButtonElement>('[role="menuitemcheckbox"]'))
      .find((item) => item.textContent?.includes("Mode"));
    assert.ok(mode);
    assert.equal(mode.getAttribute("aria-checked"), "true");
    await click(mode);
    assert.equal(headers().includes("Mode"), false);
    await click(button(/^Density: Compact$/));
    const comfortable = Array.from(document.querySelectorAll<HTMLButtonElement>('[role="menuitemradio"]'))
      .find((item) => item.textContent?.includes("Comfortable"));
    assert.ok(comfortable);
    await click(comfortable);
    assert.ok(host.querySelector(".df2-jobs-grid--comfortable"));

    const key = jobsGridPrefsKey();
    assert.equal(key, "df2.jobsGrid.v1:operator@example.com");
    assert.deepEqual(JSON.parse(localStorage.getItem(key) || "{}"), { density: "comfortable", hidden: ["mode"] });

    await act(async () => root?.unmount());
    root = undefined;
    await render();
    assert.equal(headers().includes("Mode"), false, "the layout survives a remount");
    assert.ok(host.querySelector(".df2-jobs-grid--comfortable"));
  });

  it("sorts the loaded page locally and says so", async () => {
    await render();
    const jobHeader = Array.from(host.querySelectorAll<HTMLButtonElement>("th button")).find((b) => b.textContent === "Job")!;
    await click(jobHeader);
    await click(jobHeader);
    assert.equal(host.querySelector('th[aria-sort="descending"]')?.textContent, "Job");
    assert.deepEqual(
      Array.from(host.querySelectorAll('[role="grid"] tbody tr')).map((tr) => tr.id),
      ["job-item-grid-gamma", "job-item-grid-beta", "job-item-grid-alpha"],
    );
    assert.match(host.textContent || "", /Sorted within this page/);
    assert.equal(queries.every((q) => q.offset === 0), true, "a local sort never re-pages the server");
  });

  it("disables Cancel unless the selected run is live, and Re-run for live runs", async () => {
    await render();
    assert.equal(button("Cancel", host).disabled, true);
    assert.match(button("Cancel", host).title, /Only a running or queued job/);
    assert.equal(button("Re-run", host).disabled, false);
    assert.match(button("Re-run", host).title, /Sync now/);
    row("grid-beta").focus();
    await settle();
    assert.match(button("Re-run", host).title, /Retry from start/);
  });
});

describe("Jobs KPI tiles", () => {
  it("shows a rate only when finished runs exist, and never estimates", () => {
    assert.deepEqual(successRateMetric({ completed: 312, failed: 7 }).value, "97.8%");
    assert.equal(successRateMetric({ completed: 312, failed: 7 }).sub, "312 succeeded · 7 failed");
    assert.equal(successRateMetric({ completed: 5, failed: 0 }).value, "100%");
    assert.equal(successRateMetric({ completed: 0, failed: 0 }).value, "—");
    assert.equal(successRateMetric(null).value, "—");
    assert.equal(needsAttentionMetric(undefined, 3).value, "—");
    assert.equal(needsAttentionMetric(0, 0).sub, "Nothing failed");
    assert.equal(needsAttentionMetric(9, 4).sub, "4 failed · 5 failed verification");
  });

  it("counts seven UTC calendar days and compacts large numbers only", () => {
    assert.equal(sevenDayWindowStart(new Date("2026-11-11T03:00:00Z")), "2026-11-05");
    assert.equal(compactCount(9_999), (9_999).toLocaleString());
    assert.match(compactCount(48_213_004), /^48\.2M$/);
  });
});
