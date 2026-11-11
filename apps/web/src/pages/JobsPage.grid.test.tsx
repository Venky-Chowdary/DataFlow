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

async function render(withPaging = true, recent: TransferJob[] = jobs) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => {
    root!.render(createElement(ToastProvider, null,
      createElement(ConfirmProvider, null,
        createElement(JobsPage, { jobs: recent, onLoadPage: withPaging ? loadPage : undefined, onRefresh: () => {} })),
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

  function menuItem(label: string) {
    const item = Array.from(document.querySelectorAll<HTMLButtonElement>('[role="menu"] [role^="menuitem"]'))
      .find((entry) => entry.textContent?.replace("✓", "").trim() === label);
    assert.ok(item, `menu item ${label}`);
    return item;
  }

  it("filters with compact chips: value on the chip, × clears one, Clear filters clears all", async () => {
    await render();
    const chips = () => Array.from(host.querySelectorAll(".df2-jobs-filter-chips .df2-chip")).map((c) => c.textContent?.trim());
    assert.deepEqual(chips(), ["Date: Any", "Source: Any", "Destination: Any", "Mode: Any", "Verification: Any"]);
    assert.equal(host.querySelector(".df2-jobs-filter-chips select"), null, "no wide label + select pairs");
    await click(button("Source: Any"));
    assert.deepEqual(
      Array.from(document.querySelectorAll('[role="menu"] [role="menuitemradio"]')).map((i) => i.textContent?.replace("✓", "").trim()),
      ["Any", "MySQL", "PostgreSQL"],
    );
    await click(menuItem("MySQL"));
    await click(button("Verification: Any"));
    await click(menuItem("Failed"));
    await settle();
    assert.equal(queries.at(-1)?.sourceType, "mysql");
    assert.equal(queries.at(-1)?.verification, "failed");
    assert.ok(button("Source: MySQL").classList.contains("active"));
    assert.equal(host.querySelector('[aria-label="Clear Destination filter"]'), null, "only active chips offer ×");
    const clear = host.querySelector<HTMLButtonElement>('button[aria-label="Clear Source filter"]')!;
    assert.ok(clear.classList.contains("df2-close-btn"), "the shared close control, not a typed ×");
    await click(clear);
    await settle();
    assert.equal(queries.at(-1)?.sourceType, undefined);
    assert.equal(queries.at(-1)?.verification, "failed");
    await click(button("Clear filters"));
    await settle();
    assert.equal(queries.at(-1)?.verification, undefined);
    assert.equal(host.querySelector(".df2-jobs-filter-chips .df2-chip.active"), null);
  });

  it("sets a date range from the Date chip, with custom dates behind Custom range", async () => {
    await render();
    await click(button("Date: Any"));
    await click(menuItem("Last 7 days"));
    await settle();
    assert.equal(queries.at(-1)?.dateFrom, sevenDayWindowStart());
    assert.equal(queries.at(-1)?.dateTo, undefined);
    assert.ok(button("Date: Last 7 days").classList.contains("active"));
    await click(button("Date: Last 7 days"));
    await click(menuItem("Custom range…"));
    assert.equal(host.querySelectorAll('input[type="date"]').length, 2);
    await click(host.querySelector<HTMLButtonElement>('[aria-label="Clear Date filter"]')!);
    await settle();
    assert.equal(queries.at(-1)?.dateFrom, undefined);
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
    assert.equal(open.querySelector(".df2-drawer-title")?.textContent, "Synthetic beta", "the drawer is titled by the job");
    assert.equal(open.querySelector(".df2-drawer-title-row .df2-badge")?.textContent, "Failed");
    assert.equal(open.querySelector(".df2-drawer-subtitle")?.textContent, "MySQL → MySQL");
    assert.doesNotMatch(open.querySelector(".df2-drawer-header")?.textContent || "", /Job detail/);
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
    await click(button("View"));
    const groups = Array.from(document.querySelectorAll('[role="menu"] [role="group"]')).map((g) => g.getAttribute("aria-label"));
    assert.deepEqual(groups, ["Columns", "Density", "More"]);
    const mode = menuItem("Mode");
    assert.equal(mode.getAttribute("role"), "menuitemcheckbox");
    assert.equal(mode.getAttribute("aria-checked"), "true");
    await click(mode);
    assert.equal(headers().includes("Mode"), false);
    assert.ok(document.querySelector('[role="menu"]'), "toggling a column keeps the menu open");
    const comfortable = menuItem("Comfortable");
    assert.equal(comfortable.getAttribute("role"), "menuitemradio");
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

  it("keeps server-page rows live from the polled recent feed without re-paging", async () => {
    await render();
    const requests = queries.length;
    const status = () => row("grid-alpha").querySelector(".df2-jobs-grid-status")?.textContent;
    assert.equal(status(), "Completed");
    await render(true, [{ ...jobs[0], status: "failed", updated_at: "2026-11-07T00:10:00" } as TransferJob]);
    assert.equal(status(), "Failed", "the newer recent row replaces the page row");
    await render(true, [{ ...jobs[0], status: "running" } as TransferJob]);
    assert.equal(status(), "Completed", "a recent row with no later write never overrides the page");
    assert.equal(queries.length, requests, "no extra server page read");
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

  it("shows Re-run or Cancel only when the selected run can take it", async () => {
    const live = { ...jobs[0], _id: "grid-live", name: "Synthetic live", status: "running" } as TransferJob;
    jobs.push(live);
    try {
      await render();
      const toolbarButtons = () => Array.from(host.querySelectorAll<HTMLButtonElement>(".df2-jobs-toolbar button"))
        .map((b) => b.textContent?.trim());
      assert.ok(toolbarButtons().includes("Re-run"));
      assert.equal(toolbarButtons().includes("Cancel"), false, "no permanently disabled Cancel");
      assert.match(button("Re-run", host).title, /Sync now/);
      row("grid-beta").focus();
      await settle();
      assert.match(button("Re-run", host).title, /Retry from start/);
      row("grid-live").focus();
      await settle();
      assert.ok(toolbarButtons().includes("Cancel"));
      assert.equal(toolbarButtons().includes("Re-run"), false, "a live run cannot be re-run");
      for (const label of ["Refresh", "Export CSV", "View"]) assert.ok(toolbarButtons().includes(label), label);
      for (const label of ["Columns", "Journals"]) assert.equal(toolbarButtons().includes(label), false, label);
    } finally {
      jobs.pop();
    }
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
