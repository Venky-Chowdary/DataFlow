/**
 * JobsGrid in jsdom: grid semantics, keyboard, sort headers, states, row menu,
 * stacked phone layout and the status live region.
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { TransferJob } from "../../lib/types.js";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  MouseEvent: dom.window.MouseEvent,
  KeyboardEvent: dom.window.KeyboardEvent,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
let narrow = false;
dom.window.matchMedia = (query: string) => ({
  matches: narrow && query === "(max-width: 720px)",
  media: query,
  onchange: null,
  addListener() {},
  removeListener() {},
  addEventListener() {},
  removeEventListener() {},
  dispatchEvent: () => true,
}) as MediaQueryList;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { JobsGrid } = await import("./JobsGrid.js");
const { DEFAULT_JOBS_GRID_SORT, JOBS_GRID_COLUMNS } = await import("../../lib/jobGrid.js");

const jobs: TransferJob[] = [
  { _id: "job-live", name: "orders_daily", status: "running", progress_pct: 40, total_rows: 1000, records_processed: 400 },
  { _id: "job-ok", name: "customers_full", status: "completed", records_processed: 10,
    verification_summary: { passed: false, message: "2 rows differ" } },
  { _id: "job-bad", name: "events_stream", status: "failed", records_processed: 0 },
].map((j) => ({
  source_type: "mysql",
  source_name: "src",
  destination_type: "postgresql",
  destination_database: "wh",
  destination_collection: "t",
  created_at: "2026-11-10T10:00:00",
  ...j,
})) as TransferJob[];

let root: ReturnType<typeof createRoot> | undefined;
let host: HTMLDivElement;
const calls: { select: string[]; open: string[]; sort: unknown[]; menu: string[] } = { select: [], open: [], sort: [], menu: [] };

type Props = Parameters<typeof JobsGrid>[0];
async function render(patch: Partial<Props> = {}) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  const props: Props = {
    jobs,
    displayName: (job) => job.name || job._id,
    selectedId: "job-ok",
    columns: JOBS_GRID_COLUMNS.map((c) => c.id),
    density: "compact",
    sort: DEFAULT_JOBS_GRID_SORT,
    onSortChange: (sort) => calls.sort.push(sort),
    onSelect: (id) => calls.select.push(id),
    onOpen: (id) => calls.open.push(id),
    rowActions: {
      menuItems: (job) => [
        { id: "open", label: "Open details", onSelect: () => calls.menu.push(`open:${job._id}`) },
        { id: "copy", label: "Copy job ID", onSelect: () => calls.menu.push(`copy:${job._id}`) },
      ],
    },
    emptyTitle: "No jobs in this view",
    emptyDescription: "Nothing here.",
    ...patch,
  };
  await act(async () => root!.render(createElement(JobsGrid, props)));
}

function rowEl(id: string) {
  const el = document.getElementById(`job-item-${id}`);
  assert.ok(el, id);
  return el;
}

async function key(el: Element, k: string) {
  await act(async () => {
    el.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));
  });
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  narrow = false;
  calls.select = []; calls.open = []; calls.sort = []; calls.menu = [];
});

after(() => dom.window.close());

describe("JobsGrid", () => {
  it("renders an ARIA grid with sortable headers and badge-and-text cells", async () => {
    await render();
    const grid = host.querySelector('[role="grid"]');
    assert.ok(grid);
    assert.equal(grid.getAttribute("aria-label"), "Transfer jobs");
    const headers = Array.from(grid.querySelectorAll("th[aria-sort]"));
    assert.deepEqual(headers.map((h) => h.textContent), ["Status", "Job", "Route", "Mode", "Rows", "Verification", "Duration", "Started"]);
    assert.equal(grid.querySelector('th[aria-sort="descending"]')?.textContent, "Started");
    assert.equal(rowEl("job-ok").getAttribute("aria-selected"), "true");
    assert.equal(rowEl("job-live").getAttribute("aria-selected"), "false");
    // Roving focus: only the selected row is in the tab order.
    assert.equal(rowEl("job-ok").tabIndex, 0);
    assert.equal(rowEl("job-live").tabIndex, -1);
    const status = rowEl("job-bad").querySelector(".df2-jobs-grid-status");
    assert.equal(status?.textContent, "Failed", "status is text, not colour alone");
    const verify = rowEl("job-ok").querySelector(".df2-jobs-grid-col-verification .df2-badge");
    assert.equal(verify?.textContent, "Failed", "failed verification shows its plain label");
    assert.match(rowEl("job-ok").querySelector(".df2-jobs-grid-verify")?.getAttribute("title") || "", /2 rows differ/);
    const bar = rowEl("job-live").querySelector('[role="progressbar"]');
    assert.equal(bar?.getAttribute("aria-valuenow"), "40");
    assert.equal(rowEl("job-live").querySelector(".df2-jobs-grid-col-rows .df2-jobs-grid-num")?.textContent, "400");
    assert.match(rowEl("job-live").querySelector(".df2-jobs-grid-col-rows [title]")?.getAttribute("title") || "", /400 of 1,000 rows/);
  });

  it("keeps cells plain: short status, neutral counts, one verification badge", async () => {
    await render({
      jobs: [{
        ...jobs[1],
        _id: "job-q",
        status: "completed_with_quarantine",
        rejected_rows: 37,
        records_processed: 48210,
        verification_summary: { passed: false, message: "2 rows differ" },
        row_accounting: { dest_count: 48173, writer_ack: 48210, balanced: false, conservation_kind: "overwrite", rows_written_source: "gate8_dest_readback" },
      } as TransferJob],
      selectedId: "job-q",
    });
    const status = rowEl("job-q").querySelector(".df2-jobs-grid-status")!;
    assert.equal(status.textContent, "Quarantined");
    assert.equal(status.getAttribute("title"), "Completed with quarantine");
    assert.equal(status.getAttribute("aria-label"), "Completed with quarantine");
    const rows = rowEl("job-q").querySelector(".df2-jobs-grid-col-rows")!;
    assert.equal(rows.querySelector(".df2-jobs-grid-num")?.textContent, "48,173");
    assert.equal(rows.querySelector(".is-danger, .is-warn"), null, "row counts carry no verdict colour");
    assert.match(rows.querySelector("[title]")?.getAttribute("title") || "", /at dest/);
    assert.equal(rowEl("job-q").querySelectorAll(".df2-jobs-grid-col-verification .df2-badge").length, 1);
  });

  it("moves the cursor with arrows / Home / End and opens with Enter or Space", async () => {
    await render();
    await key(rowEl("job-ok"), "ArrowDown");
    assert.deepEqual(calls.select, ["job-bad"]);
    assert.equal(document.activeElement, rowEl("job-bad"));
    await key(rowEl("job-ok"), "ArrowUp");
    await key(rowEl("job-ok"), "Home");
    await key(rowEl("job-ok"), "End");
    // Home lands on the row that already has focus, so it selects nothing new.
    assert.deepEqual(calls.select.slice(1), ["job-live", "job-bad"]);
    assert.equal(document.activeElement, rowEl("job-bad"));
    await key(rowEl("job-ok"), "Enter");
    await key(rowEl("job-bad"), " ");
    assert.deepEqual(calls.open, ["job-ok", "job-bad"]);
    await act(async () => rowEl("job-live").click());
    assert.deepEqual(calls.open.at(-1), "job-live");
  });

  it("sorts from the header and hides deselected columns", async () => {
    await render({ columns: ["status", "job", "rows"], sort: { column: "rows", dir: "asc" } });
    const headers = Array.from(host.querySelectorAll("th[aria-sort]"));
    assert.deepEqual(headers.map((h) => h.textContent), ["Status", "Job", "Rows"]);
    assert.equal(host.querySelector('th[aria-sort="ascending"]')?.textContent, "Rows");
    const jobHeader = headers[1].querySelector("button")!;
    await act(async () => jobHeader.click());
    assert.deepEqual(calls.sort, [{ column: "job", dir: "asc" }]);
  });

  it("shows empty, loading and error states", async () => {
    await render({ jobs: [] });
    assert.match(host.textContent || "", /No jobs in this view/);
    assert.equal(host.querySelector('[role="grid"]'), null);

    await render({ jobs: [], loading: true });
    assert.match(host.querySelector('[role="status"].df2-jobs-page-loading')?.textContent || "", /Updating job results/);
    assert.doesNotMatch(host.textContent || "", /No jobs in this view/, "no empty claim while loading");

    let retried = 0;
    await render({ error: "timeout", onRetry: () => { retried += 1; } });
    assert.match(host.querySelector('[role="alert"]')?.textContent || "", /Could not load this job query: timeout/);
    assert.match(host.textContent || "", /Job results unavailable/);
    const retry = Array.from(host.querySelectorAll("button")).find((b) => b.textContent === "Retry")!;
    await act(async () => retry.click());
    assert.equal(retried, 1);
  });

  it("opens the row menu without opening the row", async () => {
    await render();
    const trigger = rowEl("job-bad").querySelector<HTMLButtonElement>('button[aria-haspopup="menu"]')!;
    assert.equal(trigger.getAttribute("aria-label"), "More actions for events_stream");
    assert.ok(trigger.querySelector("svg.lucide-ellipsis"), "the row action is the horizontal ellipsis");
    await act(async () => trigger.click());
    assert.deepEqual(calls.open, [], "the menu button is not a row click");
    const menu = document.querySelector('[role="menu"]');
    assert.ok(menu);
    const items = Array.from(menu.querySelectorAll('[role="menuitem"]'));
    assert.deepEqual(items.map((i) => i.textContent), ["Open details", "Copy job ID"]);
    await act(async () => {
      host.querySelector(".df2-jobs-grid-scroll")!.dispatchEvent(new dom.window.Event("scroll"));
    });
    assert.ok(document.querySelector('[role="menu"]'), "scrolling the grid moves the menu with its row, not closes it");
    await key(menu, "ArrowDown");
    assert.equal(document.activeElement, items[1]);
    await act(async () => (items[1] as HTMLButtonElement).click());
    assert.deepEqual(calls.menu, ["copy:job-bad"]);
    assert.equal(document.querySelector('[role="menu"]'), null, "choosing an item closes the menu");
  });

  it("switches to stacked rows on a phone and keeps density as a class", async () => {
    await render({ density: "comfortable" });
    const section = host.querySelector(".df2-jobs-grid")!;
    assert.ok(section.classList.contains("df2-jobs-grid--comfortable"));
    assert.equal(section.classList.contains("is-stacked"), false);
    await act(async () => root?.unmount());
    root = undefined;
    narrow = true;
    await render();
    assert.ok(host.querySelector(".df2-jobs-grid.is-stacked.df2-jobs-grid--compact"));
    assert.equal(rowEl("job-ok").querySelector("td[data-label='Verification']")?.getAttribute("data-label"), "Verification");
  });

  it("reports the server's row count and each row's position in it", async () => {
    await render({ totalRows: 60, rowOffset: 50 });
    const grid = host.querySelector('[role="grid"]')!;
    assert.equal(grid.getAttribute("aria-rowcount"), "61", "60 jobs plus the header row");
    assert.equal(rowEl("job-live").getAttribute("aria-rowindex"), "52", "page 2 starts at job 51 (row 52)");
    assert.equal(rowEl("job-bad").getAttribute("aria-rowindex"), "54");
  });

  it("Tab out of a row menu closes it and continues from its trigger", async () => {
    await render();
    const trigger = rowEl("job-bad").querySelector<HTMLButtonElement>('button[aria-haspopup="menu"]')!;
    await act(async () => trigger.click());
    const menu = document.querySelector('[role="menu"]')!;
    const tab = new dom.window.KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true });
    await act(async () => { menu.dispatchEvent(tab); });
    assert.equal(document.querySelector('[role="menu"]'), null);
    assert.equal(document.activeElement, trigger, "focus is back in the row, not lost at the end of <body>");
    assert.equal(tab.defaultPrevented, false, "the browser's Tab then moves on from the trigger");
  });

  it("announces a status change on refresh in a polite live region", async () => {
    await render();
    const live = host.querySelector('[aria-live="polite"][role="status"]');
    assert.ok(live);
    assert.equal(live.textContent, "");
    await render({ jobs: jobs.map((j) => (j._id === "job-live" ? { ...j, status: "completed" } : j)) });
    assert.equal(live.textContent, "orders_daily: Completed.");
  });
});
