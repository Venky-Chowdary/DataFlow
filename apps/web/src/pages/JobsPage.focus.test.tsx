import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { TransferJob } from "../lib/types.js";
import type { JobHistory, JobHistoryQuery } from "../lib/jobHistory.js";

const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  url: "http://localhost/",
});
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
let mobile = true;
dom.window.matchMedia = (query) => ({
  matches: mobile && query === "(max-width: 720px)",
  media: query,
  onchange: null,
  addListener() {},
  removeListener() {},
  addEventListener() {},
  removeEventListener() {},
  dispatchEvent: () => true,
});
const originalFetch = globalThis.fetch;
/** Full job records the detail endpoint serves; list rows stay slim like the API's. */
const detailRecords = new Map<string, unknown>();
let detailGate: Promise<void> | null = null;
globalThis.fetch = async (input: RequestInfo | URL) => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  const id = url.match(/\/connectors\/jobs\/([^/?]+)$/)?.[1];
  if (id && detailRecords.has(id)) {
    if (detailGate) await detailGate;
    return new Response(JSON.stringify(detailRecords.get(id)), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }
  return new Response("{}", { status: 404 });
};

async function settle(ms = 20) {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, ms));
  });
}

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { JobsPage } = await import("./JobsPage.js");
const { ToastProvider } = await import("../components/Toast.js");
const { ConfirmProvider } = await import("../components/ui/ConfirmDialog.js");

const jobs: TransferJob[] = ["alpha", "beta"].map((name) => ({
  _id: `synthetic-${name}`,
  name: `Synthetic ${name}`,
  source_type: "postgresql",
  source_name: "synthetic_source",
  destination_type: "mysql",
  destination_collection: "synthetic_target",
  status: "completed",
  records_processed: 10,
  created_at: "2026-11-07T00:00:00Z",
}));

let root: ReturnType<typeof createRoot> | undefined;
let host: HTMLDivElement;

async function render(
  list = jobs,
  initialJobId?: string,
  onLoadPage?: (query: JobHistoryQuery) => Promise<JobHistory>,
) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => {
    root!.render(createElement(ToastProvider, null,
      createElement(ConfirmProvider, null, createElement(JobsPage, { jobs: list, initialJobId, onLoadPage })),
    ));
  });
}

function row(index = 0) {
  const element = document.getElementById(`job-item-${jobs[index]._id}`);
  assert.ok(element instanceof dom.window.HTMLTableRowElement, "each job is a grid row");
  return element;
}

/** The job detail drawer (portaled out of the page host), or null when closed. */
function detailDrawer() {
  return document.querySelector<HTMLElement>('[role="dialog"][aria-label="Job detail"]');
}

function back() {
  const element = document.querySelector<HTMLButtonElement>(".df2-jobs-v3-back");
  assert.ok(element);
  return element;
}

async function click(element: HTMLElement) {
  element.focus();
  await act(async () => element.click());
}

/** Select a row and open its detail drawer, as a pointer click does. */
async function openDetail(index = 0) {
  await click(row(index));
  assert.ok(detailDrawer(), "a row click opens the job detail drawer");
}

afterEach(async () => {
  detailRecords.clear();
  detailGate = null;
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  mobile = true;
});

describe("Jobs focus: isolated real components with synthetic jobs (not authenticated browser coverage)", () => {
  it("moves mobile row focus into detail and restores the selected row on All jobs", async () => {
    await render();
    await click(row(1));
    assert.ok(detailDrawer());
    assert.ok(document.activeElement === back(), "mobile entry must focus the visible back control");
    await click(back());
    assert.equal(detailDrawer(), null);
    assert.ok(document.activeElement === row(1), "list return must focus the selected row");
    assert.equal(row(1).getAttribute("aria-selected"), "true");
    await click(row(1));
    assert.ok(document.activeElement === back(), "reopening the same selection must move focus too");
  });

  it("does not steal existing focus when jobs asynchronously auto-select in the mobile list", async () => {
    await render([]);
    const outside = document.createElement("input");
    document.body.appendChild(outside);
    outside.focus();
    await render();
    assert.equal(row().getAttribute("aria-selected"), "true");
    assert.equal(detailDrawer(), null);
    assert.ok(document.activeElement === outside, "automatic selection must not steal list focus");
  });

  it("focuses mobile detail when a deferred deep-linked job arrives", async () => {
    await render([], jobs[1]._id);
    await render(jobs, jobs[1]._id);
    assert.ok(document.activeElement === back(), "deferred deep link must focus the visible back control");
    await click(back());
    assert.ok(document.activeElement === row(1), "deep-linked selection must be restored");
    await render([...jobs], jobs[1]._id);
    assert.ok(document.activeElement === row(1), "polling must not reopen the deep link");
    assert.equal(detailDrawer(), null);
  });

  it("does not steal detail focus on polling and restores the current selection", async () => {
    await render();
    await click(row(1));
    const detailControl = document.querySelector<HTMLButtonElement>(".df2-jobs-v3-detail button:not(.df2-jobs-v3-back)");
    assert.ok(detailControl);
    detailControl.focus();
    await render([...jobs]);
    assert.ok(document.activeElement === detailControl, "polling must not steal detail focus");
    await click(back());
    assert.ok(document.activeElement === row(1), "list return must restore the current selection");
  });

  it("moves desktop focus into the detail drawer and returns it to the row on close", async () => {
    mobile = false;
    await render();
    await click(row(1));
    const drawer = detailDrawer();
    assert.ok(drawer);
    assert.ok(drawer.contains(document.activeElement), "the modal detail drawer takes focus");
    const close = drawer.querySelector<HTMLButtonElement>('button[aria-label="Close"]');
    assert.ok(close);
    await click(close);
    assert.equal(detailDrawer(), null);
    assert.ok(document.activeElement === row(1), "closing the drawer returns focus to the selected row");
    await render(jobs, jobs[0]._id);
    assert.ok(detailDrawer(), "a deep link opens its job's detail");
    assert.equal(row(0).getAttribute("aria-selected"), "true");
  });

  it("restores the current row after asynchronous list replacement changes the selection", async () => {
    await render();
    await click(row(1));
    await render([jobs[0]]);
    assert.equal(row().getAttribute("aria-selected"), "true");
    assert.ok(document.activeElement === back(), "automatic detail selection must preserve visible focus");
    await click(back());
    assert.ok(document.activeElement === row(), "return must restore the current row, not the removed entry row");
  });
});

describe("Jobs evidence: isolated real components with synthetic jobs", () => {
  function evidenceSection(label = "Evidence panels") {
    const section = document.querySelector<HTMLElement>(`.df2-jobs-evidence-list[aria-label="${label}"]`);
    assert.ok(section);
    assert.ok(section.querySelector("ul"), "evidence must retain native list semantics");
    assert.equal(section.querySelector('[role="listitem"]'), null, "buttons must retain button semantics");
    return section;
  }

  function evidenceAction(label: string, section = evidenceSection()) {
    const button = section.querySelector<HTMLButtonElement>(`button[aria-label="Open ${label}"]`);
    assert.ok(button);
    assert.ok(button.querySelector("svg.lucide-panel-right-open"));
    return button;
  }

  /** The evidence drawer opened over the job detail drawer. */
  function topDialog() {
    const dialogs = document.querySelectorAll<HTMLElement>('[role="dialog"]');
    return dialogs.length > 1 ? dialogs.item(dialogs.length - 1) : null;
  }

  async function closeDrawer() {
    const button = topDialog()?.querySelector<HTMLButtonElement>('button[aria-label="Close"]');
    assert.ok(button);
    await click(button);
    assert.equal(topDialog(), null);
    assert.ok(detailDrawer(), "closing evidence leaves the job detail open");
  }

  it("shows absent evidence as unavailable rather than hiding it or implying a valid proof", async () => {
    mobile = false;
    detailRecords.set(jobs[0]._id, jobs[0]);
    await render([jobs[0]]);
    await openDetail(0);
    await settle();
    assert.equal(evidenceSection().querySelectorAll("li").length, 9);
    for (const label of ["Gate-8 reconcile", "Validate / preflight", "Writer & throughput", "Mapping proof", "DDL & stream log", "Pipeline explanation"]) {
      const button = evidenceAction(label);
      assert.equal(button.disabled, true, label);
      const item = button.closest("li")!;
      assert.equal(item.querySelector(".df2-jobs-evidence-list-status")?.textContent, "Not captured");
      assert.equal(item.querySelector(".tone-ok"), null);
      await click(button);
      assert.equal(topDialog(), null);
    }
    await click(evidenceAction("Run metadata"));
    assert.match(topDialog()?.textContent || "", /Run metadata/);
    await closeDrawer();
  });

  function evidenceStatus(label: string) {
    return evidenceAction(label).closest("li")!.querySelector(".df2-jobs-evidence-list-status")?.textContent;
  }

  it("reads saved proof on first open instead of calling the slim list row Not captured", async () => {
    mobile = false;
    let release!: () => void;
    detailGate = new Promise<void>((resolve) => { release = resolve; });
    detailRecords.set(jobs[0]._id, {
      ...jobs[0],
      reconciliation: { passed: true, source_rows: 10, target_rows: 10, verification_method: "batch" },
      preflight: { passed: false, passed_count: 19, total_gates: 24, readiness_score: 79,
        gates: [{ id: "synthetic_gate", status: "warn", message: "Synthetic gate warned" }] },
    });
    await render([jobs[0]]);
    await openDetail(0);
    assert.equal(evidenceStatus("Gate-8 reconcile"), "Loading…");
    assert.equal(evidenceStatus("Validate / preflight"), "Loading…");
    release();
    await settle();
    assert.equal(evidenceAction("Gate-8 reconcile").disabled, false);
    assert.notEqual(evidenceStatus("Gate-8 reconcile"), "Not captured");
    assert.match(evidenceStatus("Validate / preflight") || "", /^19\/24/);
  });

  it("reports an unreadable job record as unavailable, not as proof that was never captured", async () => {
    mobile = false;
    await render([jobs[1]]);
    await openDetail(1);
    await settle(1700);
    assert.equal(evidenceStatus("Gate-8 reconcile"), "Detail unavailable — Refresh");
    assert.equal(evidenceAction("Gate-8 reconcile").disabled, true);
  });

  it("labels the existing mapping-table fallback honestly and keeps proof unavailable in Mapping", async () => {
    mobile = false;
    await render([{ ...jobs[0], transfer_request: { mappings: [{ source: "customer_id", target: "account_id" }] } } as TransferJob]);
    await openDetail(0);
    assert.equal(evidenceSection().querySelector('button[aria-label="Open Mapping proof"]'), null);
    await click(evidenceAction("Column mappings"));
    assert.match(topDialog()?.textContent || "", /Column map.*customer_id.*account_id/s);
    await closeDrawer();
    const mapping = evidenceSection("Mapping evidence");
    assert.equal(evidenceAction("Mapping proof", mapping).disabled, true);
    assert.match(document.querySelector(".df2-jobs-map-overview")?.textContent || "", /Risks \/ reviewNot captured/);
    await click(evidenceAction("Column map table", mapping));
    assert.match(topDialog()?.textContent || "", /Column map.*customer_id.*account_id/s);
    await closeDrawer();
  });

  it("opens recorded evidence without upgrading limited or failed proof to success", async () => {
    mobile = false;
    const job = {
      ...jobs[0],
      records_per_second: 25,
      event_log: ["Synthetic durable event"],
      streams: [{ name: "synthetic_accounts", status: "completed", records_processed: 10 }],
      reconciliation: { passed: true, unproven: true, source_rows: 10, target_rows: 10, message: "Readback not captured" },
      preflight: { passed: false, passed_count: 0, total_gates: 1, readiness_score: 0,
        gates: [{ id: "synthetic_gate", status: "block", message: "Synthetic gate blocked" }] },
      mapping_proof: { dest_mode: "match_existing", summary: { mapped_count: 1, risk_count: 0, review_count: 0 },
        mappings: [{ source: "customer_id", target: "account_id", source_type: "INTEGER", target_type: "INTEGER",
          transform: "none", transform_fidelity: "preserve", confidence: 0.8 }] },
    } as TransferJob;
    await render([job]);
    await openDetail(0);
    for (const label of ["Gate-8 reconcile", "Validate / preflight"]) {
      assert.equal(evidenceAction(label).disabled, false);
      assert.equal(evidenceAction(label).closest("li")!.querySelector(".tone-ok"), null);
    }
    for (const [label, content] of [
      ["Gate-8 reconcile", /Gate-8 reconcile/],
      ["Validate / preflight", /Synthetic gate blocked/],
      ["Writer & throughput", /Writer & throughput/],
      ["Phase timeline", /Phase timeline/],
      ["Event log", /Synthetic durable event/],
      ["Mapping proof", /customer_id.*account_id/s],
    ] as const) {
      await click(evidenceAction(label));
      assert.match(topDialog()?.textContent || "", content, label);
      await closeDrawer();
    }
    const logsTab = document.querySelector<HTMLButtonElement>('#job-tab-log');
    assert.ok(logsTab);
    await click(logsTab);
    await click(evidenceAction("Streams", evidenceSection("Logs")));
    assert.match(topDialog()?.textContent || "", /Stream conservation.*synthetic_accounts/s);
    await closeDrawer();
  });

  it("preserves log and explanation drawer callbacks with separately labeled evidence actions", async () => {
    mobile = false;
    await render([{ ...jobs[0], ddl_log: ["CREATE TABLE synthetic_accounts"], explanation: "Synthetic pipeline summary." } as TransferJob]);
    await openDetail(0);
    await click(evidenceAction("DDL & stream log"));
    assert.match(topDialog()?.textContent || "", /CREATE TABLE synthetic_accounts/);
    await closeDrawer();
    await click(evidenceAction("Pipeline explanation"));
    assert.match(topDialog()?.textContent || "", /Synthetic pipeline summary/);
    await closeDrawer();
    const logsTab = document.querySelector<HTMLButtonElement>('#job-tab-log');
    assert.ok(logsTab);
    await click(logsTab);
    const logs = evidenceSection("Logs");
    assert.equal(logs.querySelectorAll("li").length, 4);
    assert.equal(evidenceAction("Streams", logs).disabled, true);
    await click(evidenceAction("DDL & stream", logs));
    assert.match(topDialog()?.textContent || "", /CREATE TABLE synthetic_accounts/);
    await closeDrawer();
  });
});

after(() => {
  globalThis.fetch = originalFetch;
  dom.window.close();
});

describe("Jobs server-backed list queries", () => {
  it("pages through history and sends status, search, and date filters to the API callback", async () => {
    mobile = false;
    const queries: JobHistoryQuery[] = [];
    const loadPage = async (query: JobHistoryQuery): Promise<JobHistory> => {
      queries.push(query);
      const resultJob = query.search ? { ...jobs[0], name: `Import ${query.search}` } : jobs[0];
      return {
        jobs: [resultJob],
        total: 120,
        filteredTotal: query.search || query.dateFrom || query.status !== "all" ? 1 : 120,
        statusCounts: { completed: 80, failed: 20, running: 20 },
      };
    };
    const settle = async () => act(async () => { await new Promise((resolve) => setTimeout(resolve, 5)); });

    await render([jobs[0]], undefined, loadPage);
    await settle();
    assert.equal(queries[0].limit, 50);
    assert.equal(queries[0].offset, 0);

    const initialRequestCount = queries.length;
    await render([{ ...jobs[0], records_processed: 11 }], undefined, loadPage);
    await settle();
    assert.equal(queries.length, initialRequestCount, "recent-feed updates should not reload the server page");

    const next = Array.from(host.querySelectorAll<HTMLButtonElement>("button"))
      .find((button) => button.textContent?.trim() === "Next");
    assert.ok(next);
    await click(next);
    await settle();
    assert.equal(queries.at(-1)?.offset, 50);

    const dateToggle = Array.from(host.querySelectorAll<HTMLButtonElement>("button"))
      .find((button) => button.textContent?.trim() === "Date range");
    assert.ok(dateToggle);
    await click(dateToggle);
    const dateInputs = Array.from(host.querySelectorAll<HTMLInputElement>('input[type="date"]'));
    assert.equal(dateInputs.length, 2);
    const setInput = async (input: HTMLInputElement, value: string) => {
      const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, "value")?.set;
      setter?.call(input, value);
      await act(async () => input.dispatchEvent(new dom.window.Event("input", { bubbles: true })));
      await act(async () => input.dispatchEvent(new dom.window.Event("change", { bubbles: true })));
    };
    await setInput(dateInputs[0], "2026-11-01");
    await setInput(dateInputs[1], "2026-11-08");
    await settle();

    const search = host.querySelector<HTMLInputElement>('input[type="search"]');
    assert.ok(search);
    await setInput(search, "orders_feed");
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 300)); });
    const latest = queries.at(-1);
    assert.equal(latest?.offset, 0);
    assert.equal(latest?.status, "all");
    assert.equal(latest?.search, "orders_feed");
    assert.equal(latest?.dateFrom, "2026-11-01");
    assert.equal(latest?.dateTo, "2026-11-08");
    assert.match(host.textContent || "", /Showing 1–1 of 1/);
  });
});