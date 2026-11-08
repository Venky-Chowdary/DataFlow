import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { TransferJob } from "../lib/types.js";

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
globalThis.fetch = async () => new Response("{}", { status: 404 });

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

async function render(list = jobs, initialJobId?: string) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => {
    root!.render(createElement(ToastProvider, null,
      createElement(ConfirmProvider, null, createElement(JobsPage, { jobs: list, initialJobId })),
    ));
  });
}

function row(index = 0) {
  const element = document.getElementById(`job-item-${jobs[index]._id}`);
  assert.ok(element instanceof dom.window.HTMLButtonElement);
  return element;
}

function back() {
  const element = host.querySelector<HTMLButtonElement>(".df2-jobs-v3-back");
  assert.ok(element);
  return element;
}

async function click(element: HTMLButtonElement) {
  element.focus();
  await act(async () => element.click());
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  mobile = true;
});

describe("Jobs focus: isolated real components with synthetic jobs (not authenticated browser coverage)", () => {
  it("moves mobile row focus into detail and restores the selected row on All jobs", async () => {
    await render();
    await click(row(1));
    assert.ok(host.querySelector(".df2-jobs-v3-layout.has-selection"));
    assert.ok(document.activeElement === back(), "mobile entry must focus the visible back control");
    await click(back());
    assert.ok(host.querySelector(".df2-jobs-v3-layout.is-mobile-list-open"));
    assert.ok(document.activeElement === row(1), "list return must focus the selected row");
    assert.equal(row(1).getAttribute("aria-current"), "true");
    await click(row(1));
    assert.ok(document.activeElement === back(), "reopening the same selection must move focus too");
  });

  it("does not steal existing focus when jobs asynchronously auto-select in the mobile list", async () => {
    await render([]);
    const outside = document.createElement("input");
    document.body.appendChild(outside);
    outside.focus();
    await render();
    assert.equal(row().getAttribute("aria-current"), "true");
    assert.ok(host.querySelector(".df2-jobs-v3-layout.is-mobile-list-open"));
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
    assert.ok(host.querySelector(".df2-jobs-v3-layout.is-mobile-list-open"));
  });

  it("does not steal detail focus on polling and restores the current selection", async () => {
    await render();
    await click(row(1));
    const detailControl = host.querySelector<HTMLButtonElement>(".df2-jobs-v3-detail button:not(.df2-jobs-v3-back)");
    assert.ok(detailControl);
    detailControl.focus();
    await render([...jobs]);
    assert.ok(document.activeElement === detailControl, "polling must not steal detail focus");
    await click(back());
    assert.ok(document.activeElement === row(1), "list return must restore the current selection");
  });

  it("preserves desktop row focus on selection and deferred deep links", async () => {
    mobile = false;
    await render();
    await click(row(1));
    assert.ok(document.activeElement === row(1), "desktop selection must preserve row focus");
    await render(jobs, jobs[0]._id);
    assert.ok(document.activeElement === row(1), "desktop deep link must preserve focus");
  });

  it("restores the current row after asynchronous list replacement changes the selection", async () => {
    await render();
    await click(row(1));
    await render([jobs[0]]);
    assert.equal(row().getAttribute("aria-current"), "true");
    assert.ok(document.activeElement === back(), "automatic detail selection must preserve visible focus");
    await click(back());
    assert.ok(document.activeElement === row(), "return must restore the current row, not the removed entry row");
  });
});

describe("Jobs evidence: isolated real components with synthetic jobs", () => {
  function evidenceSection(label = "Evidence panels") {
    const section = host.querySelector<HTMLElement>(`.df2-jobs-evidence-list[aria-label="${label}"]`);
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

  async function closeDrawer() {
    const button = document.querySelector<HTMLButtonElement>('[role="dialog"] button[aria-label="Close"]');
    assert.ok(button);
    await click(button);
    assert.equal(document.querySelector('[role="dialog"]'), null);
  }

  it("shows absent evidence as unavailable rather than hiding it or implying a valid proof", async () => {
    mobile = false;
    await render([jobs[0]]);
    assert.equal(evidenceSection().querySelectorAll("li").length, 9);
    for (const label of ["Gate-8 reconcile", "Validate / preflight", "Writer & throughput", "Mapping proof", "DDL & stream log", "Pipeline explanation"]) {
      const button = evidenceAction(label);
      assert.equal(button.disabled, true, label);
      const item = button.closest("li")!;
      assert.equal(item.querySelector(".df2-jobs-evidence-list-status")?.textContent, "Not captured");
      assert.equal(item.querySelector(".tone-ok"), null);
      await click(button);
      assert.equal(document.querySelector('[role="dialog"]'), null);
    }
    await click(evidenceAction("Run metadata"));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Run metadata/);
    await closeDrawer();
  });

  it("labels the existing mapping-table fallback honestly and keeps proof unavailable in Mapping", async () => {
    mobile = false;
    await render([{ ...jobs[0], transfer_request: { mappings: [{ source: "customer_id", target: "account_id" }] } } as TransferJob]);
    assert.equal(evidenceSection().querySelector('button[aria-label="Open Mapping proof"]'), null);
    await click(evidenceAction("Column mappings"));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Column map.*customer_id.*account_id/s);
    await closeDrawer();
    const mapping = evidenceSection("Mapping evidence");
    assert.equal(evidenceAction("Mapping proof", mapping).disabled, true);
    assert.match(host.querySelector(".df2-jobs-map-overview")?.textContent || "", /Risks \/ reviewNot captured/);
    await click(evidenceAction("Column map table", mapping));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Column map.*customer_id.*account_id/s);
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
      assert.match(document.querySelector('[role="dialog"]')?.textContent || "", content, label);
      await closeDrawer();
    }
    const logsTab = host.querySelector<HTMLButtonElement>('#job-tab-log');
    assert.ok(logsTab);
    await click(logsTab);
    await click(evidenceAction("Streams", evidenceSection("Logs")));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Stream conservation.*synthetic_accounts/s);
    await closeDrawer();
  });

  it("preserves log and explanation drawer callbacks with separately labeled evidence actions", async () => {
    mobile = false;
    await render([{ ...jobs[0], ddl_log: ["CREATE TABLE synthetic_accounts"], explanation: "Synthetic pipeline summary." } as TransferJob]);
    await click(evidenceAction("DDL & stream log"));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /CREATE TABLE synthetic_accounts/);
    await closeDrawer();
    await click(evidenceAction("Pipeline explanation"));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Synthetic pipeline summary/);
    await closeDrawer();
    const logsTab = host.querySelector<HTMLButtonElement>('#job-tab-log');
    assert.ok(logsTab);
    await click(logsTab);
    const logs = evidenceSection("Logs");
    assert.equal(logs.querySelectorAll("li").length, 4);
    assert.equal(evidenceAction("Streams", logs).disabled, true);
    await click(evidenceAction("DDL & stream", logs));
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /CREATE TABLE synthetic_accounts/);
    await closeDrawer();
  });
});

after(() => {
  globalThis.fetch = originalFetch;
  dom.window.close();
});