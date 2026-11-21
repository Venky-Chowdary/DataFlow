/**
 * Parity at Scale in the Trust Report: verification after the write is its own
 * state (sign-off waits), a keyless table reads "Content match (no key)", and
 * the difference explorer's Columns / Keys / Values views, re-verify and
 * repair SQL behave as the reviewer expects. Repair SQL is only ever read and
 * copied: nothing is sent to run it.
 */
import assert from "node:assert/strict";
import { after, afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TrustReportView, type TrustReportViewProps } from "./TrustReportTab.js";
import { DifferenceExplorer } from "./DifferenceExplorer.js";
import {
  contentMatchReport,
  provenReport,
  verificationStoppedReport,
  verifiedWithDifferencesReport,
  verifyingReport,
} from "./parityFixtures.js";
import {
  checkLabel,
  noDifferencesText,
  signoffBlockedReason,
  signoffShortcut,
  tableParityLabel,
  verdictPresentation,
  type ParityReport,
} from "../../lib/parity.js";
import {
  cellAccessibleName,
  cellText,
  differingPct,
  explorableTables,
  repairSummary,
  verificationProgress,
  verificationRunning,
} from "../../lib/parityScale.js";

const noop = () => {};

function view(report: ParityReport, over: Partial<TrustReportViewProps> = {}) {
  return renderToStaticMarkup(
    <TrustReportView
      report={report}
      busy={null}
      canFix
      canSign
      onRefresh={noop}
      onExplain={noop}
      onApprove={noop}
      onDismiss={noop}
      onSign={noop}
      onExport={noop}
      onReverify={noop}
      canReverify
      {...over}
    />,
  );
}

function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");
}

describe("Parity at Scale words", () => {
  it("verification running is a state, not a verdict, and blocks sign-off", () => {
    const report = verifyingReport();
    assert.deepEqual(verdictPresentation(report), { label: "Verification running", tone: "muted" });
    assert.match(signoffBlockedReason(report), /Verification is still running/);
    assert.equal(signoffShortcut(report).state, "blocked");
    assert.match(noDifferencesText(report), /still comparing every row/);
    assert.equal(tableParityLabel(report.tables[0]), "Checking");
    assert.equal(verificationRunning(report), true);
    assert.equal(verificationRunning(provenReport()), false);
  });

  it("names the in-database checks, keyless first-class", () => {
    assert.equal(checkLabel("multiset_digest"), "Content match (no key)");
    assert.equal(checkLabel("segment_diff"), "Every row, compared in both databases");
    assert.equal(checkLabel("future_check", "From the API"), "From the API");
  });

  it("progress, shares and cells read plainly", () => {
    assert.deepEqual(verificationProgress({ name: "t", status: "running", progress: { items_done: 12, items_queued: 36 } }), {
      done: 12,
      total: 48,
      pct: 25,
    });
    assert.equal(verificationProgress({ name: "t", status: "running", progress: {} }), null);
    assert.equal(differingPct(0.001, 1), "< 0.01%");
    assert.equal(differingPct(0, 0), "0%");
    assert.equal(differingPct(12.345, 9), "12.3%");
    assert.equal(
      cellAccessibleName({ column: "order_amt", source: "10.50", destination: "10.5", differs: true, protected: false }),
      "order_amt, source 10.50, destination 10.5, differs",
    );
    assert.equal(
      cellAccessibleName({ column: "ssn", source: "protected", destination: "protected", differs: true, protected: true }),
      "ssn, protected, value not shown",
    );
    assert.equal(cellText(null, false), "NULL");
    assert.equal(cellText("", false), "(blank)");
    assert.equal(cellText("123-45", true), "Protected");
    assert.equal(repairSummary({ table: "t", available: true, sql: "", statements: { insert: 1, delete: 0, update: 2 } }), "1 insert, 2 updates");
  });

  it("only finished tables can be explored", () => {
    assert.deepEqual(explorableTables(verifyingReport().verification), []);
    assert.deepEqual(explorableTables(verifiedWithDifferencesReport().verification).map((t) => t.name), ["payments"]);
  });
});

describe("Trust Report with verification after the write (static)", () => {
  it("running: says verification is running, the write is complete, and sign-off waits", () => {
    const html = view(verifyingReport());
    const t = text(html);
    assert.match(t, /Verification running/);
    assert.match(t, /Comparing every row of 1 table in both databases\./);
    assert.match(t, /Sign-off and cutover wait until it finishes; the write itself is complete\./);
    assert.match(t, /12 of 48 steps/);
    assert.match(html, /role="progressbar"[^>]*aria-valuenow="25"/);
    assert.match(t, /Why sign-off is blocked/);
    assert.doesNotMatch(t, /Go to sign-off/);
    assert.match(t, /Verifying/);
    assert.match(t, /checking/);
    // Nothing to explore until the comparison ends.
    assert.doesNotMatch(t, /Explore differences/);
    // A user never sees the feature flag or internal names.
    assert.doesNotMatch(html, /DATAWRAP_|parity_scale|segment_diff/);
  });

  it("finished with differences: the explorer is offered with re-verify and repair SQL", () => {
    const t = text(view(verifiedWithDifferencesReport()));
    assert.match(t, /Verification after the write/);
    assert.match(t, /Differences found/);
    assert.match(t, /Explore differences/);
    assert.match(t, /Re-verify this table/);
    assert.match(t, /Copy repair SQL/);
    assert.match(t, /Columns/);
    assert.match(t, /Keys/);
    assert.match(t, /Values/);
  });

  it("without job.run, re-verify is disabled and says why", () => {
    const html = view(verifiedWithDifferencesReport(), { canReverify: false, reverifyDeniedReason: "Ask an editor to re-verify." });
    assert.match(text(html), /Ask an editor to re-verify\./);
    assert.match(html, /<button[^>]*disabled=""[^>]*>.*?Re-verify this table/);
  });

  it("keyless: Content match (no key), and no repair SQL to copy", () => {
    const html = view(contentMatchReport());
    const t = text(html);
    assert.match(t, /Proven equal/);
    assert.match(t, /Content match \(no key\)/);
    assert.match(t, /differences cannot be attributed to a key/);
    assert.match(html, /<button[^>]*disabled=""[^>]*title="A table with no key has no rows a statement could name\."/);
  });

  it("a verification that stopped offers re-verify on the card, with the plain reason (review 10)", () => {
    const html = view(verificationStoppedReport());
    const t = text(html);
    assert.match(t, /Could not finish/);
    assert.match(t, /stopped before it finished/);
    assert.match(t, /Re-verify orders/);
    assert.equal(t.match(/stopped before it finished/g)?.length, 1, "the reason is said once");
    assert.doesNotMatch(t, /Explore differences/); // nothing finished to explore
    const denied = view(verificationStoppedReport(), { canReverify: false, reverifyDeniedReason: "Ask an editor." });
    assert.match(denied, /<button[^>]*disabled=""[^>]*title="Ask an editor\."[^>]*>.*?Re-verify orders/);
  });

  it("every row compared but not proven reads as no difference found, never a sample", () => {
    const report = provenReport({
      verdict: "no_difference_found",
      verdict_reason:
        "Every row of orders was compared and none differs. The tables matched at scan time, but writes during the scan were not ruled out.",
    });
    assert.deepEqual(verdictPresentation(report), { label: "No difference found", tone: "ok" });
    const t = text(view(report));
    assert.doesNotMatch(t, /sample/i);
    assert.match(t, /writes during the scan were not ruled out/);
  });

  it("mounts Track W's AI decisions section as given", () => {
    const t = text(view(provenReport(), { aiDecisions: <section aria-label="AI decisions">Ledger here</section> }));
    assert.match(t, /Ledger here/);
    assert.doesNotMatch(text(view(provenReport())), /Ledger here/);
  });
});

/* ── Interactive explorer ────────────────────────────────────── */

const COLUMNS = {
  table: "payments",
  rows_compared: 3000,
  complete: true,
  columns: [
    { column: "id", compared_by: "in_engine", rows_compared: 3000, rows_differing: 0, differing_pct: 0, status: "match", causes: [], protected: false },
    {
      column: "name", compared_by: "in_engine", rows_compared: 3000, rows_differing: 1, differing_pct: 0.0333, status: "unexplained",
      causes: [{ code: "letter_case", label: "Letter case", rows: 1, explained: false }], protected: false,
    },
    { column: "ssn", compared_by: "streamed_fingerprint", rows_compared: 3000, rows_differing: 0, differing_pct: 0, status: "match", causes: [], protected: true },
  ],
};
const KEYS = {
  table: "payments", keyless: false, key_columns: ["id"], key_protected: false, note: "",
  missing_in_destination: { count: 1, keys: ["20"], keys_shown: 1, at_least: false },
  extra_in_destination: { count: 1, keys: ["99999"], keys_shown: 1, at_least: false },
  duplicate_keys: { count: 0, keys: [], keys_shown: 0, at_least: false },
  changed: { count: 1, keys: ["1500"], keys_shown: 1, at_least: false },
};
const VALUES = {
  table: "payments", keyless: false, offset: 0, limit: 25, total: 2, stored: 2,
  rows: [
    {
      key: ["1500"], in_source: true, in_destination: true,
      cells: [
        { column: "name", source: "n1500", destination: "wrong", differs: true, protected: false },
        // A protected cell never renders a value, even one sent by mistake.
        { column: "ssn", source: "123-45-6789", destination: "protected", differs: true, protected: true },
      ],
    },
    {
      key: ["1501"], in_source: true, in_destination: true,
      cells: [
        { column: "name", source: "n1501", destination: "n1501", differs: false, protected: false },
        { column: "ssn", source: "protected", destination: "protected", differs: true, protected: true },
      ],
    },
  ],
};
const REPAIR = {
  table: "payments", available: true, engine: "postgresql", statements: { insert: 1, delete: 1, update: 1 }, rows_differing: 3,
  sql: "-- Datawrap does not run these. Read them, test them, and run them yourself.\nDELETE FROM \"payments\" WHERE \"id\" = 99999;\n",
  executed_by_datawrap: false,
};

describe("Difference explorer (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  const calls: Array<{ method: string; url: string }> = [];
  const copied: string[] = [];
  let failColumns = false;
  const saved = { fetch: globalThis.fetch };

  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
      requestAnimationFrame: (cb: FrameRequestCallback) => setTimeout(() => cb(0), 0),
      cancelAnimationFrame: (id: number) => clearTimeout(id),
    });
    Object.assign(dom.window, {
      requestAnimationFrame: (cb: FrameRequestCallback) => setTimeout(() => cb(0), 0),
      cancelAnimationFrame: (id: number) => clearTimeout(id),
    });
    Object.defineProperty(dom.window.navigator, "clipboard", {
      value: { writeText: async (t: string) => void copied.push(t) },
      configurable: true,
    });
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
    globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const method = String(init.method || "GET");
      const url = String(input);
      calls.push({ method, url });
      const json = (body: unknown, status = 200) =>
        new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
      if (method === "POST") return json({ state: "running", tables: [{ name: "payments", status: "running", progress: {} }] });
      if (url.includes("/columns")) return failColumns ? json({ detail: "Server error" }, 500) : json(COLUMNS);
      if (url.includes("/keys")) return json(KEYS);
      if (url.includes("/values")) return json(VALUES);
      if (url.includes("/repair-sql")) return json(REPAIR);
      return json({}, 404);
    }) as typeof fetch;
  });
  after(() => {
    globalThis.fetch = saved.fetch;
  });
  afterEach(async () => {
    if (root) await React.act(async () => root!.unmount());
    root = null;
    host?.remove();
    host = null;
    calls.length = 0;
    copied.length = 0;
    failColumns = false;
  });

  const settle = () => new Promise((resolve) => setTimeout(resolve, 0));
  const reverified: string[] = [];

  async function mount() {
    const { createRoot } = await import("react-dom/client");
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    const tables = verifiedWithDifferencesReport().verification!.tables;
    await React.act(async () => {
      root!.render(
        <DifferenceExplorer jobId="job-31" tables={tables} canReverify onReverify={(t) => reverified.push(t)} />,
      );
      await settle();
    });
    await React.act(async () => settle());
  }

  function button(label: string | RegExp, scope: ParentNode = document.body): HTMLButtonElement {
    const all = [...scope.querySelectorAll<HTMLButtonElement>("button")];
    const found = all.find((b) => (typeof label === "string" ? b.textContent?.trim() === label : label.test(b.textContent || "")));
    assert.ok(found, `no button ${String(label)} in: ${all.map((b) => b.textContent?.trim()).join(" | ")}`);
    return found!;
  }

  async function click(el: HTMLElement) {
    await React.act(async () => {
      el.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
      await settle();
    });
    await React.act(async () => settle());
  }

  async function key(el: HTMLElement, k: string) {
    await React.act(async () => {
      el.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: k, bubbles: true }));
      await settle();
    });
    await React.act(async () => settle());
  }

  it("Columns: each column with how it was compared, rows differing and the cause", async () => {
    await mount();
    const t = host!.textContent || "";
    assert.match(t, /name/);
    assert.match(t, /In both databases/);
    assert.match(t, /Read and compared/);
    assert.match(t, /Unexplained/);
    assert.match(t, /Letter case \(1 row\)/);
    assert.match(t, /Protected/);
    assert.ok(calls.every((c) => c.method === "GET"));
  });

  it("tabs follow the arrow keys and load Keys, then Values as a grid", async () => {
    await mount();
    const tabs = [...host!.querySelectorAll<HTMLButtonElement>('[role="tab"]')];
    assert.deepEqual(tabs.map((b) => b.textContent), ["Columns", "Keys", "Values"]);
    assert.equal(tabs[0].getAttribute("aria-selected"), "true");
    assert.equal(tabs[1].tabIndex, -1);
    tabs[0].focus();
    await key(tabs[0], "ArrowRight");
    assert.equal(host!.querySelector('[role="tab"][aria-selected="true"]')!.textContent, "Keys");
    assert.equal(document.activeElement?.textContent, "Keys");
    const t = host!.textContent || "";
    assert.match(t, /Missing in destination 1/);
    assert.match(t, /20/);
    assert.match(t, /Extra in destination 1/);
    assert.match(t, /99999/);
    assert.match(t, /Duplicate keys 0/);

    await key(document.activeElement as HTMLElement, "End");
    assert.equal(host!.querySelector('[role="tab"][aria-selected="true"]')!.textContent, "Values");
    const grid = host!.querySelector('[role="grid"]')!;
    assert.ok(grid, "values render as a grid");
    const cells = [...grid.querySelectorAll('[role="gridcell"]')].map((c) => c.getAttribute("aria-label"));
    assert.ok(cells.includes("name, source n1500, destination wrong, differs"));
    assert.ok(cells.includes("ssn, protected, value not shown"));
    // Protected values never render.
    assert.doesNotMatch(host!.innerHTML, /123-45/);
    assert.match(grid.textContent || "", /Protected/);
    // Roving focus: one cell in the tab order, arrows move it.
    const first = grid.querySelector<HTMLElement>('[data-cell="0-0"]')!;
    assert.equal(first.tabIndex, 0);
    first.focus();
    await key(first, "ArrowRight");
    assert.equal((document.activeElement as HTMLElement).dataset.cell, "0-1");
    await key(document.activeElement as HTMLElement, "ArrowDown");
    assert.equal((document.activeElement as HTMLElement).dataset.cell, "1-1");
    await key(document.activeElement as HTMLElement, "Home");
    assert.equal((document.activeElement as HTMLElement).dataset.cell, "1-0");
  });

  it("a failed view says so and retries", async () => {
    failColumns = true;
    await mount();
    assert.match(host!.textContent || "", /server reported an error/i);
    failColumns = false;
    await click(button("Try again", host!));
    assert.match(host!.textContent || "", /In both databases/);
  });

  it("repair SQL is previewed and copied, never sent to run", async () => {
    await mount();
    await click(button("Copy repair SQL", host!));
    const dialog = document.body.querySelector(".df2-pscale-repair")!;
    assert.ok(dialog, "the preview opens first");
    assert.match(dialog.textContent || "", /Datawrap does not run these/);
    assert.match(dialog.textContent || "", /1 insert, 1 delete, 1 update/);
    assert.match(dialog.textContent || "", /DELETE FROM "payments"/);
    await click(button("Copy statements"));
    assert.deepEqual(copied, [REPAIR.sql]);
    assert.match(document.body.textContent || "", /Copied/);
    assert.ok(calls.every((c) => c.method === "GET"), "nothing but reads: the SQL is never run by Datawrap");
  });

  it("re-verify asks the page to compare the table again", async () => {
    reverified.length = 0;
    await mount();
    await click(button("Re-verify this table", host!));
    assert.deepEqual(reverified, ["payments"]);
  });
});
