/**
 * Page C: the Reversibility tab. Per-table verdicts with per-column reasons,
 * every lossy column named, a sample fit never shown as proof, the switch and
 * the g24 gate, and the rollback note that never promises an undo.
 */
import assert from "node:assert/strict";
import { after, afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ReversibilityReportView, type ReversibilityReportViewProps } from "./ReversibilityReportView.js";
import { JobReversibilityTab, PlanReversibilityCard } from "./ReversibilityPanels.js";
import { blockedReversibilityFixture, planReversibilityFixture } from "../exit/exitFixtures.js";
import { realPlanReversibility } from "../exit/realExitPayloads.js";
import type { ReversibilityReport } from "../../lib/reversibility.js";

function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
}

function view(report: ReversibilityReport, over: Partial<ReversibilityReportViewProps> = {}) {
  return renderToStaticMarkup(<ReversibilityReportView report={report} onRequireChange={() => {}} canChangeRequirement {...over} />);
}

describe("Reversibility tab (static)", () => {
  it("states the verdict per table, with the mockup's reasons", () => {
    const t = text(view(planReversibilityFixture()));
    assert.match(t, /Reversibility Partly reversible/);
    assert.match(t, /orders Reversible Every mapping is lossless: widened types, renamed columns, no transforms that drop information\./);
    assert.match(t, /customers Not reversible customer_id is hashed \(hash_pii\)/);
    assert.match(t, /payments Partly amount narrows NUMERIC\(12,4\) to NUMERIC\(12,2\); values that do not fit lose data; 4 of 25 observed row\(s\) would lose data\./);
    assert.match(t, /Restore from destination/);
    assert.match(t, /Keep source as fallback/);
  });

  it("names every lossy column with its reason code and reason", () => {
    const t = text(view(planReversibilityFixture()));
    assert.match(t, /Lossy mappings \(4\)/);
    assert.match(t, /Hashed \(hash_pii\) customers\.email email is hashed \(hash_pii\); the original value cannot be recovered from the destination\./);
    assert.match(t, /Masked \(mask_pii\) customers\.phone/);
    assert.match(t, /Narrowed payments\.amount/);
  });

  it("lists each lossy column as a badge at its own width, then the full name (never clipped)", () => {
    const html = view(planReversibilityFixture());
    const items = html.match(/<li><span class="df2-rev-lossy-head"><span class="df2-badge df2-badge-error">[^<]+<\/span><span class="df2-mono df2-rev-lossy-name">[^<]+<\/span><\/span>/g) ?? [];
    assert.equal(items.length, 4);
    assert.match(html, /df2-rev-lossy-name">customers\.customer_id</);
  });

  it("opens the tables that are not reversible, lossy columns first, with the data check in words", () => {
    const html = view(planReversibilityFixture());
    const t = text(html);
    // customers and payments are open; orders (reversible) is closed.
    assert.equal((html.match(/aria-expanded="true"/g) ?? []).length, 2);
    assert.match(t, /Columns \(4\)/);
    assert.match(t, /amount NUMERIC\(12,4\) → NUMERIC\(12,2\) Lossy Narrowed amount narrows .* 4 of 25 observed row\(s\) would lose data\./);
    assert.match(t, /ref TEXT → INTEGER Not checked Cast/);
    assert.ok(t.indexOf("amount NUMERIC") < t.indexOf("payment_id BIGINT"), "lossy before invertible");
  });

  it("never shows a sample fit as proof", () => {
    const report = planReversibilityFixture();
    const orders = { ...report, tables: [{ ...report.tables[0], verdict: "partly_reversible" as const }] };
    const html = view(orders);
    const t = text(html);
    assert.match(t, /status TEXT Fits sample Normalised/);
    assert.match(t, /Fits the 25 observed row\(s\); not proven for rows not sampled\./);
    // Said once: the API's reason carries the data check, so the view does not repeat it.
    assert.equal(t.split("not proven for rows not sampled").length - 1, 1);
    assert.doesNotMatch(html, /df2-badge-live">Fits sample/);
    assert.match(t, /Conditional columns were checked against 25 observed rows from the plan sample\. A sample never proves the full table\./);
  });

  it("the switch off: the gate is not required; on and not reversible: it blocks Execute", () => {
    const off = view(planReversibilityFixture());
    assert.match(off, /role="switch" aria-checked="false"/);
    assert.match(text(off), /Gate G24R · Not required\./);
    const on = view(blockedReversibilityFixture());
    assert.match(on, /role="switch" aria-checked="true"/);
    assert.match(on, /role="alert"/);
    assert.match(text(on), /Gate G24R · Blocks Execute\. This transfer must be reversible, and it is not: customers\.customer_id/);
  });

  it("adds the data check in words when the reason does not state it", () => {
    const report = planReversibilityFixture();
    const col = { ...report.tables[2].columns[2], reason: "ref casts TEXT to INTEGER." };
    const html = view({ ...report, tables: [{ ...report.tables[2], columns: [col] }] });
    assert.match(text(html), /ref casts TEXT to INTEGER\. Not checked: no observed values to test\./);
  });

  it("a job shows what the run required, with no switch", () => {
    const report = { ...planReversibilityFixture(), scope: "job" as const };
    const html = renderToStaticMarkup(<ReversibilityReportView report={report} />);
    assert.doesNotMatch(html, /role="switch"/);
    assert.match(text(html), /Require reversibility: off for this run/);
  });

  it("says why the switch is off for a role that cannot plan", () => {
    const html = view(planReversibilityFixture(), { canChangeRequirement: false, requirementDeniedReason: "Your role cannot plan transfers." });
    assert.match(html, /disabled=""/);
    assert.match(text(html), /Your role cannot plan transfers\./);
  });

  it("the rollback note is a runbook, never an undo", () => {
    const t = text(view(planReversibilityFixture()));
    assert.match(t, /Rollback plan for this cutover The rollback plan for this cutover: orders can be restored from the destination; customers and payments need the source kept as the fallback until sign-off\./);
    assert.match(t, /Datawrap does not undo committed rows/);
    assert.doesNotMatch(t, /one-click|zero data loss|guarantee/i);
  });

  it("renders the report the API really returns", () => {
    const t = text(view(realPlanReversibility));
    assert.match(t, /Partly reversible/);
    assert.match(t, /Dropped payments\.note note is not mapped, so the destination does not hold it\./);
    assert.match(t, /email → email_hash/);
    assert.match(t, /Blocks Execute/);
  });
});

/* ── Interactive: the switch and the containers ─────────────── */

describe("Reversibility tab actions (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  const calls: Array<{ method: string; url: string; body: string }> = [];
  let report: ReversibilityReport = planReversibilityFixture();
  const saved = { fetch: globalThis.fetch };

  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
    });
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
    globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const method = String(init.method || "GET");
      const url = String(input);
      const body = String(init.body ?? "");
      calls.push({ method, url, body });
      if (method === "PATCH") {
        report = blockedReversibilityFixture();
        return new Response(JSON.stringify({ plan: {} }), { status: 200 });
      }
      return new Response(JSON.stringify(report), { status: 200 });
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
    report = planReversibilityFixture();
  });

  const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

  async function mount(node: React.ReactNode) {
    const { createRoot } = await import("react-dom/client");
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    await React.act(async () => {
      root!.render(node);
      await settle();
    });
    await React.act(async () => {
      await settle();
    });
  }

  async function click(el: Element) {
    await React.act(async () => {
      el.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
      await settle();
    });
  }

  it("turning the switch on saves the plan policy, re-reads the report and asks to re-run Validate", async () => {
    const changes: boolean[] = [];
    let require = false;
    const Harness = () => {
      const [value, setValue] = React.useState(false);
      require = value;
      return (
        <PlanReversibilityCard
          planId="plan-1"
          preflightReport={null}
          requireReversibility={value}
          onRequireChange={(next) => {
            changes.push(next);
            setValue(next);
          }}
          onRevalidate={() => {}}
        />
      );
    };
    await mount(<Harness />);
    assert.match(host!.textContent || "", /Partly reversible/);
    const sw = host!.querySelector('button[role="switch"]')!;
    assert.equal(sw.getAttribute("aria-checked"), "false");
    await click(sw);
    await React.act(async () => {
      await settle();
    });
    const patch = calls.find((c) => c.method === "PATCH");
    assert.ok(patch);
    assert.match(patch!.url, /\/transfer\/plans\/plan-1$/);
    assert.equal(patch!.body, '{"policies":{"require_reversibility":true}}');
    assert.deepEqual(changes, [true]);
    assert.equal(require, true);
    const t = host!.textContent || "";
    assert.match(t, /Blocks Execute/);
    assert.match(t, /Re-run Validate to apply the reversibility gate/);
    assert.equal(host!.querySelector('button[role="switch"]')!.getAttribute("aria-checked"), "true");
  });

  it("the job tab reads the run's report", async () => {
    report = { ...planReversibilityFixture(), scope: "job", subject_id: "job-31" };
    await mount(<JobReversibilityTab jobId="job-31" />);
    assert.ok(calls.some((c) => /\/jobs\/job-31\/reversibility$/.test(c.url)));
    assert.match(host!.textContent || "", /Require reversibility: off for this run/);
  });
});
