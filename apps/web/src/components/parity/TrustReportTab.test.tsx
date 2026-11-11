/**
 * Trust Report tab (Page A) and sign-off panel (Page C): each state renders the
 * mockup's structure and copy, controls follow permissions, and actions confirm
 * in the page, disable while running and refresh by version.
 */
import assert from "node:assert/strict";
import { after, afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TrustReportTab, TrustReportView, type TrustReportViewProps } from "./TrustReportTab.js";
import { provenReport, signedReport, unexplainedReport } from "./parityFixtures.js";
import type { ParityReport } from "../../lib/parity.js";

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
      {...over}
    />,
  );
}

/** Visible text without tags, for copy assertions. */
function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");
}

describe("Trust Report states (static)", () => {
  it("shows a loading state before the report arrives", () => {
    const html = renderToStaticMarkup(<TrustReportTab jobId="job-31" jobStatus="completed" />);
    assert.match(text(html), /Building the trust report/);
  });

  it("waits for a run that is still in flight instead of asking for a report", () => {
    const html = renderToStaticMarkup(<TrustReportTab jobId="job-31" jobStatus="running" />);
    assert.match(text(html), /The trust report appears when this run finishes/);
  });

  it("proven: one verdict, no differences, sign-off open", () => {
    const t = text(view(provenReport()));
    assert.match(t, /Proven equal/);
    assert.match(t, /100% rows proven equal/);
    assert.match(t, /rows in source 1,204,331/);
    assert.match(t, /No differences\. Every checked row matches\./);
    assert.match(t, /How sure is this\? Counts and checksums cover every row\./);
    assert.doesNotMatch(t, /Proposed fixes/);
    assert.match(t, /Sign as Engineering/);
    assert.doesNotMatch(t, /Sign-off opens when/);
  });

  it("unexplained with fixes: causes, protected values, explanation and fix cards", () => {
    const html = view(unexplainedReport());
    const t = text(html);
    assert.match(t, /4 differences unexplained/);
    assert.match(t, /explained 8 \/ 12/);
    assert.match(t, /≥ 99\.85%/);
    assert.match(t, /4 to resolve/);
    assert.match(t, /Differences in payments/);
    assert.match(t, /Differences in payments Unexplained 4 rows · amount/, "unexplained differences are listed first");
    assert.match(t, /Explained 8 rows · note/);
    assert.match(t, /Protected/);
    assert.match(t, /Example values: protected/);
    assert.doesNotMatch(t, /123-45/); // no protected value ever renders
    assert.match(t, /Source changed during the run\./); // an unknown cause code shows the API label
    assert.match(t, /Source 10\.1049 → destination 10\.10/);
    assert.match(html, /aria-label="8 explained, 4 unexplained"/);
    assert.match(t, /source: template/);
    assert.match(t, /Explain again/);
    assert.match(t, /Approve and re-check/);
    assert.match(t, /Can't be applied: The destination table has no primary key\./);
    assert.equal((t.match(/Approve and re-check/g) || []).length, 1, "a fix that cannot apply has no approve button");
    assert.match(t, /0 of 3 write fixes used/);
    assert.match(t, /4 differences are unexplained\. Sign-off opens at zero\./);
    assert.match(html, /<button[^>]*disabled=""[^>]*>Sign as Engineering/);
    assert.doesNotMatch(html, /window\.confirm/);
  });

  it("hides fix and sign controls without permission and says why", () => {
    const t = text(
      view(unexplainedReport(), {
        canFix: false,
        fixDeniedReason: "You don't have permission to approve or dismiss a fix.",
        canSign: false,
        signDeniedReason: "You don't have permission to explain or sign off a trust report.",
      }),
    );
    assert.doesNotMatch(t, /Approve and re-check/);
    assert.doesNotMatch(t, /Dismiss/);
    assert.doesNotMatch(t, /Explain again/);
    assert.doesNotMatch(t, /Sign as/);
    assert.match(t, /approve or dismiss a fix/);
    assert.match(t, /explain or sign off a trust report/);
  });

  it("signed: approvals with names and times, the signature, and the round history", () => {
    const html = view(signedReport());
    const t = text(html);
    assert.match(t, /Every difference explained/);
    assert.match(t, /Engineering · A\. Rao/);
    assert.match(t, /Data owner · M\. Chen Checked totals\./);
    assert.equal((t.match(/Signed \d/g) || []).length, 3);
    assert.doesNotMatch(t, /Waiting/);
    assert.doesNotMatch(t, /Sign as/);
    assert.match(t, /hmac-sha256:9f3c…a71e/);
    assert.match(html, /title="hmac-sha256:9f3c0d1e/);
    assert.match(t, /Round 1/);
    assert.match(t, /Unexplained 4 → 0/);
    assert.match(t, /Applied/);
    assert.match(t, /Export Markdown/);
  });

  it("states an unverifiable verdict's reason up front", () => {
    const t = text(view(provenReport({ verdict: "unverifiable", verdict_reason: "Only row counts were captured for this run." })));
    assert.match(t, /Can't be verified/);
    assert.match(t, /Only row counts were captured for this run\./);
  });

  it("blocks sign-off while the verdict is unverifiable and says why", () => {
    const report = provenReport({
      verdict: "unverifiable",
      verdict_reason: "Values were not compared for orders (only row counts or nothing was recorded), so the evidence cannot decide.",
      signoff: { ...provenReport().signoff, status: "blocked", blocked_reason: "The evidence cannot decide parity, so there is nothing to sign." },
    });
    const html = view(report);
    const t = text(html);
    assert.match(t, /Blocked/);
    assert.match(t, /The evidence cannot decide parity, so there is nothing to sign\./);
    assert.match(html, /<button[^>]*disabled=""[^>]*>Sign as Engineering/);
  });

  it("links to the existing Gate-8 and trust-score cards rather than redrawing them", () => {
    const t = text(view(provenReport(), { onOpenGate8: noop, onOpenDetail: noop }));
    assert.match(t, /Open Gate-8 proof/);
    assert.match(t, /Open trust score and ledger/);
  });
});

/* ── Interactive: confirm, disable while running, refresh by version ── */

describe("Trust Report actions (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  const calls: Array<{ method: string; url: string; body: string }> = [];
  let getReport: () => ParityReport = () => unexplainedReport();
  let onPost: (url: string) => Promise<Response> = async () => new Response("{}", { status: 500 });
  let failGet = false;
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
      calls.push({ method, url, body: String(init.body ?? "") });
      if (method === "GET" && /\/export\?/.test(url)) return new Response("# Trust report", { status: 200 });
      if (method === "GET" && failGet) {
        return new Response(JSON.stringify({ detail: "Job not found" }), { status: 404, headers: { "Content-Type": "application/json" } });
      }
      if (method === "GET") return new Response(JSON.stringify(getReport()), { status: 200 });
      return onPost(url);
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
    getReport = () => unexplainedReport();
    failGet = false;
  });

  const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

  async function mount() {
    const { createRoot } = await import("react-dom/client");
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    await React.act(async () => {
      root!.render(<TrustReportTab jobId="job-31" jobStatus="completed" />);
      await settle();
    });
    await React.act(async () => {
      await settle();
    });
  }

  function button(label: string | RegExp): HTMLButtonElement {
    const all = [...host!.querySelectorAll<HTMLButtonElement>("button")];
    const found = all.find((b) => (typeof label === "string" ? b.textContent?.trim() === label : label.test(b.textContent || "")));
    assert.ok(found, `no button ${String(label)} in: ${all.map((b) => b.textContent?.trim()).join(" | ")}`);
    return found!;
  }

  async function click(el: HTMLElement) {
    await React.act(async () => {
      el.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
      await settle();
    });
  }

  async function type(el: HTMLTextAreaElement, value: string) {
    const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLTextAreaElement.prototype, "value")!.set!;
    await React.act(async () => {
      setter.call(el, value);
      el.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
    });
  }

  it("asks for confirmation in the card, disables everything while applying, then shows the new version", async () => {
    let release: (r: Response) => void = () => {};
    onPost = () => new Promise<Response>((resolve) => (release = resolve));
    await mount();
    assert.match(host!.textContent || "", /4 differences unexplained/);

    await click(button("Approve and re-check"));
    assert.match(host!.textContent || "", /Apply this fix\?/);
    assert.match(host!.textContent || "", /changes the destination column, then checks the affected rows again/);
    assert.equal(calls.filter((c) => c.method === "POST").length, 0, "nothing is applied before confirming");

    await type(host!.querySelector<HTMLTextAreaElement>(".df2-parity-confirm textarea")!, "Widen per ticket 88");
    await click(button("Confirm and apply"));
    const post = calls.find((c) => c.method === "POST")!;
    assert.match(post.url, /\/parity-report\/fixes\/fix-widen\/approve$/);
    assert.equal(post.body, '{"note":"Widen per ticket 88"}');
    // While the request runs, every action is disabled.
    assert.equal(button("Explain again").disabled, true);
    assert.equal(button(/Export pack/).disabled, true);
    assert.equal(host!.querySelector(".df2-parity")!.getAttribute("aria-busy"), "true");

    const fixed = unexplainedReport({
      version: 4,
      verdict: "explained",
      parity: { ...unexplainedReport().parity, explained: 12, unexplained: 0 },
      fixes: unexplainedReport().fixes.map((f) =>
        f.id === "fix-widen" ? { ...f, status: "applied", approved_by: "A. Rao", applied_at: "2026-11-11T14:10:00Z", result: "4 keys re-checked; 0 differ." } : f,
      ),
      rounds: [{ n: 1, at: "2026-11-11T14:10:00Z", unexplained_before: 4, unexplained_after: 0, fix_id: "fix-widen" }],
    });
    await React.act(async () => {
      release(new Response(JSON.stringify(fixed), { status: 200 }));
      await settle();
    });
    await React.act(async () => settle());
    const t = host!.textContent || "";
    assert.match(t, /Every difference explained/);
    assert.match(t, /version 4/);
    assert.match(t, /4 keys re-checked; 0 differ\./);
    assert.match(t, /Round 1/);
    assert.equal(button("Explain again").disabled, false);
  });

  it("does not roll back to an older version, and re-reads instead", async () => {
    onPost = async () => new Response(JSON.stringify(unexplainedReport({ version: 2, verdict_reason: "STALE" })), { status: 200 });
    getReport = () => unexplainedReport({ version: 3 });
    await mount();
    const getsBefore = calls.filter((c) => c.method === "GET").length;
    getReport = () => unexplainedReport({ version: 5, verdict_reason: "Fresh from the server." });
    await click(button("Explain again"));
    await React.act(async () => settle());
    const t = host!.textContent || "";
    assert.doesNotMatch(t, /STALE/);
    assert.match(t, /version 5/);
    assert.equal(calls.filter((c) => c.method === "GET").length, getsBefore + 1);
  });

  it("explains a conflict and refreshes the report", async () => {
    onPost = async () =>
      new Response(JSON.stringify({ detail: "This fix was already applied." }), { status: 409, headers: { "Content-Type": "application/json" } });
    await mount();
    const getsBefore = calls.filter((c) => c.method === "GET").length;
    await click(button("Dismiss"));
    await click(button("Confirm dismiss"));
    await React.act(async () => settle());
    const alert = [...host!.querySelectorAll('[role="alert"]')].map((n) => n.textContent).join(" ");
    assert.match(alert, /This fix was already applied\. The report has been refreshed/);
    assert.equal(calls.filter((c) => c.method === "GET").length, getsBefore + 1);
  });

  it("exports the Markdown pack through a Blob download", async () => {
    const made: Blob[] = [];
    let downloaded = "";
    Object.assign(dom.window.URL, {
      createObjectURL: (b: Blob) => {
        made.push(b);
        return "blob:parity";
      },
      revokeObjectURL: () => {},
    });
    Object.assign(globalThis, { URL: dom.window.URL });
    dom.window.HTMLAnchorElement.prototype.click = function click(this: HTMLAnchorElement) {
      downloaded = this.download;
    };
    // jsdom has no object URLs; the stubs stay so the delayed revoke still finds one.
    await mount();
    await click(button(/Export pack/));
    await React.act(async () => settle());
    assert.ok(calls.some((c) => /\/parity-report\/export\?format=markdown$/.test(c.url)));
    assert.equal(made.length, 1);
    assert.equal(await made[0].text(), "# Trust report");
    assert.equal(downloaded, "datawrap-trust-report-job-31.md");
  });

  it("offers a retry when the report cannot load", async () => {
    failGet = true;
    await mount();
    const t = host!.textContent || "";
    assert.match(t, /Could not load the trust report/);
    assert.match(t, /not found in this workspace/);
    failGet = false;
    await click(button("Try again"));
    await React.act(async () => settle());
    assert.match(host!.textContent || "", /4 differences unexplained/);
  });
});
