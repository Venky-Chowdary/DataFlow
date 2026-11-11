/**
 * Parity Studio against what the backend really returns: the payloads in
 * realParityPayloads.ts come from an engine run (see the generator named in
 * that file) and are typed with the contract, so tsc checks them. Here the
 * Trust Report, the sign-off panel and Migration health render them, and the
 * backend's refusals (workspace write access, one person one role) read clearly
 * without leaving a control spinning.
 */
import assert from "node:assert/strict";
import { after, afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TrustReportTab, TrustReportView } from "./TrustReportTab.js";
import { SignoffPanel } from "./SignoffPanel.js";
import { MigrationHealthSection, MigrationHealthView } from "./MigrationHealthSection.js";
import { reportInitial, reportResynced, reportSigned, summary } from "./realParityPayloads.js";
import type { ParityReport } from "../../lib/parity.js";

const noop = () => {};

function view(report: ParityReport, over: Partial<React.ComponentProps<typeof TrustReportView>> = {}) {
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
      onDownloadSigned={noop}
      {...over}
    />,
  );
}

function text(html: string): string {
  return html
    .replace(/<[^>]+>/g, " ")
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ");
}

describe("real backend payloads (static)", () => {
  it("initial report: explained, unexplained and out-of-scope differences, fixes and a blocked sign-off", () => {
    const html = view(reportInitial);
    const t = text(html);
    assert.match(t, /4 differences unexplained/);
    assert.match(t, /rows in source 40/);
    assert.match(t, /explained 9 \/ 13/);
    assert.match(t, /Every row was compared: 4 row\(s\) hold a difference no rule explains\./);
    assert.match(t, /Differences in pay Unexplained 3 rows · amount/, "unexplained differences come first");
    assert.match(t, /Numeric scale\. Scale rounded from 4 to 2 decimal place\(s\)\./);
    assert.match(t, /Source 7\.1249 → destination 7\.12/);
    assert.match(t, /1 row · missing in destination/);
    assert.match(t, /Your transform\. trim\(note\) changed 8 cell\(s\)/);
    assert.match(t, /Not written by this run\. The destination already held 1 row\(s\)/);
    assert.match(t, /Can't be applied: sqlite cannot change a column type in place/);
    assert.match(t, /upsert pay for id IN \(7, 8, 9, 13\) — 4 key\(s\)/);
    assert.match(t, /0 of 3 write fixes used/);
    assert.match(t, /Re-read now 4 keys re-read/);
    assert.match(t, /4 row\(s\) still hold an unexplained difference\./, "the API's blocked reason");
    assert.match(html, /<button[^>]*disabled=""[^>]*>Sign as Engineering/);
    assert.doesNotMatch(t, /Download signed pack/);
  });

  it("after the approved re-sync: the round, the failed try, the -2 retry and the dismissed note", () => {
    const t = text(view(reportResynced));
    assert.match(t, /Every difference explained/);
    assert.match(t, /Round 1/);
    assert.match(t, /Unexplained 4 → 0/);
    assert.ok(reportResynced.fixes.some((f) => f.id.endsWith("-2") && f.status === "applied"));
    assert.match(t, /Failed/);
    // The re-sync runs as its own job (review finding 10); its failure is that job's.
    assert.match(t, /Approved by ops@example\.com · [^.]+\. re-sync job resync-child-1 failed: destination locked/);
    assert.match(t, /Note: “first try”/);
    assert.match(t, /Re-synced 4 key\(s\) through job resync-child-2 \(upsert on id; 4 row\(s\) written\)\. Note: “triggers removed”/);
    assert.match(t, /Dismissed by owner@example\.com/);
    assert.equal((t.match(/repair it instead/g) || []).length, 1, "a dismissal's note is shown once");
    assert.match(t, /2 of 3 write fixes used/, "a failed write counts against the limit too");
    assert.doesNotMatch(t, /Sign-off opens|still hold an unexplained/);
    assert.match(t, /Sign as Engineering/);
  });

  it("signed: the three approvals, the signature and the signed pack download", () => {
    const html = renderToStaticMarkup(
      <SignoffPanel report={reportSigned} busy={null} canSign onSign={noop} onExport={noop} onDownloadSigned={noop} />,
    );
    const t = text(html);
    assert.match(t, /Signed/);
    // Each approval names the workspace authority it was signed with (review finding 5).
    assert.match(t, /Engineering · eng@example\.com \(workspace editor\) checked/);
    assert.match(t, /Data owner · owner@example\.com/);
    assert.match(t, /Compliance · comp@example\.com \(workspace admin\) ok/);
    assert.equal((t.match(/Signed \d/g) || []).length, 3);
    assert.doesNotMatch(t, /Sign as/);
    assert.match(t, /hmac-sha256:5e5e…5e5e/);
    assert.match(t, /Download signed pack/);
    assert.match(t, /Export Markdown/);
  });

  it("summary: KPIs, trend and heatmap from the workspace summary", () => {
    const html = renderToStaticMarkup(<MigrationHealthView state="ready" summary={summary} limit={30} onOpenJob={noop} />);
    const t = text(html);
    // The re-sync job is a repair of run 1, not a run (review U4): three runs, two fully proven.
    assert.match(t, /runs fully proven 66\.6%/);
    assert.match(t, /runs with unexplained differences 1/);
    assert.match(t, /sign-offs 1/);
    assert.match(t, /most-drifting table pay/);
    assert.match(html, /aria-label="Parity over the last 3 runs/);
    assert.match(html, /aria-label="1 table over 3 runs\. Unexplained differences: pay in 1 run\."/);
    assert.doesNotMatch(t, /\bdrift\b/, "no run has drift: true (too little route history)");
  });
});

/* ── jsdom: the tab and the section on the real payloads ── */

describe("real backend payloads (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  const calls: Array<{ method: string; url: string; body: string }> = [];
  let getBody: (url: string) => unknown = () => reportInitial;
  let onPost: (url: string) => Response = () => new Response("{}", { status: 500 });
  const saved = { fetch: globalThis.fetch };
  let downloaded = "";
  const made: Blob[] = [];

  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
    });
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
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
    globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const method = String(init.method || "GET");
      const url = String(input);
      calls.push({ method, url, body: String(init.body ?? "") });
      if (method === "GET") return new Response(JSON.stringify(getBody(url)), { status: 200, headers: { "Content-Type": "application/json" } });
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
    made.length = 0;
    downloaded = "";
    getBody = () => reportInitial;
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
    await React.act(async () => settle());
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
    await React.act(async () => settle());
  }

  const refusal = (status: number, detail: string) =>
    new Response(JSON.stringify({ detail }), { status, headers: { "Content-Type": "application/json" } });

  const alerts = () => [...host!.querySelectorAll('[role="alert"]')].map((n) => n.textContent).join(" ");

  it("a viewer's approve gets the API's 403 in words, and the button stops spinning", async () => {
    onPost = () => refusal(403, "Workspace write access required");
    await mount(<TrustReportTab jobId="parity-run-1" jobStatus="completed" />);
    assert.match(host!.textContent || "", /4 differences unexplained/);
    await click(button("Approve and re-check"));
    await click(button("Confirm and apply"));
    const post = calls.find((c) => c.method === "POST")!;
    const resync = reportInitial.fixes.find((f) => f.action === "resync_keys")!;
    assert.match(post.url, new RegExp(`/parity-report/fixes/${resync.id}/approve$`));
    assert.match(alerts(), /Workspace write access required\. Approving or dismissing a fix needs editor or admin access/);
    const confirm = button("Confirm and apply");
    assert.equal(confirm.disabled, false);
    assert.equal(confirm.getAttribute("aria-busy"), null);
    assert.equal(host!.querySelector(".df2-parity")!.getAttribute("aria-busy"), null);
  });

  it("a second role by the same person gets the 409 in words, and the report is read again", async () => {
    getBody = () => reportResynced;
    onPost = () => refusal(409, "One person signs one role; ask another approver.");
    await mount(<TrustReportTab jobId="parity-run-1" jobStatus="completed" />);
    const gets = calls.filter((c) => c.method === "GET").length;
    await click(button("Sign as Engineering"));
    const post = calls.find((c) => c.method === "POST")!;
    assert.match(post.url, /\/parity-report\/signoff$/);
    assert.equal(post.body, `{"role":"engineering","expected_version":${reportResynced.version}}`);
    assert.match(alerts(), /One person signs one role; ask another approver\. The report has been refreshed/);
    assert.equal(calls.filter((c) => c.method === "GET").length, gets + 1);
    assert.equal(button("Sign as Engineering").disabled, false);
  });

  it("a signed report offers the signed pack and downloads it through the shared helper", async () => {
    getBody = (url) => (/format=json/.test(url) ? { content_sha256: "ab", signature: { value: "cd" } } : reportSigned);
    await mount(<TrustReportTab jobId="parity-run-1" jobStatus="completed" />);
    await click(button(/Download signed pack/));
    assert.ok(calls.some((c) => /\/parity-report\/export\?format=json$/.test(c.url)));
    assert.equal(made.length, 1);
    assert.match(await made[0].text(), /content_sha256/);
    assert.equal(downloaded, "datawrap-parity-signed-pack-parity-run-1.json");
  });

  it("Migration health loads the real summary", async () => {
    getBody = () => summary;
    await mount(<MigrationHealthSection limit={30} />);
    const t = host!.textContent || "";
    assert.match(t, /runs fully proven/);
    assert.match(t, /66\.6%/);
    assert.match(t, /pay backfill|pay nightly/);
    assert.ok(calls.some((c) => /\/parity\/summary\?limit=30$/.test(c.url)));
  });
});
