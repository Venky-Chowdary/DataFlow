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
import { failedRunReport, provenApiWordingReport, provenReport, signedReport, unexplainedReport } from "./parityFixtures.js";
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
    // A retrying run has not finished either (re-review 3, M4): no report is read for it.
    const retrying = renderToStaticMarkup(<TrustReportTab jobId="job-31" jobStatus="retrying" />);
    assert.match(text(retrying), /The trust report appears when this run finishes/);
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
    assert.match(t, /Download report \(\.md\)/);
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

  it("shows each signer's workspace authority and approvals that no longer count (review finding 5)", () => {
    const base = provenReport();
    const report = provenReport({
      signoff: {
        ...base.signoff,
        approvals: [{ role: "engineering", user: "e@x.io", at: "2026-11-11T10:00:00Z", note: "", signer_role: "editor", authority: "workspace editor" }],
        invalidated_approvals: [{ role: "data_owner", user: "d@x.io", at: "2026-11-11T09:00:00Z", note: "", signer_role: "editor" }],
      },
    });
    const t = text(view(report));
    assert.match(t, /e@x\.io \(workspace editor\)/);
    assert.match(t, /1 approval \(Data owner\) attested an earlier version of this report and no longer counts/);
    assert.match(t, /your role in this job's workspace/);
  });

  it("colours the evidence badges by their value, not by their presence (review U1)", () => {
    const base = provenReport();
    const html = view(provenReport({
      evidence: { ...base.evidence, assurance_level: "none", trust_score: 41, transform_proof: "failed", protection_proof: "verified" },
    }));
    const badge = (value: string) => new RegExp(`<span class="([^"]+)"[^>]*>${value}</span>`).exec(html)?.[1] ?? "";
    assert.match(badge("none"), /df2-badge-error/);
    assert.match(badge("41"), /df2-badge-error/);
    assert.match(badge("failed"), /df2-badge-error/);
    assert.match(badge("verified"), /df2-badge-live/);
  });

  it("links to the existing Gate-8 and trust-score cards rather than redrawing them", () => {
    const t = text(view(provenReport(), { onOpenGate8: noop, onOpenDetail: noop }));
    assert.match(t, /Open Gate-8 proof/);
    assert.match(t, /Open trust score and ledger/);
  });
});

/* ── Polish: defects found on the merged tab ─────────────────── */

/** The header toolbar's markup. */
function toolbar(html: string): string {
  return /<header class="df2-parity-toolbar">([\s\S]*?)<\/header>/.exec(html)?.[1] ?? "";
}

/** Each button in some markup: its classes and visible text. */
function buttons(html: string): Array<{ cls: string; text: string }> {
  return [...html.matchAll(/<button[^>]*class="([^"]*)"[^>]*>([\s\S]*?)<\/button>/g)].map((m) => ({
    cls: m[1],
    text: text(m[2]).trim(),
  }));
}

/** The KPI tiles, in order, as "label: value". */
function kpis(html: string): string[] {
  return [...html.matchAll(/<div class="df2-parity-kpi"><dt>([^<]*)<\/dt><dd[^>]*>([\s\S]*?)<\/dd><\/div>/g)].map(
    (m) => `${m[1]}: ${text(m[2]).trim()}`,
  );
}

describe("Trust Report polish: nothing compared reads as nothing compared", () => {
  it("the ring of a failed run shows a dash and \"not compared\", never a sampled bound", () => {
    const html = view(failedRunReport(), { jobStatus: "failed" });
    assert.match(html, /class="df2-parity-ring-value">—</);
    assert.match(html, /class="df2-parity-ring-caption">not compared</);
    assert.doesNotMatch(html, /sampled bound/);
  });

  it("a ring without a figure after a comparison that could not decide says so, not \"not compared\"", () => {
    const failedGate8 = provenReport({
      verdict: "unverifiable",
      verdict_reason: "Gate-8 failed on rows its evidence does not name.",
      parity: { ...provenReport().parity, proven_equal_pct: null },
      evidence: { ...provenReport().evidence, reconciliation_passed: false },
    });
    const html = view(failedGate8);
    assert.match(html, /class="df2-parity-ring-caption">not determined</);
  });

  it("rows differ is \"Not compared\" when no comparison ran, and there is no explained tile", () => {
    const html = view(failedRunReport(), { jobStatus: "failed" });
    assert.deepEqual(kpis(html), [
      "rows in source: Unknown",
      "rows in destination: Unknown",
      "rows differ: Not compared",
    ]);
    assert.match(html, /<dl class="df2-parity-kpis is-3">/);
  });

  it("an undecided comparison does not claim zero rows differ", () => {
    const failedGate8 = provenReport({
      verdict: "unverifiable",
      parity: { ...provenReport().parity, proven_equal_pct: null },
      evidence: { ...provenReport().evidence, reconciliation_passed: false },
    });
    assert.ok(kpis(view(failedGate8)).includes("rows differ: Unknown"));
  });

  it("the explained tile shows only when rows differ, so the grid holds 3 or 4 tiles", () => {
    const proven = view(provenApiWordingReport());
    assert.deepEqual(kpis(proven), ["rows in source: 6", "rows in destination: 6", "rows differ: 0"]);
    assert.match(proven, /<dl class="df2-parity-kpis is-3">/);
    const differing = view(unexplainedReport());
    assert.ok(kpis(differing).includes("explained: 8 / 12"));
    assert.match(differing, /<dl class="df2-parity-kpis is-4">/);
  });
});

describe("Trust Report polish: header actions follow the sign-off state", () => {
  it("sign-off open: Go to sign-off is the primary action", () => {
    const bar = buttons(toolbar(view(provenReport())));
    const go = bar.find((b) => b.text === "Go to sign-off");
    assert.ok(go, bar.map((b) => b.text).join(" | "));
    assert.match(go.cls, /df2-btn-primary/);
  });

  it("sign-off blocked: no primary sign-off button, a secondary link says why", () => {
    const html = view(failedRunReport(), { jobStatus: "failed" });
    const bar = buttons(toolbar(html));
    assert.ok(!bar.some((b) => /df2-btn-primary/.test(b.cls)), `no primary action while blocked: ${JSON.stringify(bar)}`);
    assert.ok(!bar.some((b) => b.text === "Go to sign-off"));
    const why = bar.find((b) => b.text === "Why sign-off is blocked");
    assert.ok(why, bar.map((b) => b.text).join(" | "));
    assert.match(toolbar(html), /title="The evidence cannot decide parity, so there is nothing to sign\."/);
    // An unexplained report is blocked too.
    assert.ok(!buttons(toolbar(view(unexplainedReport()))).some((b) => b.text === "Go to sign-off"));
  });

  it("downloads are labelled by what they download; the signed pack only once signed", () => {
    const blocked = buttons(toolbar(view(failedRunReport(), { onDownloadSigned: noop })));
    assert.ok(blocked.some((b) => b.text === "Download report (.md)"), blocked.map((b) => b.text).join(" | "));
    assert.ok(!blocked.some((b) => /pack/i.test(b.text)), "no pack before signing: the API answers 409");

    const signed = buttons(toolbar(view(signedReport(), { onDownloadSigned: noop })));
    assert.ok(signed.some((b) => b.text === "Download report (.md)"));
    const pack = signed.find((b) => b.text === "Download signed pack (.json)");
    assert.ok(pack, signed.map((b) => b.text).join(" | "));
    assert.match(pack.cls, /df2-btn-primary/);
    const viewSignoff = signed.find((b) => b.text === "View sign-off");
    assert.ok(viewSignoff);
    assert.doesNotMatch(viewSignoff.cls, /df2-btn-primary/);
  });
});

describe("Trust Report polish: one verdict, said once", () => {
  it("the verdict pill shows in the header only, not again under the ring", () => {
    for (const report of [provenReport(), failedRunReport(), unexplainedReport()]) {
      const html = view(report);
      const ringCard = /<div class="df2-parity-card df2-parity-ring-card">([\s\S]*?)<\/div><div class="df2-parity-card/.exec(html)?.[1] ?? "";
      assert.ok(ringCard, "ring card found");
      assert.doesNotMatch(ringCard, /df2-badge/);
      assert.match(toolbar(html), /df2-badge/);
    }
  });

  it("a failed run that never reached verification says so plainly", () => {
    const t = text(view(failedRunReport(), { jobStatus: "failed" }));
    assert.match(t, /This run failed before verification ran, so the source and destination were never compared\./);
    assert.doesNotMatch(t, /Gate-8 recorded no reconciliation/);
    assert.doesNotMatch(t, /No differences were found/);
    const cancelled = text(view(failedRunReport(), { jobStatus: "cancelled" }));
    assert.match(cancelled, /This run was cancelled before verification ran/);
  });

  it("otherwise the notice keeps the API's reason", () => {
    // A completed job with no reconciliation: the API's reason is the honest one.
    assert.match(text(view(failedRunReport(), { jobStatus: "completed" })), /Gate-8 recorded no reconciliation for this job/);
    // A failed job whose Gate-8 did run: the reason names what it found.
    const ran = provenReport({
      verdict: "unverifiable",
      verdict_reason: "Values were not compared for orders (only row counts or nothing was recorded), so the evidence cannot decide.",
      evidence: { ...provenReport().evidence, reconciliation_passed: false },
    });
    const t = text(view(ran, { jobStatus: "failed" }));
    assert.match(t, /Values were not compared for orders/);
    assert.doesNotMatch(t, /failed before verification ran/);
  });

  it("proven: the confidence statement is not repeated by the verdict reason", () => {
    const t = text(view(provenApiWordingReport()));
    assert.match(t, /How sure is this\? Every row was compared and no row differs\./);
    assert.doesNotMatch(t, /No differences: the destination matches the source\./);
    // A reason that adds information still shows.
    assert.match(text(view(unexplainedReport())), /4 rows in payments\.amount differ with no known cause\./);
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

  async function mount(jobStatus = "completed") {
    const { createRoot } = await import("react-dom/client");
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    await React.act(async () => {
      root!.render(<TrustReportTab jobId="job-31" jobStatus={jobStatus} />);
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
    // The decision carries the version it was taken on (review finding 4: 409 when it moved on).
    assert.equal(post.body, '{"note":"Widen per ticket 88","expected_version":3}');
    // While the request runs, every action is disabled.
    assert.equal(button("Explain again").disabled, true);
    assert.equal(button("Download report (.md)").disabled, true);
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

  it("will not accept a difference without a note (review finding 6)", async () => {
    getReport = () => {
      const r = unexplainedReport();
      const accept = { ...r.fixes[0], id: "fix-accept", action: "accept" as const, title: "Accept 4 row(s)", preview: null };
      return { ...r, fixes: [accept] };
    };
    await mount();
    await click(button("Mark as accepted"));
    const confirm = button("Confirm and apply");
    assert.equal(confirm.disabled, true, "no note, no acceptance");
    assert.match(host!.textContent || "", /required: why is this acceptable\?/);
    await type(host!.querySelector<HTMLTextAreaElement>(".df2-parity-confirm textarea")!, "Known source rounding");
    assert.equal(button("Confirm and apply").disabled, false);
    assert.equal(calls.filter((c) => c.method === "POST").length, 0);
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
    assert.equal(host!.querySelectorAll(".df2-parity-toolbar button").length, 3, "no signed-pack button before signing");
    await click(button("Download report (.md)"));
    await React.act(async () => settle());
    assert.ok(calls.some((c) => /\/parity-report\/export\?format=markdown$/.test(c.url)));
    assert.equal(made.length, 1);
    assert.equal(await made[0].text(), "# Trust report");
    assert.equal(downloaded, "datawrap-trust-report-job-31.md");

    // Once signed, the header also offers the signed pack, which is the JSON export.
    await React.act(async () => root!.unmount());
    host!.remove();
    calls.length = 0;
    getReport = () => signedReport({ job_id: "job-31" });
    await mount();
    const packs = [...host!.querySelectorAll<HTMLButtonElement>(".df2-parity-toolbar button")].filter((b) =>
      /signed pack/.test(b.textContent || ""),
    );
    assert.equal(packs.length, 1);
    await click(packs[0]);
    await React.act(async () => settle());
    assert.ok(calls.some((c) => /\/parity-report\/export\?format=json$/.test(c.url)));
    assert.equal(downloaded, "datawrap-parity-signed-pack-job-31.json");
  });

  it("tells a failed run that never reached verification apart from the API's generic reason", async () => {
    getReport = () => failedRunReport({ job_id: "job-31" });
    await mount("failed");
    const t = host!.textContent || "";
    assert.match(t, /This run failed before verification ran/);
    assert.match(t, /Not compared/);
    assert.doesNotMatch(t, /Go to sign-off/);
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

describe("bounded re-reads while a fix applies (re-review finding 15)", () => {
  it("says so when the automatic re-reads stop and the fix is still applying", () => {
    const base = unexplainedReport();
    const applying = { ...base, fixes: base.fixes.map((f, i) => (i === 0 ? { ...f, status: "applying" as const } : f)) };
    assert.match(text(view(applying, { pollStopped: true })), /Stopped checking automatically/);
    assert.doesNotMatch(text(view(applying, { pollStopped: false })), /Stopped checking automatically/);
    assert.doesNotMatch(text(view(base, { pollStopped: true })), /Stopped checking automatically/);
  });

  it("shows a lower-bound row count as at least (re-review finding 4)", () => {
    const base = unexplainedReport();
    const lower = { ...base, differences: base.differences.map((d, i) => (i === 0 ? { ...d, rows_at_least: true } : d)) };
    assert.match(view(lower), /aria-label="at least \d+ rows"/);
  });
});
