/**
 * Exit readiness (Page A) and a rehearsal run (Page B): each state renders the
 * mockup's structure and the plan's honesty rules — proven, not proven with
 * its reason, overdue, running, incomplete with missing rows, an anchor that is
 * anchored / not anchored / failed — against hand-made fixtures at the mockup's
 * scale and against payloads captured from the API.
 */
import assert from "node:assert/strict";
import { after, afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ExitBoardView, boardMetrics, type ExitBoardViewProps } from "./ExitBoardView.js";
import { RehearsalView, type RehearsalViewProps } from "./RehearsalView.js";
import { RehearsalPanel } from "./RehearsalPanel.js";
import { SystemFormDrawer } from "./SystemFormDrawer.js";
import { PiiReviewCard } from "./SystemDrawer.js";
import { DurationTrendChart, WindowBar, durationTrendSummary } from "../charts/ExitCharts.js";
import {
  ANCHORED,
  ANCHOR_FAILED,
  trendFixture,
  FIXTURE_NOW,
  NOT_ANCHORED,
  boardFixture,
  incompleteRehearsal,
  notProvenRehearsal,
  provenRehearsal,
  runningRehearsalRaw,
  signedRehearsal,
  verifyFixture,
} from "./exitFixtures.js";
import { realBoard, realFailed, realIncomplete, realMasked, realNotProven, realRunning, realSigned, realUnprovable, realVerify } from "./realExitPayloads.js";
import { realPiiAcked, realPiiHeld, realSystemPii, realSystemPiiAcked } from "./realExitPayloads.js";
import { realStale } from "./realExitPayloads.js";
import { STALE_TEXT, formatDate, normalizeRehearsal, type ExitRehearsal } from "../../lib/exitReadiness.js";

const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const noop = () => {};

function text(html: string): string {
  return html
    .replace(/<title>[\s\S]*?<\/title>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ");
}

/** The plan's forbidden claims, on every page. */
function assertHonest(t: string) {
  assert.doesNotMatch(t, /\bcompliant\b|compliance with the|zero data loss|one-click|undo the|autonomous|zero-knowledge/i);
}

function board(over: Partial<ExitBoardViewProps> = {}) {
  return renderToStaticMarkup(
    <ExitBoardView
      readiness={boardFixture()}
      query=""
      onQueryChange={noop}
      filter="all"
      onFilterChange={noop}
      selectedId={null}
      onOpenSystem={noop}
      onRunRehearsal={noop}
      onDeclare={noop}
      canDeclare
      canRun
      now={FIXTURE_NOW}
      {...over}
    />,
  );
}

function run(rehearsal: ExitRehearsal, over: Partial<RehearsalViewProps> = {}) {
  return renderToStaticMarkup(
    <RehearsalView
      rehearsal={rehearsal}
      busy={null}
      canSign
      onBack={noop}
      onRefresh={noop}
      onExportMarkdown={noop}
      onDownloadSigned={noop}
      onVerifySigned={noop}
      onVerifyFile={noop}
      onSign={noop}
      {...over}
    />,
  );
}

describe("Page A: the exit readiness board", () => {
  it("shows the four KPIs the mockup names, measured and labelled", () => {
    const t = text(board());
    assert.match(t, /Systems with a proven exit 4 \/ 9/);
    assert.match(t, /Rehearsals overdue 2 Past their rehearsal interval/);
    assert.match(t, /Longest measured exit 61 h Core ledger · proven, not estimated/);
    assert.match(t, new RegExp(`Next regulatory date ${esc(formatDate("2027-01-12"))} Data Act switching charges end · Core ledger`));
    assert.match(t, /Declare a system/);
    assert.match(t, /Run rehearsal/);
    assertHonest(t);
  });

  it("lists each system with criticality, last rehearsal, measured vs window, completeness and status", () => {
    const t = text(board());
    assert.match(t, new RegExp(`Core ledger Oracle · 412 tables Critical ${esc(formatDate("2026-11-02T09:00:00Z"))} Due in 79 days 61 h / 30 d Proven, 412 tables Proven`));
    // Overdue outranks an old proof, and says by how much.
    assert.match(t, new RegExp(`Payments SQL Server · 88 tables Critical ${esc(formatDate("2026-08-19T09:00:00Z"))} Overdue by 24 days 28 h / 30 d Proven, 88 tables Overdue`));
    // Never rehearsed: nothing measured, nothing invented.
    assert.match(t, /CRM Unknown engine · 31 tables Standard Never Due in 38 days Not measured Never rehearsed Never rehearsed/);
    // Measured but not proven: not compared with the window (contract 10.1.4), in words.
    assert.match(t, /40 h \/ 36 h · not compared Incomplete Incomplete/);
    assert.match(t, /Rehearsal running Running/);
  });

  it("puts overdue systems first and draws the window bar only where something was measured", () => {
    const html = board();
    const order = [...html.matchAll(/id="exit-system-([^"]+)"/g)].map((m) => m[1]);
    assert.deepEqual(order.slice(0, 2), ["exs_payments0001", "exs_loyalty00001"]);
    const bars = html.match(/class="df2-exit-window-bar/g) ?? [];
    assert.equal(bars.length, boardFixture().systems.filter((s) => s.measured_seconds != null).length);
    assert.match(html, /aria-label="40 h measured of a 36 h window, 111% of the window, not compared with the window \(only proven runs are\)"/);
  });

  it("keeps rows keyboard reachable with one tab stop and labelled actions", () => {
    const html = board();
    assert.equal((html.match(/<tr[^>]*tabindex="0"/g) ?? []).length, 1);
    assert.match(html, /aria-label="Run a rehearsal of Core ledger"/);
    assert.match(html, /aria-label="Open Core ledger"/);
    assert.match(html, /Use the up and down arrow keys/);
  });

  it("filters and searches", () => {
    assert.match(text(board({ filter: "overdue" })), /Payments.*Loyalty/);
    assert.doesNotMatch(text(board({ filter: "overdue" })), /Core ledger Oracle/);
    assert.match(text(board({ query: "zzz" })), /No matching systems/);
  });

  it("disables run and declare without permission and says why", () => {
    const html = board({ canRun: false, canDeclare: false, runDeniedReason: "Your role cannot run jobs." });
    assert.match(html, /title="Your role cannot run jobs\."/);
  });

  it("renders the board the API really returns", () => {
    const t = text(renderToStaticMarkup(
      <ExitBoardView readiness={realBoard} query="" onQueryChange={noop} filter="all" onFilterChange={noop} selectedId={null}
        onOpenSystem={noop} onRunRehearsal={noop} onDeclare={noop} canDeclare canRun />,
    ));
    assert.match(t, new RegExp(`Systems with a proven exit ${realBoard.kpis.systems_proven} / ${realBoard.kpis.systems}`));
    assert.match(t, /Rehearsal failed/);
    assert.match(t, /Rehearsal running Running/);
    const m = boardMetrics(realBoard);
    assert.match(m[2].value, /^\d+ s$/);  // a seconds-long engine run, measured
  });
});

describe("Page B: a rehearsal run", () => {
  it("proven: timeline, completeness with coverage per method and the Merkle root, measured duration, sign-off open", () => {
    const t = text(run(provenRehearsal()));
    assert.match(t, /Exit readiness \/ Core ledger · rehearsal 6/);
    assert.match(t, /Proven/);
    assert.match(t, /Inventory Done · 3 s Extract Done · 52 h Verify Done · 8 h Seal Done · 6 s Sign-off Pending/);
    assert.match(t, /412 of 412 declared tables extracted/);
    assert.match(t, /290 Full checksum every row compared by checksum 30 Key diff/);
    assert.match(t, /92 Population digest/);
    assert.doesNotMatch(t, /Not proof/);
    assert.match(t, /Files sealed: Merkle root sha256:[0-9a-f]{4}…[0-9a-f]{4} over 412 parts/);
    assert.match(t, /measured 61 h window 30 d margin 91%/);
    assert.match(t, /Fits the window · 8\.5% of the window/);
    assert.match(t, /Measured from job clocks, not estimated/);
    assert.match(t, /Projection, not measured: about 63 h/);
    assert.match(t, /Sign as Data owner/);
    assert.doesNotMatch(t, /Only a proven rehearsal can be signed/);
    assertHonest(t);
  });

  it("not proven: the reason, sampled tables marked 'Not proof', sign-off blocked", () => {
    const t = text(run(notProvenRehearsal()));
    assert.match(t, /Not proven/);
    assert.match(t, /26 of 40 table\(s\) proven; 14 could not be proven in full/);
    assert.match(t, /14 Sampled a sample bounds nothing here; not proof Not proof/);
    assert.match(t, /Sampled \(not proof\)/);
    assert.match(t, /Only a proven rehearsal can be signed/);
    // The sign form stays visible (Parity's panel) but cannot submit.
    assert.match(run(notProvenRehearsal()), /<button[^>]*type="submit"[^>]*disabled=""[^>]*>Sign as Engineering/);
    assert.match(t, /Files sealed: Merkle root sha256:[0-9a-f]{4}…[0-9a-f]{4} over 40 parts/);
  });

  it("incomplete: missing rows and differing control totals are named, at least where only digests differ", () => {
    const html = run(incompleteRehearsal());
    const t = text(html);
    assert.match(t, /Incomplete/);
    assert.match(t, /1 of 6 table\(s\) differ from the source: fact_orders/);
    assert.match(t, /≥ 1 missing rows The extract holds 1 row\(s\) fewer than the source/);
    assert.match(t, /control total Control totals of amount differ/);
    assert.match(t, /1,204,331 → 1,204,330/);
    assert.match(t, /1 of 2 differ/);
    // Not proven, so not compared with the window (contract 10.1.5): no margin, no ratio, no fit claim.
    assert.match(t, /margin — /);
    assert.match(t, /Not compared with the window Only measured for proven runs\./);
    assert.doesNotMatch(t, /over by|Does not fit the window|of the window ·/);
    assert.match(html, /aria-expanded="true"/, "a table that differs opens its details");
  });

  it("running: nothing measured is shown as measured", () => {
    const t = text(run(normalizeRehearsal(runningRehearsalRaw())));
    assert.match(t, /Running/);
    assert.match(t, /Extract Running/);
    assert.match(t, /0 of 3 declared tables extracted/);
    assert.match(t, /Not measured yet: the rehearsal is still running/);
    assert.match(t, /Not sealed yet/);
    assert.match(t, /measured — window 30 d/);
    assert.match(t, /The rehearsal is still running; sign-off opens when it is proven/);
    assert.doesNotMatch(t, /Fits the window/);
    assert.doesNotMatch(t, /Extract Not measured|0 B sealed/);
    assert.match(t, /timed from the job clocks once they finish; nothing is estimated/);
  });

  it("anchor: anchored, not anchored and failed each read as what they are", () => {
    const anchored = text(run(signedRehearsal(ANCHORED)));
    assert.match(anchored, /Write-once anchor Anchored Held in S3 Object Lock \(compliance mode\) until/);
    assert.match(anchored, /Verify signed pack/);
    assert.match(anchored, /Download signed pack/);

    const stub = text(run(signedRehearsal(NOT_ANCHORED)));
    assert.match(stub, /Write-once anchor Not anchored the local stub is not a write-once store/);
    assert.doesNotMatch(stub, /Held in S3 Object Lock/);

    const failedHtml = run(signedRehearsal(ANCHOR_FAILED));
    assert.match(text(failedHtml), /Anchor failed The anchor could not be confirmed: the bucket datawrap-audit-anchors does not have Object Lock enabled/);
    assert.match(failedHtml, /df2-badge-error">Anchor failed/);
  });

  it("shows a verification result check by check, and a failed one with its errors", () => {
    const ok = text(run(signedRehearsal(), { verifyResult: verifyFixture() }));
    assert.match(ok, /Pack verified/);
    assert.match(ok, /Match Signature over the pack body/);
    assert.match(ok, /Match Merkle root recomputed from the parts/);
    const bad = text(run(signedRehearsal(), {
      verifyResult: verifyFixture({ ok: false, merkle_ok: false, errors: ["the Merkle root does not match the parts listed in the pack"] }),
    }));
    assert.match(bad, /Pack did not verify/);
    assert.match(bad, /Differs Merkle root recomputed/);
    assert.match(bad, /the Merkle root does not match the parts listed in the pack/);
  });

  it("states that the pack is evidence, not a compliance certificate", () => {
    const t = text(run(signedRehearsal()));
    assert.match(t, /It does not certify compliance with any regulation; your auditors decide what it satisfies/);
  });

  it("renders every rehearsal the API really returns", () => {
    for (const r of [realSigned, realIncomplete, realNotProven, realFailed, realMasked, realUnprovable, normalizeRehearsal(realRunning)]) {
      const t = text(run(r));
      assert.match(t, new RegExp(`rehearsal ${r.n}`));
      assertHonest(t);
    }
    const failed = text(run(realFailed));
    assert.match(failed, /1 of 2 table\(s\) could not be extracted: no_such_table/);
    const notProven = text(run(realNotProven));
    assert.match(notProven, /Count only/);
    assert.match(notProven, /Not proof/);
    const signed = text(run(realSigned));
    assert.match(signed, /Not anchored the local stub is not a write-once store/);
  });
});

describe("contract 10: what the review fixes changed on screen", () => {
  function realBoardHtml(over: Partial<ExitBoardViewProps> = {}) {
    return renderToStaticMarkup(
      <ExitBoardView readiness={realBoard} query="" onQueryChange={noop} filter="all" onFilterChange={noop} selectedId={null}
        onOpenSystem={noop} onRunRehearsal={noop} onDeclare={noop} canDeclare canRun {...over} />,
    );
  }

  it("board: a system whose definition changed reads not proven and says to re-run", () => {
    const stale = realBoard.systems.find((s) => s.stale)!;
    assert.equal(stale.name, "Billing");
    const html = realBoardHtml();
    const rowHtml = html.match(new RegExp(`<tr[^>]*id="exit-system-${stale.id}"[\\s\\S]*?</tr>`))![0];
    const t = text(rowHtml);
    assert.match(t, new RegExp(`Not proven \\(changed since the last rehearsal\\) ${esc(STALE_TEXT)} Not proven`));
    assert.doesNotMatch(t, /\bProven\b/);
  });

  it("board: the running rehearsal is linked directly", () => {
    const html = realBoardHtml({ onOpenRehearsal: noop });
    const row = realBoard.systems.find((s) => s.state === "running")!;
    assert.match(html, new RegExp(`aria-label="Open the running rehearsal of ${esc(row.name)}"`));
    assert.doesNotMatch(html, new RegExp(`aria-label="A rehearsal of ${esc(row.name)} is running"`));
  });

  it("board: the longest measured exit counts proven runs only, and says so", () => {
    const m = boardMetrics(realBoard);
    assert.match(m[2].sub ?? "", /proven, not estimated/);
    const none = boardMetrics({ ...realBoard, kpis: { ...realBoard.kpis, longest_measured_seconds: null, longest_measured_system_id: null } });
    assert.equal(none[2].sub, "No proven exit measured yet");
  });

  it("board: a measured run that is not proven is not compared with the window", () => {
    const html = board();
    assert.match(text(html), /40 h \/ 36 h · not compared Incomplete Incomplete/);
    assert.match(html, /aria-label="40 h measured of a 36 h window, 111% of the window, not compared with the window \(only proven runs are\)"/);
    assert.doesNotMatch(text(html), /does not fit/);
  });

  it("rehearsal: control totals of a PII column read Masked (PII), never 0 or blank", () => {
    const html = run(realMasked);
    assert.match(html, /aria-expanded="false"/);
    const opened = renderToStaticMarkup(
      <RehearsalView rehearsal={{ ...realMasked, tables: realMasked.tables.map((t) => ({ ...t, status: "incomplete" as const })) }} busy={null} canSign onBack={noop}
        onRefresh={noop} onExportMarkdown={noop} onDownloadSigned={noop} onVerifySigned={noop} onVerifyFile={noop} onSign={noop} />,
    );
    const t = text(opened);
    assert.match(t, /zip Masked \(PII\) Masked \(PII\) Masked \(PII\) Match/);
    assert.doesNotMatch(t, /zip — —/);
  });

  it("rehearsal: a running table's value check reads Pending", () => {
    const t = text(run(normalizeRehearsal(realRunning)));
    assert.match(t, /orders Running Not counted Not measured Not measured Not measured Pending/);
    assert.doesNotMatch(t, /Not measured \(not proof\)/);
  });

  it("rehearsal: a measured run that is not proven is not compared with the window", () => {
    const t = text(run(realNotProven));
    assert.match(t, /margin — /);
    assert.match(t, /Not compared with the window/);
    assert.match(t, /Only measured for proven runs\./);
    assert.doesNotMatch(t, /Fits the window|Does not fit the window/);
    // The source could not be re-read: its totals were not measured, not "no numeric columns".
    assert.match(t, /orders Not proven \? → 60 Not measured Not measured Not measured Count only/);
  });

  it("rehearsal: the verify answer says the stored parts were not re-read", () => {
    const t = text(run(realSigned, { verifyResult: realVerify }));
    assert.match(t, /Not re-read The stored extract parts are not re-read here: the Merkle root is recomputed/);
  });

  it("rehearsal: a column that cannot be proven is named with why", () => {
    const opened = renderToStaticMarkup(
      <RehearsalView rehearsal={{ ...realUnprovable, tables: realUnprovable.tables.map((t) => ({ ...t, status: "incomplete" as const })) }} busy={null} canSign onBack={noop}
        onRefresh={noop} onExportMarkdown={noop} onDownloadSigned={noop} onVerifySigned={noop} onVerifyFile={noop} onSign={noop} />,
    );
    assert.match(text(opened), /Not provable payload No declared type/);
    // In the table row too, so it is seen before the details open.
    assert.match(text(run(realUnprovable)), /notes Not proven .*1 column not provable/);
  });

  it("rehearsal: names the definition it proved", () => {
    const t = text(run(realSigned));
    assert.match(t, new RegExp(`Definition SHA-256 ${realSigned.definition_sha256.slice(0, 4)}…${realSigned.definition_sha256.slice(-4)}`));
  });
});

describe("duration trend", () => {
  it("draws no point for a rehearsal that was not measured, and says which", () => {
    const trend = trendFixture(); // rehearsal 3 failed: measured_seconds null
    const html = renderToStaticMarkup(<DurationTrendChart trend={trend} windowSeconds={30 * 86400} />);
    // Five measured points (each with a hit target), none for rehearsal 3, and no hollow mark on an axis.
    assert.equal((html.match(/<circle class="df2-parity-point/g) ?? []).length, 5);
    assert.doesNotMatch(html, /Rehearsal 3:/);
    assert.doesNotMatch(html, /is-sampled/);
    assert.match(html, /class="df2-exit-trend-gap"/, "a dotted bridge marks the gap");
    assert.match(text(html), /Not measured, so not drawn: rehearsal 3 \(failed\)/);
    assert.match(durationTrendSummary(trend, 30 * 86400), /Not measured, so not drawn: rehearsal 3 \(failed\)\./);
  });

  it("uses a min/max scale, not a zero baseline", () => {
    const html = renderToStaticMarkup(<DurationTrendChart trend={trendFixture()} windowSeconds={30 * 86400} />);
    const labels = [...html.matchAll(/class="df2-parity-chart-label"[^>]*>([^<]+)</g)].map((m) => m[1]);
    assert.ok(labels.includes("63 h"), labels.join("|"));
    assert.ok(labels.includes("52 h"), labels.join("|"));
    assert.ok(!labels.includes("0"), "no zero baseline label");
    // The lowest point sits near the bottom of the plot, the highest near the top.
    const ys = [...html.matchAll(/<circle class="df2-parity-point[^"]*" cx="[^"]+" cy="([^"]+)"/g)].map((m) => Number(m[1]));
    assert.ok(Math.max(...ys) - Math.min(...ys) > 60, `spread ${Math.max(...ys) - Math.min(...ys)}`);
  });
});

describe("window bar", () => {
  it("says what it draws; nothing measured draws no fill", () => {
    const fit = renderToStaticMarkup(<WindowBar measured={61 * 3600} window={30 * 86400} fits />);
    assert.match(fit, /aria-label="61 h measured of a 30 d window, 8\.5% of the window"/);
    assert.match(fit, /df2-exit-window-fill/);
    const over = renderToStaticMarkup(<WindowBar measured={40 * 3600} window={36 * 3600} fits={false} />);
    assert.match(over, /aria-label="40 h measured of a 36 h window, 111% of the window, does not fit"/);
    assert.match(over, /is-over/);
    // Measured but not compared (fits null): never drawn as over the window.
    const uncompared = renderToStaticMarkup(<WindowBar measured={40 * 3600} window={36 * 3600} fits={null} />);
    assert.match(uncompared, /not compared with the window \(only proven runs are\)/);
    assert.doesNotMatch(uncompared, /is-over|does not fit/);
    const none = renderToStaticMarkup(<WindowBar measured={null} window={30 * 86400} />);
    assert.match(none, /Not measured against a 30 d window/);
    assert.doesNotMatch(none, /df2-exit-window-fill/);
  });
});

/* ── Interactive (jsdom) ─────────────────────────────────────── */

describe("Exit pages actions (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  let root: Root | null = null;
  let host: HTMLElement | null = null;
  const calls: Array<{ method: string; url: string; body: string }> = [];
  let getRehearsal: () => unknown = () => provenRehearsal();
  let onPost: (url: string, body: string) => Response = () => new Response("{}", { status: 500 });
  const saved = { fetch: globalThis.fetch };

  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
    });
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
    // The shared Drawer focuses its panel on the next frame.
    Object.assign(dom.window, {
      requestAnimationFrame: (cb: FrameRequestCallback) => setTimeout(() => cb(0), 0) as unknown as number,
      cancelAnimationFrame: (id: number) => clearTimeout(id),
    });
    globalThis.fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const method = String(init.method || "GET");
      const url = String(input);
      const body = String(init.body ?? "");
      calls.push({ method, url, body });
      if (method === "GET") return new Response(JSON.stringify(getRehearsal()), { status: 200 });
      return onPost(url, body);
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
    getRehearsal = () => provenRehearsal();
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

  function button(label: string | RegExp, scope: ParentNode = host!): HTMLButtonElement {
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
  }

  it("signs with the version decided on, then shows the signed state", async () => {
    let version = 7;
    getRehearsal = () => ({ ...provenRehearsal(), version });
    onPost = (url) => {
      assert.match(url, /\/exit\/rehearsals\/exr_core00000006\/signoff$/);
      version = 8;
      return new Response(JSON.stringify(signedRehearsal()), { status: 200 });
    };
    await mount(<RehearsalPanel rehearsalId="exr_core00000006" onBack={noop} />);
    assert.match(host!.textContent || "", /412 of 412 declared tables extracted/);
    await click(button(/^Sign as Data owner$/));
    const post = calls.find((c) => c.method === "POST");
    assert.ok(post);
    assert.deepEqual(JSON.parse(post!.body), { role: "data_owner", expected_version: 7 });
    assert.match(host!.textContent || "", /Download signed pack/);
  });

  it("a refused sign-off shows the API's reason beside the form", async () => {
    onPost = () => new Response(JSON.stringify({ detail: "This role has already signed." }), { status: 409, headers: { "Content-Type": "application/json" } });
    await mount(<RehearsalPanel rehearsalId="exr_core00000006" onBack={noop} />);
    await click(button(/^Sign as Data owner$/));
    assert.match(host!.textContent || "", /This role has already signed\. The page has been refreshed/);
  });

  it("verifies the signed pack: reads the pack, posts it, shows each check", async () => {
    getRehearsal = () => signedRehearsal(NOT_ANCHORED);
    onPost = (url, body) => {
      assert.match(url, /\/exit\/evidence\/verify$/);
      assert.ok(JSON.parse(body).pack);
      return new Response(JSON.stringify(verifyFixture({ anchor: { ...verifyFixture().anchor, status: "not_anchored", provider: "stub", reason: "the local stub is not a write-once store", retention: null } })), { status: 200 });
    };
    await mount(<RehearsalPanel rehearsalId="exr_core00000006" onBack={noop} />);
    await click(button("Verify signed pack"));
    const t = host!.textContent || "";
    assert.match(t, /Pack verified/);
    assert.match(t, /Not anchored/);
    assert.ok(calls.some((c) => c.method === "GET" && /evidence\?format=json$/.test(c.url)));
  });

  it("a table's details open and close from the keyboard-reachable toggle", async () => {
    await mount(<RehearsalPanel rehearsalId="exr_core00000006" onBack={noop} />);
    const toggle = host!.querySelector<HTMLButtonElement>('button[aria-label="Show details for gl_entries"]');
    assert.ok(toggle);
    assert.equal(toggle!.getAttribute("aria-expanded"), "false");
    await click(toggle!);
    const open = host!.querySelector<HTMLButtonElement>('button[aria-label="Hide details for gl_entries"]');
    assert.equal(open?.getAttribute("aria-expanded"), "true");
    assert.match(host!.textContent || "", /Key-set hash/);
  });

  it("the declare form refuses an empty system and names each field", async () => {
    let submitted = false;
    await mount(
      <SystemFormDrawer open system={null} connectors={[]} onClose={noop} onSubmit={async () => { submitted = true; }} />,
    );
    await click(button("Declare system", document.body));
    assert.equal(submitted, false);
    const t = document.body.textContent || "";
    assert.match(t, /Name the system\./);
    assert.match(t, /Choose the saved connection/);
    assert.match(t, /Declare at least one table/);
  });
});

describe("contract 11: what the second review fixes changed on screen", () => {
  function pii(system: Parameters<typeof PiiReviewCard>[0]["system"], over: Partial<Parameters<typeof PiiReviewCard>[0]> = {}) {
    return renderToStaticMarkup(
      <PiiReviewCard system={system} reason="" onReasonChange={noop} busy={false} error="" canManage onAcknowledge={noop}
        onWithdraw={noop} {...over} />,
    );
  }

  it("rehearsal: a run the PII gate held reads Needs PII review, names the columns, never a value, and offers the review", () => {
    const html = run(realPiiHeld, { onOpenSystem: noop });
    const t = text(html);
    assert.match(t, /Needs PII review/);
    assert.match(t, /The PII gate held these extracts: nothing was bypassed\. Acknowledge PII for HR people/);
    assert.match(t, /Open PII review/);
    assert.doesNotMatch(t, /123-45-60|p1@example\.com/);
    assert.doesNotMatch(t, /\bProven\b/);
  });

  it("rehearsal: an acknowledged run shows who acknowledged PII, when and why", () => {
    const t = text(run(realPiiAcked));
    assert.match(t, /PII acknowledged by dpo@example\.com on .*DPA clause 9 allows the exit extract \(carried by this rehearsal's extracts and its evidence pack\)/);
  });

  it("system: the PII review asks for a reason and the action, and is disabled without job.manage", () => {
    const t = text(pii(realSystemPii));
    assert.match(t, /PII review The PII gate held the last rehearsal/);
    assert.match(t, /Why the extract may carry this PII/);
    assert.match(t, /Acknowledge PII/);
    const denied = pii(realSystemPii, { canManage: false, manageDeniedReason: "Needs job.manage" });
    assert.match(denied, /disabled=""[^>]*title="Needs job.manage"|title="Needs job.manage"[^>]*disabled=""/);
    assert.match(text(pii(realSystemPii, { error: "Say why" })), /Say why/);
  });

  it("system: an acknowledged system names who, and can withdraw it; a system with neither shows nothing", () => {
    const t = text(pii(realSystemPiiAcked));
    assert.match(t, /Acknowledged by dpo@example\.com on .*DPA clause 9 allows the exit extract/);
    assert.match(t, /Withdraw acknowledgement/);
    assert.doesNotMatch(t, /Why the extract may carry this PII/);
    assert.equal(pii({ ...realSystemPii, readiness: { ...realSystemPii.readiness, state: "proven" }, pii_acknowledgement: null }), "");
  });

  it("rehearsal: a failed extract names its job and quotes the server log reference", () => {
    const opened = renderToStaticMarkup(
      <RehearsalView rehearsal={realFailed} busy={null} canSign onBack={noop} onRefresh={noop} onExportMarkdown={noop}
        onDownloadSigned={noop} onVerifySigned={noop} onVerifyFile={noop} onSign={noop} />,
    );
    const failed = realFailed.tables.find((x) => x.status === "failed")!;
    assert.match(failed.reason, /Reference [0-9a-f]+\./);
    const t = text(opened);
    assert.match(t, new RegExp(`The extract job ${failed.job_id} failed`));
    assert.match(t, /Reference/);
  });

  it("rehearsal: a verification that hit its limit says to rehearse again", () => {
    const timedOut = {
      ...realNotProven,
      tables: realNotProven.tables.map((x) => ({
        ...x, status: "incomplete" as const,
        reason: "The verification did not finish within its 6.0 h limit (the source or the stored part stopped answering or was too slow), so this table is not proven; rehearse it again.",
      })),
    };
    assert.match(text(run(timedOut)), /stopped at its time limit, so nothing was proven or disproven: rehearse again/);
  });
});

describe("contract 12: what the third review fixes changed on screen", () => {
  function pii(system: Parameters<typeof PiiReviewCard>[0]["system"]) {
    return text(renderToStaticMarkup(
      <PiiReviewCard system={system} reason="" onReasonChange={noop} busy={false} error="" canManage onAcknowledge={noop}
        onWithdraw={noop} />,
    ));
  }

  it("rehearsal: one that no longer proves the system's definition says so on its own page", () => {
    assert.equal(realStale.current, false);
    const t = text(run(realStale));
    assert.match(t, new RegExp(`${esc(STALE_TEXT)}\\. This rehearsal proved an earlier definition of Billing`));
    assert.match(t, /changed since this rehearsal/);
    assert.doesNotMatch(text(run(realPiiAcked)), new RegExp(esc(STALE_TEXT)));
  });

  it("rehearsal: the completeness chips count tables the PII gate held", () => {
    const t = text(run(realPiiHeld));
    assert.match(t, /1 held for PII review/);
    assert.match(t, /0 proven/);
  });

  it("system: the PII form lists the held columns and categories it would cover, never a value", () => {
    const t = pii(realSystemPii);
    assert.match(t, /The PII gate held rehearsal 1 on: people: email \(email\), ssn \(ssn\)/);
    assert.match(t, /Acknowledging covers exactly these columns and categories/);
    assert.doesNotMatch(t, /123-45-60|p1@example\.com/);
    const unheld = pii({ ...realSystemPii, pii_hold: null });
    assert.match(unheld, /Rehearse this definition first/);
    assert.doesNotMatch(unheld, /Acknowledge PII\b(?! for)/);
  });

  it("system: an acknowledgement names what it covers, and a stale one can still be withdrawn", () => {
    assert.match(pii(realSystemPiiAcked), /It covers: people: email \(email\), ssn \(ssn\)/);
    const ack = realSystemPiiAcked.pii_acknowledgement!;
    const stale = pii({ ...realSystemPiiAcked, pii_acknowledgement: { ...ack, current: false } });
    assert.match(stale, /\(an earlier definition\)/);
    assert.match(stale, /Withdraw acknowledgement/);
  });
});
