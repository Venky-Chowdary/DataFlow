/**
 * Parity Studio: presentation helpers and the API client, against the shared
 * contract (job trust report, fix loop, sign-off, export, workspace summary).
 */
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import {
  approveParityFix,
  dismissParityFix,
  downloadParityReportMarkdown,
  explainParityReport,
  fetchParityReport,
  fetchParitySummary,
  signParityReport,
  ApiError,
} from "./api.js";
import {
  awaitingRerun,
  causeChip,
  causeLabel,
  checkLabel,
  differenceTitle,
  differencesByTable,
  formatParityPct,
  newerReport,
  parityErrorMessage,
  roleLabel,
  shortSignature,
  signoffBlockedReason,
  signoffRows,
  tableParityLabel,
  tableStatusPresentation,
  unsignedRoles,
  verdictPresentation,
} from "./parity.js";
import { provenReport, signedReport, summaryFixture, unexplainedReport } from "../components/parity/parityFixtures.js";

describe("parity figures", () => {
  it("never rounds a percentage up to 100%", () => {
    assert.equal(formatParityPct(99.996), "99.99%");
    assert.equal(formatParityPct(99.97), "99.97%");
    assert.equal(formatParityPct(99.6), "99.6%");
    assert.equal(formatParityPct(87.25), "87.2%");
    assert.equal(formatParityPct(100), "100%");
    assert.equal(formatParityPct(0), "0%");
  });

  it("reads null as unknown, not zero", () => {
    assert.equal(formatParityPct(null), "Unknown");
    assert.equal(tableParityLabel({ check: "full_checksum", proven_equal_pct: null }), "Unknown");
  });

  it("states a sampled table as a lower bound", () => {
    assert.equal(tableParityLabel({ check: "sampled", proven_equal_pct: 99.85 }), "≥ 99.85%");
    assert.equal(tableParityLabel({ check: "key_diff", proven_equal_pct: 99.6 }), "99.6%");
    assert.equal(tableParityLabel({ check: "sampled", proven_equal_pct: 100 }), "100%");
  });
});

describe("verdicts, statuses and causes", () => {
  it("phrases each verdict with a tone", () => {
    assert.deepEqual(verdictPresentation(provenReport()), { label: "Proven equal", tone: "ok" });
    assert.deepEqual(verdictPresentation(unexplainedReport()), { label: "4 differences unexplained", tone: "warn" });
    assert.equal(verdictPresentation(provenReport({ verdict: "unverifiable" })).tone, "muted");
    assert.equal(
      verdictPresentation({ ...unexplainedReport(), parity: { ...unexplainedReport().parity, unexplained: 1 } }).label,
      "1 difference unexplained",
    );
  });

  it("labels table statuses like the mockup", () => {
    const payments = unexplainedReport().tables[2];
    assert.deepEqual(tableStatusPresentation(payments), { label: "4 to resolve", tone: "warn" });
    assert.equal(tableStatusPresentation({ status: "match", unexplained: 0 }).label, "Match");
    assert.equal(checkLabel("sampled"), "Count + sampled values");
    assert.equal(checkLabel("some_future_check"), "Some future check");
  });

  it("shows the API's label for a cause code the UI does not know", () => {
    const diff = unexplainedReport().differences.find((d) => d.id === "d-ssn")!;
    assert.equal(diff.cause.code, "concurrent_source_update");
    assert.equal(causeLabel(diff.cause), "Source changed during the run");
    assert.equal(causeLabel({ code: "out_of_scope_population", label: "" }), "Out of scope population");
    assert.deepEqual(causeChip(diff), { label: "Explained", tone: "ok" });
  });

  it("titles a difference by rows and column, or by kind when no column applies", () => {
    assert.equal(differenceTitle({ rows: 4, column: "amount", kind: "value" }), "4 rows · amount");
    assert.equal(differenceTitle({ rows: 1, column: null, kind: "missing_in_destination" }), "1 row · missing in destination");
  });

  it("groups differences by table, unexplained first", () => {
    const groups = differencesByTable(unexplainedReport());
    assert.equal(groups.length, 1);
    assert.equal(groups[0].table, "payments");
    assert.equal(groups[0].differences[0].id, "d-amount");
  });
});

describe("version-aware merge", () => {
  it("keeps the newer report when an older response arrives late", () => {
    const current = unexplainedReport({ version: 5 });
    assert.equal(newerReport(current, unexplainedReport({ version: 4 })), current);
    const incoming = unexplainedReport({ version: 6 });
    assert.equal(newerReport(current, incoming), incoming);
  });

  it("takes any report for a different job", () => {
    const other = provenReport({ job_id: "job-other", version: 1 });
    assert.equal(newerReport(unexplainedReport({ version: 9 }), other), other);
  });

  it("knows when a re-run fix is still finishing", () => {
    const fix = { ...unexplainedReport().fixes[0], id: "fix-rerun", action: "rerun" as const, status: "applied" as const };
    assert.equal(awaitingRerun(unexplainedReport({ fixes: [fix] })), true);
    assert.equal(
      awaitingRerun(unexplainedReport({ fixes: [fix], rounds: [{ n: 1, at: "", unexplained_before: 4, unexplained_after: 0, fix_id: "fix-rerun" }] })),
      false,
    );
  });
});

describe("sign-off", () => {
  it("lists every required role with its approval or nothing", () => {
    const rows = signoffRows(signedReport().signoff);
    assert.deepEqual(rows.map((r) => [r.label, r.approval?.user ?? null]), [
      ["Engineering", "A. Rao"],
      ["Data owner", "M. Chen"],
      ["Compliance", "J. Okafor"],
    ]);
    assert.deepEqual(unsignedRoles(unexplainedReport().signoff), ["engineering", "data_owner", "compliance"]);
    assert.equal(roleLabel("risk_officer"), "Risk officer");
  });

  it("explains a block from the API's reason, or from the unexplained count", () => {
    assert.match(signoffBlockedReason(unexplainedReport()), /4 differences are unexplained/);
    const noReason = unexplainedReport();
    noReason.signoff = { ...noReason.signoff, blocked_reason: null };
    assert.match(signoffBlockedReason(noReason), /4 differences still need a cause or a fix/);
    assert.equal(signoffBlockedReason(provenReport()), "");
    assert.equal(signoffBlockedReason(signedReport()), "");
  });

  it("shortens a signature for display and keeps its scheme", () => {
    assert.equal(shortSignature(signedReport().signoff.signature!), "hmac-sha256:9f3c…a71e");
  });
});

describe("error copy", () => {
  it("says what went wrong and what to do", () => {
    assert.match(parityErrorMessage(new ApiError("Job not found", 404), "x"), /not found in this workspace/);
    assert.match(parityErrorMessage(new ApiError("Fix already applied", 409), "x"), /^Fix already applied\. The report has been refreshed/);
    assert.match(parityErrorMessage(new Error("Request timed out"), "Could not load."), /Check your connection/);
    assert.match(parityErrorMessage(new ApiError("boom", 503), "Could not load."), /HTTP 503\)\. Try again/);
  });
});

/* ── API client with a mocked fetch ──────────────────────────── */

const realFetch = globalThis.fetch;
const realWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
type Call = { url: string; method: string; body: string; headers: Headers };

function mockFetch(respond: (call: Call) => Response): Call[] {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, dispatchEvent: () => true },
  });
  const calls: Call[] = [];
  globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
    const call = { url: String(url), method: String(init.method || "GET"), body: String(init.body ?? ""), headers: new Headers(init.headers) };
    calls.push(call);
    return respond(call);
  }) as typeof fetch;
  return calls;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), { status, headers: { "Content-Type": "application/json" } });
}

afterEach(() => {
  globalThis.fetch = realFetch;
  if (realWindow) Object.defineProperty(globalThis, "window", realWindow);
  else Reflect.deleteProperty(globalThis, "window");
});

describe("parity API client", () => {
  it("reads the report for an encoded job id", async () => {
    const calls = mockFetch(() => json(unexplainedReport()));
    const report = await fetchParityReport("job/31");
    assert.equal(report.verdict, "unexplained");
    assert.equal(calls[0].method, "GET");
    assert.match(calls[0].url, /\/jobs\/job%2F31\/parity-report$/);
  });

  it("posts explain, approve, dismiss and sign-off to the contract paths with JSON bodies", async () => {
    const calls = mockFetch(() => json(unexplainedReport({ version: 4 })));
    await explainParityReport("job-31");
    await approveParityFix("job-31", "fix-widen", "  widen it  ");
    await dismissParityFix("job-31", "fix-resync");
    await signParityReport("job-31", "data_owner", "Checked totals.");
    assert.deepEqual(
      calls.map((c) => [c.method, c.url.replace(/^.*\/jobs\//, "/jobs/"), c.body]),
      [
        ["POST", "/jobs/job-31/parity-report/explain", "{}"],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-widen/approve", '{"note":"widen it"}'],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-resync/dismiss", "{}"],
        ["POST", "/jobs/job-31/parity-report/signoff", '{"role":"data_owner","note":"Checked totals."}'],
      ],
    );
    for (const c of calls) assert.equal(c.headers.get("Content-Type"), "application/json");
  });

  it("keeps the status and the API's reason on a refusal", async () => {
    mockFetch(() => json({ detail: "Sign-off is blocked while 4 differences are unexplained." }, 409));
    await assert.rejects(signParityReport("job-31", "engineering"), (err: unknown) => {
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 409);
      assert.match(err.message, /blocked while 4 differences/);
      return true;
    });
  });

  it("downloads the Markdown export as a Blob", async () => {
    const calls = mockFetch(() => new Response("# Trust report\n", { status: 200, headers: { "Content-Type": "text/markdown" } }));
    const blob = await downloadParityReportMarkdown("job-31");
    assert.match(calls[0].url, /\/jobs\/job-31\/parity-report\/export\?format=markdown$/);
    assert.equal(await blob.text(), "# Trust report\n");
  });

  it("reads the workspace summary with a bounded limit", async () => {
    const calls = mockFetch(() => json(summaryFixture()));
    const summary = await fetchParitySummary(30);
    assert.equal(summary.runs.length, 12);
    assert.match(calls[0].url, /\/parity\/summary\?limit=30$/);
    await fetchParitySummary(10_000);
    assert.match(calls[1].url, /limit=200$/);
  });

  it("reports a missing job as a 404 ApiError", async () => {
    mockFetch(() => json({ detail: "Job not found" }, 404));
    await assert.rejects(fetchParityReport("nope"), (err: unknown) => err instanceof ApiError && err.status === 404);
  });
});
