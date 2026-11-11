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
  downloadParitySignedPack,
  explainParityReport,
  fetchParityReport,
  fetchParitySummary,
  signParityReport,
  ApiError,
} from "./api.js";
import {
  causeChip,
  causeLabel,
  checkLabel,
  differenceTitle,
  differencesByTable,
  explanationIsStale,
  fixAttemptsUsed,
  formatParityPct,
  newerReport,
  parityErrorMessage,
  rereadLabel,
  roleLabel,
  shortSignature,
  signoffBlockedReason,
  signoffRows,
  tableParityLabel,
  tableStatusPresentation,
  unsignedRoles,
  verdictPresentation,
  parityIsLowerBound,
  heatCellLabel,
  trustScoreTone,
  assuranceTone,
  proofTone,
  shouldPollApplying,
  PARITY_APPLYING_POLL_LIMIT,
  approvalAuthority,
  invalidatedApprovalsNote,
  fixStatusPresentation,
  approveOutcome,
  signedDriftNote,
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
    // A clean sample is not a match of every row (review finding 9).
    assert.equal(tableStatusPresentation({ status: "match", unexplained: 0, check: "sampled" }).label, "No difference in sample");
    assert.equal(verdictPresentation(provenReport({ verdict: "no_difference_found" })).label, "No difference in sample");
    assert.equal(heatCellLabel("sampled"), "No difference in sample");
    // A re-sync runs as its own job (review finding 10): re-read while it applies, never forever.
    const applyingReport = { fixes: [{ ...unexplainedReport().fixes[0], status: "applying" as const }] };
    assert.equal(shouldPollApplying(applyingReport, 0), true);
    assert.equal(shouldPollApplying(applyingReport, PARITY_APPLYING_POLL_LIMIT), false);
    assert.equal(shouldPollApplying(unexplainedReport(), 0), false);
    assert.equal(shouldPollApplying(null, 0), false);
    // A claimed write fix (review finding 4) reads as in progress and counts against the limit.
    assert.equal(fixStatusPresentation("applying").label, "Applying…");
    assert.equal(
      fixAttemptsUsed({ fixes: [{ ...unexplainedReport().fixes[0], action: "resync_keys", status: "applying" }] }),
      1,
    );
    assert.equal(parityIsLowerBound(provenReport()), false);
    assert.equal(parityIsLowerBound(unexplainedReport()), true);
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

describe("evidence tones by value (review U1)", () => {
  it("trust score, assurance and proofs are green only when the value is good", () => {
    assert.equal(trustScoreTone(92), "ok");
    assert.equal(trustScoreTone(70), "warn");
    assert.equal(trustScoreTone(40), "danger");
    assert.equal(trustScoreTone(null), "muted");
    assert.equal(assuranceTone("full_checksum"), "ok");
    assert.equal(assuranceTone("sample"), "warn");
    assert.equal(assuranceTone("none"), "danger");
    assert.equal(assuranceTone(null), "muted");
    assert.equal(proofTone("verified"), "ok");
    assert.equal(proofTone("failed"), "danger");
    assert.equal(proofTone("skipped"), "warn");
    assert.equal(proofTone(null), "muted");
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

  it("counts applied and failed write fixes against the limit, re-runs included (they record no round)", () => {
    const base = unexplainedReport().fixes[0];
    const fixes = [
      { ...base, id: "f-rerun", action: "rerun" as const, status: "applied" as const },
      { ...base, id: "f-sync", action: "resync_keys" as const, status: "failed" as const },
      { ...base, id: "f-sync-2", action: "resync_keys" as const, status: "proposed" as const },
      { ...base, id: "f-acc", action: "accept" as const, status: "applied" as const },
      { ...base, id: "f-wid", action: "widen_column" as const, status: "dismissed" as const },
    ];
    assert.equal(fixAttemptsUsed(unexplainedReport({ fixes, rounds: [] })), 2);
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

  it("is blocked while the verdict is unverifiable, with the API's reason or its own", () => {
    const unverifiable = provenReport({
      verdict: "unverifiable",
      signoff: { ...provenReport().signoff, status: "blocked", blocked_reason: "The evidence cannot decide parity, so there is nothing to sign." },
    });
    assert.equal(signoffBlockedReason(unverifiable), "The evidence cannot decide parity, so there is nothing to sign.");
    unverifiable.signoff = { ...unverifiable.signoff, status: "open", blocked_reason: null };
    assert.match(signoffBlockedReason(unverifiable), /nothing to sign yet/);
  });

  it("names the authority each approval was signed with, and approvals that no longer count (review finding 5)", () => {
    assert.equal(approvalAuthority({ signer_role: "admin", authority: "workspace admin" }), "workspace admin");
    assert.equal(approvalAuthority({ signer_role: "editor" }), "workspace editor");
    assert.equal(approvalAuthority({}), "");
    const stale = { role: "engineering", user: "e@x.io", at: "2026-11-11T10:00:00Z", note: "", signer_role: "editor" };
    assert.equal(invalidatedApprovalsNote({ invalidated_approvals: [] }), "");
    assert.match(
      invalidatedApprovalsNote({ invalidated_approvals: [stale] }),
      /^1 approval \(Engineering\) attested an earlier version of this report and no longer counts; that role signs again\.$/,
    );
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

  it("keeps the workspace refusal's words and says who can approve", () => {
    const msg = parityErrorMessage(new ApiError("Workspace write access required", 403), "Could not complete this fix.");
    assert.match(msg, /^Workspace write access required\. /);
    assert.match(msg, /editor or admin access to this job's workspace/);
    assert.equal(parityErrorMessage(new ApiError("You need job.run.", 403), "x"), "You need job.run.");
    assert.match(
      parityErrorMessage(new ApiError("One person signs one role; ask another approver.", 409), "x"),
      /^One person signs one role; ask another approver\. The report has been refreshed/,
    );
  });
});

describe("explanation freshness", () => {
  it("is stale once a fix was decided or a round landed after it was written", () => {
    const base = unexplainedReport();
    const written = { ...base.explanation!, generated_at: "2026-11-11T14:00:00Z" };
    assert.equal(explanationIsStale({ ...base, explanation: written, rounds: [] }), false);
    const round = { n: 1, at: "2026-11-11T14:05:00Z", unexplained_before: 4, unexplained_after: 0, fix_id: "fix-widen" };
    assert.equal(explanationIsStale({ ...base, explanation: written, rounds: [round] }), true);
    const dismissed = { ...base.fixes[0], status: "dismissed" as const, decided_at: "2026-11-11T14:01:00Z" };
    assert.equal(explanationIsStale({ ...base, explanation: written, fixes: [dismissed], rounds: [] }), true);
    assert.equal(explanationIsStale({ ...base, explanation: null }), false);
  });
});

describe("re-read evidence", () => {
  it("sums the keys re-read, names a table whose re-read failed, and reads empty as not needed", () => {
    assert.deepEqual(rereadLabel({ pay: { status: "ok", keys: 4 } }), { value: "4 keys re-read", tone: "ok", detail: "pay: 4 keys re-read" });
    const failed = rereadLabel({ pay: { status: "failed", keys: 0, reason: "the source could not be re-read (OperationalError)" } });
    assert.equal(failed.value, "Failed for pay");
    assert.equal(failed.tone, "warn");
    assert.match(failed.detail, /OperationalError/);
    assert.equal(rereadLabel({}).value, "Not needed");
    assert.equal(rereadLabel({ a: { status: "not_needed", keys: 0 } }).value, "Not needed");
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
    // The version the person decided on travels with the decision (409 when it moved on).
    await approveParityFix("job-31", "fix-widen", "go", 7);
    await dismissParityFix("job-31", "fix-resync", undefined, 7);
    await signParityReport("job-31", "compliance", undefined, 7);
    assert.deepEqual(
      calls.map((c) => [c.method, c.url.replace(/^.*\/jobs\//, "/jobs/"), c.body]),
      [
        ["POST", "/jobs/job-31/parity-report/explain", "{}"],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-widen/approve", '{"note":"widen it"}'],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-resync/dismiss", "{}"],
        ["POST", "/jobs/job-31/parity-report/signoff", '{"role":"data_owner","note":"Checked totals."}'],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-widen/approve", '{"note":"go","expected_version":7}'],
        ["POST", "/jobs/job-31/parity-report/fixes/fix-resync/dismiss", '{"expected_version":7}'],
        ["POST", "/jobs/job-31/parity-report/signoff", '{"role":"compliance","expected_version":7}'],
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

  it("downloads the signed JSON pack, and keeps the 409 until it is signed", async () => {
    const calls = mockFetch(() => json({ content_sha256: "ab", signature: { alg: "HS256", key_id: "k", value: "cd" } }));
    const blob = await downloadParitySignedPack("job-31");
    assert.match(calls[0].url, /\/jobs\/job-31\/parity-report\/export\?format=json$/);
    assert.match(await blob.text(), /content_sha256/);
    mockFetch(() => json({ detail: "The report is not signed yet." }, 409));
    await assert.rejects(downloadParitySignedPack("job-31"), (err: unknown) => err instanceof ApiError && err.status === 409);
  });

  it("reads the workspace summary with a bounded limit", async () => {
    const calls = mockFetch(() => json(summaryFixture()));
    const summary = await fetchParitySummary(30);
    assert.equal(summary.runs.length, 12);
    assert.match(calls[0].url, /\/parity\/summary\?limit=30$/);
    await fetchParitySummary(10_000);
    assert.match(calls[1].url, /limit=100$/, "the route refuses more than 100 with a 422");
  });

  it("reports a missing job as a 404 ApiError", async () => {
    mockFetch(() => json({ detail: "Job not found" }, 404));
    await assert.rejects(fetchParityReport("nope"), (err: unknown) => err instanceof ApiError && err.status === 404);
  });
});

describe("approve outcome (re-review finding 7)", () => {
  it("toasts by the status the server returned for the fix, never success for a failed fix", () => {
    const base = unexplainedReport();
    const fix = base.fixes[0];
    const withStatus = (status: typeof fix.status, result: string | null = null) => ({
      ...base,
      fixes: [{ ...fix, status, result }],
    });
    const failed = approveOutcome(withStatus("failed", "the destination refused the ALTER"), fix.id);
    assert.equal(failed.tone, "error");
    assert.equal(failed.title, "The fix failed");
    assert.equal(failed.message, "the destination refused the ALTER");
    assert.equal(approveOutcome(withStatus("applying"), fix.id).tone, "info");
    assert.equal(approveOutcome(withStatus("applied", "Ran: ALTER"), fix.id).tone, "success");
    assert.equal(approveOutcome(null, fix.id).tone, "info");
  });
});

describe("signed report drift (re-review finding 13a)", () => {
  it("says when the data moved since signing, and stays quiet otherwise", () => {
    const signed = signedReport();
    assert.equal(signedDriftNote(signed), null);
    const moved = {
      ...signed,
      verdict: "unexplained" as const,
      signoff: { ...signed.signoff, signed_verdict: "proven" as const, drift_since_signing: true },
    };
    const note = signedDriftNote(moved) ?? "";
    assert.match(note, /changed since it was signed/);
    assert.match(note, /still says proven/);
    assert.match(note, /now reads unexplained/);
    assert.equal(signedDriftNote({ ...moved, signoff: { ...moved.signoff, status: "open" as const } }), null);
  });
});
