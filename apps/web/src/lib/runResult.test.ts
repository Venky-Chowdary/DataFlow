/**
 * Run: npx tsx --test src/lib/runResult.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  VERDICT_WORDS,
  presentRunStatus,
  presentRunVerification,
  runHeadlineTone,
  runReadStatement,
  verdictTone,
  type VerificationLoad,
} from "./runResult.js";
import type { ParityVerdict } from "./parity.js";

function ready(verdict: ParityVerdict, over: { reason?: string; passed?: boolean | null; method?: "full" | "sampled" | "mixed" | "none"; rows?: number | null } = {}): VerificationLoad {
  return {
    kind: "ready",
    report: {
      verdict,
      verdict_reason: over.reason ?? `reason for ${verdict}`,
      parity: { rows_source: over.rows === undefined ? 10 : over.rows, rows_destination: over.rows === undefined ? 10 : over.rows, rows_differ: 0, explained: 0, unexplained: 0, proven_equal_pct: null },
      confidence: { method: over.method ?? "full", sampled_rows: 0, differences_in_sample: 0, level: 0.95, upper_bound_rate: null, statement: "" },
      evidence: { reconciliation_passed: over.passed === undefined ? true : over.passed },
    },
  };
}

const RECON = { passed: true, source_rows: 10, target_rows: 10 };

describe("run result: one verdict in the product's five words", () => {
  it("names every verdict as docs/use/verification.md does", () => {
    assert.deepEqual(Object.values(VERDICT_WORDS), ["Proven", "Explained", "No difference found", "Unexplained", "Unverifiable"]);
  });

  it("allows green only for proven and explained", () => {
    assert.equal(verdictTone("proven"), "success");
    assert.equal(verdictTone("explained"), "success");
    assert.equal(verdictTone("no_difference_found"), "neutral");
    assert.equal(verdictTone("unverifiable"), "neutral");
    assert.equal(verdictTone("unexplained"), "danger");
    // Gate-8 failed without naming a row: still a failure, not a neutral "can't tell".
    assert.equal(verdictTone("unverifiable", false), "danger");
  });

  it("uses the Trust Report's verdict and reason when it loads", () => {
    const v = presentRunVerification(ready("explained", { reason: "9 row(s) differ and none is unexplained." }), { status: "completed", reconciliation: RECON });
    assert.equal(v.verdict, "explained");
    assert.equal(v.word, "Explained");
    assert.equal(v.reason, "9 row(s) differ and none is unexplained.");
    assert.equal(v.supportsSuccess, true);
  });

  it("says a failed run never reached verification, as the Trust Report does", () => {
    const v = presentRunVerification(
      ready("unverifiable", { reason: "Gate-8 recorded no reconciliation for this job, so nothing was compared.", passed: null, method: "none", rows: null }),
      { status: "failed" },
    );
    assert.equal(v.word, "Unverifiable");
    assert.equal(v.tone, "neutral");
    assert.match(v.reason, /This run failed before verification ran/);
    assert.equal(v.supportsSuccess, false);
  });

  it("without the report, states unverifiable only when Gate-8 recorded nothing (the API's own rule)", () => {
    for (const load of [{ kind: "loading" } as const, { kind: "error", message: "503" } as const]) {
      const none = presentRunVerification(load, { status: "completed" });
      assert.equal(none.verdict, "unverifiable");
      assert.match(none.reason, /Gate-8 recorded no reconciliation/);
      const empty = presentRunVerification(load, { status: "completed", reconciliation: {} });
      assert.equal(empty.verdict, "unverifiable");
      const cancelled = presentRunVerification(load, { status: "cancelled" });
      assert.match(cancelled.reason, /was cancelled before verification ran/);
    }
  });

  it("never guesses a verdict for a reconciled run whose report is loading or failed", () => {
    const loading = presentRunVerification({ kind: "loading" }, { status: "completed", reconciliation: RECON });
    assert.equal(loading.verdict, null);
    assert.equal(loading.word, "Checking…");
    assert.equal(loading.supportsSuccess, false);
    const failed = presentRunVerification({ kind: "error", message: "Could not load the trust report for this job." }, { status: "completed", reconciliation: RECON });
    assert.equal(failed.verdict, null);
    assert.equal(failed.word, "Not loaded");
    assert.equal(failed.tone, "neutral");
    assert.match(failed.reason, /could not be loaded \(Could not load the trust report for this job\)/);
  });
});

describe("run result: job status", () => {
  it("uses the shared status labels and keeps a finished job neutral", () => {
    assert.deepEqual(presentRunStatus("completed"), { label: "Completed", title: "Transfer completed", tone: "neutral" });
    assert.deepEqual(presentRunStatus("completed_with_quarantine"), { label: "Completed with quarantine", title: "Transfer completed with quarantine", tone: "warning" });
    assert.equal(presentRunStatus("failed").tone, "danger");
    assert.equal(presentRunStatus("cancelled").title, "Transfer cancelled");
  });

  it("colours the header green only when the verdict supports it", () => {
    const done = presentRunStatus("completed");
    const held = presentRunStatus("completed_with_quarantine");
    const failed = presentRunStatus("failed");
    const proven = presentRunVerification(ready("proven"), { status: "completed", reconciliation: RECON });
    const unverifiable = presentRunVerification({ kind: "error", message: "" }, { status: "completed" });
    const unexplained = presentRunVerification(ready("unexplained"), { status: "completed", reconciliation: RECON });
    assert.equal(runHeadlineTone(done, proven), "success");
    assert.equal(runHeadlineTone(done, unverifiable), "neutral");
    assert.equal(runHeadlineTone(done, unexplained), "danger");
    assert.equal(runHeadlineTone(held, proven), "warning");
    assert.equal(runHeadlineTone(failed, unverifiable), "danger");
  });
});

describe("run result: read = landed + held + skipped", () => {
  it("closes the identity from engine numbers when the destination was counted", () => {
    const s = runReadStatement({
      status: "completed_with_quarantine",
      row_accounting: { rows_read: 1200, rows_written: 1160, dest_count: 1160, rows_quarantined: 34, rows_skipped: 6, conservation_kind: "overwrite", rows_written_source: "dest_count", balanced: true },
    });
    assert.ok(s);
    assert.equal(s.measured, true);
    assert.equal(s.identity, true);
    assert.equal(s.text, "Read 1,200 = landed 1,160 + held 34 + skipped 6");
    assert.equal(s.terms.find((t) => t.label === "Held")?.tone, "warning");
    assert.equal(s.writerReported, null);
  });

  it("never uses the writer's count as the landed figure", () => {
    const s = runReadStatement({
      status: "completed",
      records_processed: 1200,
      row_accounting: { rows_read: 1200, rows_written: 1200, writer_ack: 1200, conservation_kind: "unmeasured", rows_written_source: "unmeasured" },
    });
    assert.ok(s);
    assert.equal(s.measured, false);
    assert.equal(s.identity, false);
    assert.equal(s.terms[1].value, "not counted");
    assert.equal(s.writerReported, 1200);
    assert.match(s.text, /Landed not counted/);
    assert.match(s.text, /the writer reported 1,200/);
  });

  it("names rows removed on read so a filtered run does not read as loss", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 100, dest_count: 90, rows_quarantined: 0, rows_skipped: 0, rows_source_filtered: 10, conservation_kind: "overwrite", rows_written_source: "dest_count", balanced: true },
    });
    assert.equal(s?.text, "Read 100 = landed 90 + held 0 + skipped 0 + removed on read 10");
  });

  it("lists keyed upserts without claiming they close as read = landed", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 50, dest_delta: 20, inserts: 20, deletes: 0, dest_count: 900, rows_quarantined: 0, rows_skipped: 0, conservation_kind: "keyed", rows_written_source: "dest_count", balanced: true },
    });
    assert.equal(s?.identity, false);
    assert.equal(s?.text, "Read 50 · Landed 20 · Held 0 · Skipped 0");
  });

  it("falls back to the job's own counts when no ledger was stamped", () => {
    const s = runReadStatement({ status: "failed", records_processed: 88, total_rows: 150, rejected_rows: 0 });
    assert.equal(s?.measured, false);
    assert.equal(s?.writerReported, 88);
    assert.equal(runReadStatement({}), null);
  });
});
