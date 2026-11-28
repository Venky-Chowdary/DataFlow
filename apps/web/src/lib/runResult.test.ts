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

  it("says verification is still running when Parity at Scale reports verifying (train 2)", () => {
    // p3's API states "verifying" after the write while every row is compared; it is
    // not one of the five verdicts and never earns a success colour.
    const v = presentRunVerification(ready("verifying", { reason: "Verification is running for orders." }), { status: "completed", reconciliation: RECON });
    assert.equal(v.verdict, "verifying");
    assert.equal(v.word, "Verification running");
    assert.equal(v.tone, "neutral");
    assert.equal(v.supportsSuccess, false);
    assert.ok(!Object.values(VERDICT_WORDS).includes(v.word));
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

  it("prints the keyed identity the ledger closes on: dest change = new keys − deletes", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 50, dest_delta: 20, inserts: 25, deletes: 5, updates: 30, dest_count: 900, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "keyed", rows_written_source: "dest_readback", balanced: true },
    });
    assert.equal(s?.identity, true);
    assert.equal(s?.balance, "balanced");
    assert.equal(s?.text, "Dest change 20 = new keys 25 − deletes 5 · Read 50 · Updated 30 · Held 0 · Skipped 0");
    assert.deepEqual(s?.terms.map((t) => t.op ?? ""), ["", "=", "−", "·", "·", "·", "·"]);
  });

  it("never prints a keyed equation the numbers do not satisfy", () => {
    // Balanced on a term the payload does not carry (an earlier pass's inserts): no "=".
    const s = runReadStatement({
      row_accounting: { rows_read: 50, dest_delta: 22, inserts: 20, deletes: 0, dest_count: 900, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "keyed", rows_written_source: "dest_readback", balanced: true },
    });
    assert.equal(s?.identity, false);
    assert.ok(s?.terms.every((t) => t.op === undefined || t.op === "·"));
    assert.match(s?.balanceText ?? "", /balances on terms this summary does not show/);
  });

  it("says a keyed ledger is not proven when it could not close", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 50, dest_delta: 18, inserts: 20, deletes: 0, dest_count: 900, rows_quarantined: 2, rows_skipped: 0, unaccounted: null, conservation_kind: "keyed", rows_written_source: "dest_readback", balanced: false },
    });
    assert.equal(s?.identity, false);
    assert.equal(s?.balance, "open");
    assert.match(s?.text ?? "", /Not proven/);
    assert.doesNotMatch(s?.text ?? "", /=/);
  });
  it("falls back to the job's own counts when no ledger was stamped", () => {
    const s = runReadStatement({ status: "failed", records_processed: 88, total_rows: 150, rejected_rows: 0 });
    assert.equal(s?.measured, false);
    assert.equal(s?.writerReported, 88);
    assert.equal(runReadStatement({}), null);
  });
});

describe("run result: the identity follows the ledger, never a false equation", () => {
  it("shows an unbalanced overwrite as a list with its unaccounted rows", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 1000, dest_count: 990, rows_quarantined: 4, rows_skipped: 0, unaccounted: 6, conservation_kind: "overwrite", rows_written_source: "dest_readback", balanced: false },
    });
    assert.ok(s);
    assert.equal(s.identity, false);
    assert.equal(s.balance, "unbalanced");
    assert.equal(s.unaccounted, 6);
    assert.equal(s.balanceText, "Does not balance: 6 unaccounted");
    assert.equal(s.text, "Read 1,000 · Landed 990 · Held 4 · Skipped 0 · Does not balance: 6 unaccounted");
    assert.doesNotMatch(s.text, /=/);
  });

  it("names extra destination rows when the ledger over-accounts", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 100, dest_count: 103, rows_quarantined: 0, rows_skipped: 0, unaccounted: -3, conservation_kind: "overwrite", rows_written_source: "dest_readback", balanced: false },
    });
    assert.equal(s?.balanceText, "Does not balance: 3 more in the destination than were read");
  });

  it("does not print an equation when the ledger says balanced but the shown numbers do not add up", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 100, dest_count: 97, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "overwrite", rows_written_source: "dest_readback", balanced: true },
    });
    assert.equal(s?.identity, false);
    assert.doesNotMatch(s?.text ?? "", /=/);
  });

  it("includes retained keys in a balanced mirror identity (read + retained = active + held + skipped)", () => {
    // API: unaccounted = read + retained − (active + held + skipped + removed) = 100 + 3 − 103 = 0.
    const s = runReadStatement({
      row_accounting: { rows_read: 100, active_count: 101, dest_count: 120, rows_quarantined: 2, rows_skipped: 0, unaccounted: 0, conservation_kind: "mirror", rows_written_source: "dest_active_readback", balanced: true },
    });
    assert.ok(s);
    assert.equal(s.identity, true);
    assert.equal(s.text, "Read 100 + retained 3 = active 101 + held 2 + skipped 0");
    assert.deepEqual(s.terms.map((t) => t.op ?? ""), ["", "+", "=", "+", "+"]);
  });

  it("keeps the retained term in an unbalanced mirror and shows what is unaccounted", () => {
    // retained 3, 4 rows lost: unaccounted = 100 + 3 − (97 + 2 + 0) = 4.
    const s = runReadStatement({
      row_accounting: { rows_read: 100, active_count: 97, rows_quarantined: 2, rows_skipped: 0, unaccounted: 4, conservation_kind: "mirror", rows_written_source: "dest_active_readback", balanced: false },
    });
    assert.equal(s?.identity, false);
    assert.equal(s?.text, "Read 100 · Retained 3 · Active 97 · Held 2 · Skipped 0 · Does not balance: 4 unaccounted");
  });

  it("prints a mirror without retained rows as read = active + held + skipped", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 10, active_count: 10, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "mirror", rows_written_source: "dest_active_readback", balanced: true },
    });
    assert.equal(s?.text, "Read 10 = active 10 + held 0 + skipped 0");
  });

  it("includes retained keys in an SCD2 current-row identity", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 50, rows_written: 55, dest_count: 55, current_count: 55, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "scd2", rows_written_source: "current_readback", balanced: true },
    });
    assert.equal(s?.identity, true);
    assert.equal(s?.text, "Read 50 + retained 5 = current 55 + held 0 + skipped 0");
  });

  it("adds the recipe's expanded rows to the read side", () => {
    const s = runReadStatement({
      row_accounting: { rows_read: 10, rows_expanded: 5, dest_count: 15, rows_quarantined: 0, rows_skipped: 0, unaccounted: 0, conservation_kind: "overwrite", rows_written_source: "dest_readback", balanced: true },
    });
    assert.equal(s?.text, "Read 10 + added by recipe 5 = landed 15 + held 0 + skipped 0");
  });

  it("closes a summable multi-stream job on the summed counts", () => {
    const s = runReadStatement({
      row_accounting: {
        rows_read: 300, rows_written: 295, dest_count: 295, rows_quarantined: 5, rows_skipped: 0, unaccounted: 0,
        conservation_kind: "job_rollup", rows_written_source: "dest_readback", balanced: true, summable: true, stream_count: 2, measured_streams: 2,
        per_stream: [
          { stream: "a", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 200, rows_read: 200 },
          { stream: "b", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 95, rows_read: 100 },
        ],
      },
    });
    assert.equal(s?.identity, true);
    assert.equal(s?.text, "Read 300 = landed 295 + held 5 + skipped 0");
  });

  it("does not sum a mixed multi-stream job and says each stream balances", () => {
    const s = runReadStatement({
      row_accounting: {
        rows_read: null, rows_written: null, rows_quarantined: 0, rows_skipped: 0, unaccounted: null,
        conservation_kind: "job_rollup", rows_written_source: "per_stream", balanced: true, summable: false, stream_count: 2, measured_streams: 2,
        per_stream: [
          { stream: "a", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 200, rows_read: 200 },
          { stream: "b", measured: true, balanced: true, conservation_kind: "keyed", dest_count: 95, rows_read: 100 },
        ],
      },
    });
    assert.equal(s?.identity, false);
    assert.equal(s?.balance, "balanced");
    assert.match(s?.balanceText ?? "", /Every one of the 2 streams balances/);
    assert.doesNotMatch(s?.text ?? "", /=/);
  });

  it("says how many streams of a multi-stream job do not close", () => {
    const s = runReadStatement({
      row_accounting: {
        rows_read: 300, rows_written: 290, dest_count: 290, rows_quarantined: 5, rows_skipped: 0, unaccounted: 5,
        conservation_kind: "job_rollup", rows_written_source: "dest_readback", balanced: false, summable: true, stream_count: 3, measured_streams: 3,
        per_stream: [
          { stream: "a", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 100, rows_read: 100 },
          { stream: "b", measured: true, balanced: false, conservation_kind: "overwrite", dest_count: 90, rows_read: 100 },
          { stream: "c", measured: true, balanced: true, conservation_kind: "overwrite", dest_count: 100, rows_read: 100 },
        ],
      },
    });
    assert.equal(s?.identity, false);
    assert.equal(s?.balance, "unbalanced");
    assert.equal(s?.balanceText, "Does not balance: 5 unaccounted");
  });

  it("reports an open multi-stream job by its streams when no total is reported", () => {
    const s = runReadStatement({
      row_accounting: {
        rows_read: null, rows_quarantined: 0, rows_skipped: 0, unaccounted: null,
        conservation_kind: "job_rollup", rows_written_source: "per_stream", balanced: false, summable: false, stream_count: 2, measured_streams: 2,
        per_stream: [
          { stream: "a", measured: true, balanced: false, conservation_kind: "overwrite", dest_count: 90, rows_read: 100 },
          { stream: "b", measured: true, balanced: true, conservation_kind: "keyed", dest_count: 95, rows_read: 100 },
        ],
      },
    });
    assert.equal(s?.balanceText, "Does not balance: 1 of 2 streams do not close.");
  });
});
