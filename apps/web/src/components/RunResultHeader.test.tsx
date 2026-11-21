/**
 * The Run step's finished view (P0-1, P1-15): one status, one verdict in the
 * product's words, a Trust Report link, nothing green the verification does
 * not support, and no pre-write "Run Execute" copy after the write.
 *
 * Run: npx tsx --test src/components/RunResultHeader.test.tsx
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { JobTheaterView } from "./JobTheater.tsx";
import { RunResultHeader } from "./RunResultHeader.tsx";
import { Gate8ProofCard } from "./transfer/Gate8ProofCard.tsx";
import { presentRunVerification } from "../lib/runResult.ts";
import type { JobProgress } from "../lib/types.ts";

const here = dirname(fileURLToPath(import.meta.url));

function job(over: Partial<JobProgress> & Record<string, unknown>): JobProgress {
  return {
    _id: "job-run-1",
    status: "completed",
    source_type: "csv",
    source_name: "orders.csv",
    destination_type: "postgresql",
    destination_collection: "orders",
    records_processed: 1200,
    total_rows: 1200,
    created_at: "2026-11-20T10:00:00Z",
    completed_at: "2026-11-20T10:01:00Z",
    row_accounting: {
      rows_read: 1200, rows_written: 1200, writer_ack: 1200, rows_quarantined: 0, rows_skipped: 0,
      conservation_kind: "unmeasured", rows_written_source: "unmeasured",
    },
    ...over,
  } as JobProgress;
}

function view(j: JobProgress): string {
  return renderToStaticMarkup(
    <JobTheaterView job={j} jobId={String(j._id)} throughput={0} log={[]} onCancel={() => {}} onNewTransfer={() => {}} />,
  );
}

describe("Run result: one verdict, no contradictions", () => {
  it("a completed run Gate-8 never checked reads Unverifiable, neutral, with a Trust Report link", () => {
    const html = view(job({}));
    assert.match(html, /Transfer completed/);
    assert.match(html, /<span class="df2-verdict-prefix">Verification:<\/span><span class="df2-verdict-word">Unverifiable<\/span>/);
    assert.match(html, /Gate-8 recorded no reconciliation for this job, so nothing was compared\./);
    assert.match(html, /href="#\/jobs\?jobId=job-run-1&amp;panel=trust-report"[^>]*>.*Open Trust Report/);
    assert.match(html, /class="df2-run-result is-neutral"/);
    // The old competing verdicts are gone.
    assert.doesNotMatch(html, /Finalized/i);
    assert.doesNotMatch(html, /<strong>Success<\/strong>/);
    assert.doesNotMatch(html, /Transfer complete<\/h3>/);
    assert.doesNotMatch(html, /Cancel is in the action bar below/);
    // Read = landed + held + skipped, without the writer's count posing as landed.
    assert.match(html, /Read 1,200 · Landed not counted · Held 0 · Skipped 0 · the writer reported 1,200/);
  });

  it("the trust score is never green when the verdict does not support it", () => {
    const html = view(job({
      row_accounting: {
        rows_read: 1200, rows_written: 1200, dest_count: 1200, rows_quarantined: 0, rows_skipped: 0,
        conservation_kind: "overwrite", rows_written_source: "dest_count", balanced: true,
      },
    }));
    const trust = html.match(/class="df2-trust-score ([^"]*)"/);
    assert.ok(trust, "trust score rendered");
    assert.doesNotMatch(trust[1], /is-ok/);
  });

  it("a held-rows run is amber and names the held rows", () => {
    const html = view(job({
      status: "completed_with_quarantine",
      rejected_rows: 34,
      row_accounting: {
        rows_read: 1200, rows_written: 1166, dest_count: 1166, rows_quarantined: 34, rows_skipped: 0,
        conservation_kind: "overwrite", rows_written_source: "dest_count", balanced: true,
      },
    }));
    assert.match(html, /Transfer completed with quarantine/);
    assert.match(html, /class="df2-run-result is-warning"/);
    assert.match(html, /Read 1,200 = landed 1,166 \+ held 34 \+ skipped 0/);
  });

  it("an unbalanced ledger never prints an equation: it lists the terms and names the unaccounted rows", () => {
    const html = view(job({
      row_accounting: {
        rows_read: 1200, rows_written: 1190, dest_count: 1190, rows_quarantined: 4, rows_skipped: 0, unaccounted: 6,
        conservation_kind: "overwrite", rows_written_source: "dest_readback", balanced: false,
      },
    }));
    assert.match(html, /Read 1,200 · Landed 1,190 · Held 4 · Skipped 0 · Does not balance: 6 unaccounted/);
    assert.match(html, /class="df2-run-chip is-danger"[^>]*>Does not balance: 6 unaccounted</);
    assert.doesNotMatch(html, /<span class="df2-run-op">=<\/span>/);
  });

  it("a mirror that kept retained keys prints read + retained = active + held + skipped", () => {
    const html = view(job({
      row_accounting: {
        rows_read: 100, active_count: 101, dest_count: 120, rows_quarantined: 2, rows_skipped: 0, unaccounted: 0,
        conservation_kind: "mirror", rows_written_source: "dest_active_readback", balanced: true,
      },
    }));
    assert.match(html, /Read 100 \+ retained 3 = active 101 \+ held 2 \+ skipped 0/);
    const ops = [...html.matchAll(/<span class="df2-run-op">([^<]*)<\/span>/g)].map((m) => m[1]);
    assert.deepEqual(ops, ["+", "=", "+", "+"]);
  });

  it("a failed run says verification never ran and keeps recovery primary", () => {
    const html = view(job({ status: "failed", error: "Destination refused the write", records_processed: 88 }));
    assert.match(html, /Transfer failed/);
    assert.match(html, /This run failed before verification ran/);
    assert.match(html, /class="df2-run-result is-danger"/);
    assert.match(html, /class="df2-btn df2-btn-secondary" href="#\/jobs\?jobId=job-run-1&amp;panel=trust-report"/);
  });

  it("green appears only for a proven or explained verdict", () => {
    const j = job({ reconciliation: { passed: true, source_rows: 1200, target_rows: 1200 } });
    // One colour per verdict (lib/verdict.ts): a clean sample is blue, never green.
    for (const [verdict, tone] of [["proven", "success"], ["explained", "success"], ["no_difference_found", "info"], ["unexplained", "danger"], ["unverifiable", "neutral"]] as const) {
      const load = {
        kind: "ready" as const,
        report: {
          verdict,
          verdict_reason: "reason",
          parity: { rows_source: 1, rows_destination: 1, rows_differ: 0, explained: 0, unexplained: 0, proven_equal_pct: null },
          confidence: { method: "full" as const, sampled_rows: 0, differences_in_sample: 0, level: 0.95, upper_bound_rate: null, statement: "" },
          evidence: { reconciliation_passed: verdict !== "unexplained" },
        },
      };
      const html = renderToStaticMarkup(
        <RunResultHeader jobId="j" job={j} verification={presentRunVerification(load, j)} load={load} onRetry={() => {}} />,
      );
      assert.match(html, new RegExp(`class="df2-verdict" data-tone="${tone}" data-verdict="${verdict}"`), verdict);
    }
  });

  it("a running job shows one status pill, a real phase label, and Cancel job", () => {
    const html = view(job({
      status: "running",
      phase: "load",
      completed_at: undefined,
      records_processed: 360,
      phases: [
        { name: "extract", status: "completed" },
        { name: "load", status: "running" },
        { name: "proof", status: "pending" },
      ] as unknown as JobProgress["phases"],
    }));
    assert.doesNotMatch(html, /df2-run-result/);
    assert.doesNotMatch(html, /df2-theater-v3-live-pill/);
    assert.match(html, /Running/);
    assert.doesNotMatch(html, /Queued —/);
    assert.match(html, /Load — 360 \/ 1,200 rows/);
    assert.match(html, />Proof</);
    assert.match(html, /class="df2-btn df2-run-cancel"[^>]*>.*Cancel job/);
  });
});

describe("Cancel job asks first", () => {
  it("uses the Jobs page confirm, with the same words, before calling the API", () => {
    const src = readFileSync(join(here, "JobTheater.tsx"), "utf8");
    const body = src.match(/const handleCancel = async \(\) => \{([\s\S]*?)\n  \};/);
    assert.ok(body, "handleCancel");
    const confirmAt = body[1].indexOf("await confirm(");
    const cancelAt = body[1].indexOf("await cancelJob(jobId)");
    assert.ok(confirmAt > 0 && cancelAt > confirmAt, "confirm before cancelJob");
    for (const words of ['title: "Cancel this job?"', 'confirmLabel: "Cancel job"', 'cancelLabel: "Keep running"', 'tone: "danger"']) {
      assert.ok(body[1].includes(words), words);
    }
    const jobs = readFileSync(join(here, "jobs", "useJobRunActions.ts"), "utf8");
    assert.ok(jobs.includes('message: "The job stops at the next checkpoint. Rows already committed stay in the destination."'));
    assert.ok(body[1].includes('message: "The job stops at the next checkpoint. Rows already committed stay in the destination."'));
  });
});

describe("Gate-8 after the write", () => {
  it("never asks to run Execute after Execute ran", () => {
    const report = { preview: true, source_rows: 5, target_rows: 5 };
    const after = renderToStaticMarkup(<Gate8ProofCard report={report} afterWrite />);
    assert.doesNotMatch(after, /Run Execute/);
    assert.doesNotMatch(after, /Pending Execute/);
    assert.doesNotMatch(after, /did not verify/);
    assert.match(after, /No post-write comparison recorded/);
    assert.match(after, /Open the Trust Report/);
    const before = renderToStaticMarkup(<Gate8ProofCard report={report} />);
    assert.match(before, /Pre-write simulation/);
  });
});
