/**
 * Contract-shaped Parity Studio payloads for the web tests. Typed against
 * lib/parity.ts, so a drift between the contract types and these fixtures is a
 * compile error, not a silently passing test. Not imported by app code.
 */
import type { ParityReport, ParitySummary } from "../../lib/parity";

export function provenReport(over: Partial<ParityReport> = {}): ParityReport {
  return {
    job_id: "job-31",
    generated_at: "2026-11-11T14:00:00Z",
    version: 1,
    verdict: "proven",
    verdict_reason: "Counts and checksums match on every table.",
    parity: { rows_source: 1204331, rows_destination: 1204331, rows_differ: 0, explained: 0, unexplained: 0, proven_equal_pct: 100 },
    confidence: {
      method: "full",
      sampled_rows: 0,
      differences_in_sample: 0,
      level: 0.95,
      upper_bound_rate: null,
      statement: "Counts and checksums cover every row.",
    },
    tables: [
      {
        name: "orders", check: "full_checksum", rows_source: 1000, rows_destination: 1000, proven_equal_pct: 100,
        differences: 0, explained: 0, unexplained: 0, status: "match", upper_bound_rate: null,
      },
    ],
    differences: [],
    explanation: null,
    fixes: [],
    rounds: [],
    signoff: {
      required_roles: ["engineering", "data_owner", "compliance"],
      approvals: [],
      status: "open",
      blocked_reason: null,
      signature: null,
      signed_at: null,
    },
    evidence: {
      reconciliation_passed: true,
      assurance_level: "full_checksum",
      trust_score: 96,
      protection_proof: null,
      transform_proof: null,
      reread: { orders: { status: "not_needed", keys: 0 } },
    },
    ...over,
  };
}

/** The mockup's run 31: 12 rows differ, 8 explained (trim), 4 unexplained (scale cut). */
export function unexplainedReport(over: Partial<ParityReport> = {}): ParityReport {
  return provenReport({
    version: 3,
    verdict: "unexplained",
    verdict_reason: "4 rows in payments.amount differ with no known cause.",
    parity: { rows_source: 1204331, rows_destination: 1204331, rows_differ: 12, explained: 8, unexplained: 4, proven_equal_pct: 99.97 },
    confidence: {
      method: "mixed",
      sampled_rows: 2000,
      differences_in_sample: 0,
      level: 0.95,
      upper_bound_rate: 0.0015,
      statement: "With 95% confidence at most 0.15% of customers rows differ in a way the sample would miss.",
    },
    tables: [
      {
        name: "orders", check: "full_checksum", rows_source: 1000, rows_destination: 1000, proven_equal_pct: 100,
        differences: 0, explained: 0, unexplained: 0, status: "match", upper_bound_rate: null,
      },
      {
        name: "customers", check: "sampled", rows_source: 5000, rows_destination: 5000, proven_equal_pct: 99.85,
        differences: 0, explained: 0, unexplained: 0, status: "match", upper_bound_rate: 0.0015,
      },
      {
        name: "payments", check: "key_diff", rows_source: 3000, rows_destination: 3000, proven_equal_pct: 99.6,
        differences: 12, explained: 8, unexplained: 4, status: "unexplained", upper_bound_rate: null,
      },
    ],
    differences: [
      {
        id: "d-note", table: "payments", column: "note", kind: "value", rows: 8, keys_sample: ["88001", "88002"],
        cause: { code: "transform_applied", label: "Your trim transform", explained: true, detail: "Trailing spaces removed by your trim transform. Expected." },
        example: { source: "paid ", destination: "paid" }, protected: false,
      },
      {
        id: "d-amount", table: "payments", column: "amount", kind: "value", rows: 4, keys_sample: ["88213", "88240", "90117", "90502"],
        cause: { code: "unexplained", label: "Unexplained", explained: false, detail: "Scale cut from 4 to 2 digits." },
        example: { source: "10.1049", destination: "10.10" }, protected: false,
      },
      {
        id: "d-ssn", table: "payments", column: "ssn", kind: "value", rows: 2, keys_sample: [],
        cause: { code: "concurrent_source_update", label: "Source changed during the run", explained: true, detail: "Updated in the source after it was read." },
        example: null, protected: true,
      },
    ],
    explanation: {
      text: "The 4 rows in payments.amount lost two decimal places.\n\nThe 8 note differences come from the trim step you configured.",
      source: "template",
      generated_at: "2026-11-11T14:01:00Z",
    },
    fixes: [
      {
        id: "fix-widen", difference_ids: ["d-amount"], action: "widen_column", title: "Widen payments.amount to NUMERIC(12,4)",
        detail: "Then re-sync the 4 affected keys with an upsert.",
        preview: "ALTER TABLE payments ALTER COLUMN amount TYPE NUMERIC(12,4);", applicable: true, not_applicable_reason: null,
        status: "proposed", approved_by: null, applied_at: null, result: null,
      },
      {
        id: "fix-resync", difference_ids: ["d-amount"], action: "resync_keys", title: "Re-sync 4 keys",
        detail: "Upsert the affected keys.", preview: "88213, 88240, 90117, 90502", applicable: false,
        not_applicable_reason: "The destination table has no primary key.",
        status: "proposed", approved_by: null, applied_at: null, result: null,
      },
    ],
    signoff: {
      required_roles: ["engineering", "data_owner", "compliance"],
      approvals: [],
      status: "blocked",
      blocked_reason: "4 differences are unexplained. Sign-off opens at zero.",
      signature: null,
      signed_at: null,
    },
    ...over,
  });
}

export function signedReport(over: Partial<ParityReport> = {}): ParityReport {
  return provenReport({
    version: 9,
    verdict: "explained",
    parity: { rows_source: 10, rows_destination: 10, rows_differ: 2, explained: 2, unexplained: 0, proven_equal_pct: 99.9 },
    signoff: {
      required_roles: ["engineering", "data_owner", "compliance"],
      approvals: [
        { role: "engineering", user: "A. Rao", at: "2026-11-11T14:02:00Z", note: "" },
        { role: "data_owner", user: "M. Chen", at: "2026-11-11T14:20:00Z", note: "Checked totals." },
        { role: "compliance", user: "J. Okafor", at: "2026-11-11T14:40:00Z", note: "" },
      ],
      status: "signed",
      blocked_reason: null,
      signature: "hmac-sha256:9f3c0d1e2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9a0b1ca71e",
      signed_at: "2026-11-11T14:40:00Z",
    },
    rounds: [{ n: 1, at: "2026-11-11T14:10:00Z", unexplained_before: 4, unexplained_after: 0, fix_id: "fix-widen" }],
    fixes: [
      {
        id: "fix-widen", difference_ids: ["d-amount"], action: "widen_column", title: "Widen payments.amount",
        detail: "Widened.", preview: null, applicable: true, not_applicable_reason: null, status: "applied",
        approved_by: "A. Rao", applied_at: "2026-11-11T14:10:00Z", result: "4 keys re-checked; 0 differ.",
      },
    ],
    ...over,
  });
}

export function summaryFixture(over: Partial<ParitySummary> = {}): ParitySummary {
  const runs = [100, 100, 99.8, 100, 100, 99.5, 100, null, 99.9, 100, 100, 99.94].map((pct, i) => ({
    job_id: `job-${20 + i}`,
    at: `2026-11-${String(i + 1).padStart(2, "0")}T00:00:00Z`,
    name: `orders_daily · run ${20 + i}`,
    proven_equal_pct: pct,
    unexplained: i === 5 ? 6 : i === 11 ? 1 : 0,
    verdict: (i === 5 || i === 11 ? "unexplained" : pct === null ? "unverifiable" : "proven") as ParitySummary["runs"][number]["verdict"],
    // Too little history for the first runs; run 25 is an outlier for its route.
    drift: i < 3 ? null : i === 5,
  }));
  return {
    generated_at: "2026-11-11T15:00:00Z",
    kpis: { runs: 12, fully_proven_pct: 96.4, runs_with_unexplained: 2, signoffs: 11, most_drifting_table: "payments" },
    runs,
    heatmap: {
      tables: ["orders", "payments"],
      job_ids: runs.map((r) => r.job_id),
      cells: [
        runs.map(() => "proven" as const),
        runs.map((_, i) => (i === 5 ? "unexplained_large" : i === 11 ? "unexplained_small" : i === 7 ? "none" : i === 2 ? "explained" : "proven")),
      ],
    },
    ...over,
  };
}
