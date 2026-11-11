/**
 * Reversibility Proof fixtures at the mockup's scale, for tests and the visual
 * harness: a nine-system board, a 412-table proven rehearsal (signed and
 * anchored, or not), a not-proven one with sampled tables, an incomplete one
 * with missing rows, a running one, and plan reversibility reports.
 * Shapes follow the contract (and realExitPayloads.ts, captured from the API).
 * Not imported by app code.
 */
import type {
  AnchorReceipt,
  ExitBoardRow,
  ExitReadiness,
  ExitRehearsal,
  ExitSystem,
  ExitVerifyResult,
  RehearsalTable,
  Trend,
} from "../../lib/exitReadiness";
import type { ReversibilityReport } from "../../lib/reversibility";

const H = 3600;
const D = 86400;
export const FIXTURE_NOW = new Date("2026-11-11T12:00:00Z");

function sha(seed: number): string {
  let out = "";
  let x = (seed * 2654435761) >>> 0;
  while (out.length < 64) {
    x = (x * 1103515245 + 12345) >>> 0;
    out += x.toString(16).padStart(8, "0");
  }
  return out.slice(0, 64);
}

function row(over: Partial<ExitBoardRow> & Pick<ExitBoardRow, "id" | "name">): ExitBoardRow {
  return {
    criticality: "important",
    engine: "postgresql",
    tables: 10,
    state: "proven",
    overdue: false,
    next_due_at: "2027-01-30T09:00:00Z",
    last_rehearsal_at: "2026-11-02T09:00:00Z",
    last_rehearsal_id: `exr_${over.id.slice(4)}`,
    measured_seconds: 6 * H,
    window_seconds: 30 * D,
    fits_window: true,
    completeness_label: `Proven, ${over.tables ?? 10} tables`,
    ...over,
  };
}

export function boardFixture(): ExitReadiness {
  const systems: ExitBoardRow[] = [
    row({ id: "exs_c0re1edger01", name: "Core ledger", criticality: "critical", engine: "oracle", tables: 412, measured_seconds: 61 * H, completeness_label: "Proven, 412 tables" }),
    row({
      id: "exs_payments0001", name: "Payments", criticality: "critical", engine: "sqlserver", tables: 88, overdue: true,
      last_rehearsal_at: "2026-08-19T09:00:00Z", next_due_at: "2026-10-18T09:00:00Z", measured_seconds: 28 * H, completeness_label: "Proven, 88 tables",
    }),
    row({
      id: "exs_crm000000001", name: "CRM", criticality: "standard", engine: null, tables: 31, state: "never_rehearsed",
      last_rehearsal_at: null, last_rehearsal_id: null, measured_seconds: null, fits_window: null, completeness_label: "Never rehearsed",
      next_due_at: "2026-12-20T09:00:00Z",
    }),
    row({
      id: "exs_warehouse001", name: "Data warehouse", criticality: "important", engine: "snowflake", tables: 146, state: "incomplete",
      measured_seconds: 40 * H, window_seconds: 36 * H, fits_window: false, completeness_label: "Incomplete",
    }),
    row({ id: "exs_hr0000000001", name: "HR records", criticality: "important", engine: "mysql", tables: 24, state: "running", measured_seconds: null, fits_window: null, completeness_label: "Rehearsal running" }),
    row({ id: "exs_billing00001", name: "Billing", criticality: "critical", engine: "postgresql", tables: 57, measured_seconds: 9 * H + 20 * 60, completeness_label: "Proven, 57 tables" }),
    row({ id: "exs_docs00000001", name: "Document store", criticality: "standard", engine: "mongodb", tables: 12, state: "not_proven", measured_seconds: 3 * H, completeness_label: "Not proven" }),
    row({ id: "exs_treasury0001", name: "Treasury", criticality: "critical", engine: "db2", tables: 64, measured_seconds: 14 * H, completeness_label: "Proven, 64 tables" }),
    row({
      id: "exs_loyalty00001", name: "Loyalty", criticality: "standard", engine: "postgresql", tables: 9, overdue: true, state: "failed",
      last_rehearsal_at: "2026-06-30T09:00:00Z", next_due_at: "2026-09-28T09:00:00Z", measured_seconds: null, fits_window: null, completeness_label: "Rehearsal failed",
    }),
  ];
  return {
    generated_at: FIXTURE_NOW.toISOString(),
    kpis: {
      systems: systems.length,
      systems_proven: systems.filter((s) => s.state === "proven").length,
      overdue: systems.filter((s) => s.overdue).length,
      longest_measured_seconds: 61 * H,
      longest_measured_system_id: "exs_c0re1edger01",
      next_regulatory_date: { date: "2027-01-12", label: "Data Act switching charges end", system_id: "exs_c0re1edger01" },
    },
    systems,
  };
}

export function systemFixture(over: Partial<ExitSystem> = {}): ExitSystem {
  return {
    id: "exs_c0re1edger01",
    workspace_id: "ws_ops",
    version: 3,
    name: "Core ledger",
    description: "General ledger and sub-ledgers on the hosted Oracle service.",
    criticality: "critical",
    source: { connector_id: "ora-fin", connector_name: "Oracle finance", engine: "oracle" },
    tables: [
      { name: "gl_entries", schema: "FIN", key_columns: ["entry_id"] },
      { name: "gl_accounts", schema: "FIN", key_columns: [] },
      { name: "ap_invoices", schema: "AP", key_columns: ["invoice_id", "line_no"] },
    ],
    exit_window_hours: 720,
    window_basis: "EU Data Act switching window",
    regulatory_dates: [{ date: "2027-01-12", label: "Data Act switching charges end" }],
    rehearsal_every_days: 90,
    rehearsal_schedule: { cron: "0 2 1 */3 *", timezone: "Europe/London" },
    extract_format: "jsonl",
    required_roles: ["engineering", "data_owner", "compliance"],
    running_rehearsal_id: null,
    created_at: "2026-05-01T09:00:00Z",
    created_by: "m.chen@example.com",
    updated_at: "2026-11-02T09:00:00Z",
    updated_by: "m.chen@example.com",
    readiness: {
      state: "proven",
      overdue: false,
      next_due_at: "2027-01-31T02:00:00Z",
      last_rehearsal: null,
      last_proven_rehearsal: null,
      ...over.readiness,
    },
    ...over,
  };
}

export function trendFixture(): Trend {
  const pts: Array<[number, number | null, number]> = [
    [1, 52 * H, 2.1e9], [2, 55 * H, 2.2e9], [3, null, 2.25e9], [4, 57 * H, 2.3e9], [5, 59 * H, 2.33e9], [6, 61 * H, 2.4e9],
  ];
  return {
    points: pts.map(([n, s, rows], i) => ({
      rehearsal_id: `exr_core0000000${n}`,
      n,
      at: new Date(Date.parse("2026-01-05T02:00:00Z") + i * 60 * D * 1000).toISOString(),
      measured_seconds: s,
      rows,
      bytes: rows * 1000,
      verdict: s == null ? "failed" : "proven",
    })),
    growth_pct_rows: 3,
    projection: {
      kind: "projection",
      seconds: 63 * H,
      basis: "Projection, not a measurement: rehearsal 6 measured 61 h for 2.4 billion rows; if the data grows again as it did since rehearsal 5, the next exit takes about 63 h.",
    },
  };
}

function provenTable(i: number, method: RehearsalTable["value_parity"]["method"]): RehearsalTable {
  const name = i === 0 ? "gl_entries" : i === 1 ? "gl_accounts" : `ledger_t${String(i).padStart(3, "0")}`;
  const rows = 1_000_000 + i * 7919;
  const keyed = i !== 1;
  const h = sha(i + 1);
  return {
    name,
    schema: "FIN",
    job_id: `6af4${String(i).padStart(20, "0")}`,
    status: method === "sampled" || method === "count_only" ? "not_proven" : "proven",
    reason:
      method === "sampled" || method === "count_only"
        ? `Nothing differs where measured, but values were checked on a sample only (${method === "sampled" ? "sampled" : "count only"}), which bounds nothing; this table is not proven.`
        : `Rows, key set, every value (by ${method === "full_checksum" ? "full checksum" : method === "key_diff" ? "key diff" : "population digest"}) and control totals match over all ${rows.toLocaleString("en-US")} row(s); the part verified on read.`,
    rows_source: rows,
    rows_extracted: rows,
    key_columns: keyed ? ["entry_id"] : [],
    key_set: keyed ? { source_sha256: h, extract_sha256: h, match: true } : { source_sha256: null, extract_sha256: null, match: null },
    row_set: { source_sha256: sha(i + 900), extract_sha256: sha(i + 900), match: true },
    control_totals: [
      { column: "amount", source: { sum: "48213004.25", min: "-1200.00", max: "998001.10" }, extract: { sum: "48213004.25", min: "-1200.00", max: "998001.10" }, match: true },
      { column: "entry_id", source: { sum: String(rows * 3), min: "1", max: String(rows) }, extract: { sum: String(rows * 3), min: "1", max: String(rows) }, match: true },
    ],
    value_parity: { method, parity_verdict: "unverifiable", unexplained: 0, report: `/api/v1/jobs/6af4${String(i).padStart(20, "0")}/parity-report` },
    differences: [],
    extract_seconds: 380 + i,
    part: {
      table: name,
      name: `exit_exr_core00000006_${String(i).padStart(4, "0")}_${name}.jsonl`,
      storage: "object_store",
      object_key: `exports/ws_ops/exit/exr_core00000006/${name}.jsonl`,
      size_bytes: 5_800_000_000 + i * 1_000_000,
      sha256: sha(i + 5000),
      verified_on_read: true,
      upload_parts: 56,
    },
  };
}

export const ANCHORED: AnchorReceipt = {
  id: "8b0b7a62-1c2f-4d7b-9a51-0e52c7f1d5aa",
  provider: "s3",
  kind: "exit_evidence",
  subject: "exit_evidence:exs_c0re1edger01:exr_core00000006",
  digest: sha(77),
  anchored_at: "2026-11-04T10:02:00Z",
  event_time: null,
  status: "anchored",
  anchored: true,
  location: { bucket: "datawrap-audit-anchors", key: `audit-anchors/exit_evidence/${sha(77)}.json`, version_id: "3HL4kqtJlcpXroDTDmJ" },
  retention: { mode: "COMPLIANCE", retain_until: "2033-11-03T10:02:00Z" },
  reason: null,
};

export const NOT_ANCHORED: AnchorReceipt = {
  ...ANCHORED,
  provider: "stub",
  status: "not_anchored",
  anchored: false,
  location: null,
  retention: null,
  reason: "the local stub is not a write-once store",
};

export const ANCHOR_FAILED: AnchorReceipt = {
  ...ANCHORED,
  status: "failed",
  anchored: false,
  retention: null,
  reason: "the bucket datawrap-audit-anchors does not have Object Lock enabled",
};

export function provenRehearsal(over: Partial<ExitRehearsal> = {}): ExitRehearsal {
  const methods: Array<RehearsalTable["value_parity"]["method"]> = [];
  for (let i = 0; i < 412; i += 1) methods.push(i < 290 ? "full_checksum" : i < 320 ? "key_diff" : "population_digest");
  const tables = methods.map((m, i) => provenTable(i, m));
  const parts = tables.map((t) => t.part!);
  const total = parts.reduce((s, p) => s + p.size_bytes, 0);
  return {
    id: "exr_core00000006",
    system_id: "exs_c0re1edger01",
    system_name: "Core ledger",
    workspace_id: "ws_ops",
    n: 6,
    version: 7,
    trigger: "schedule",
    actor: "scheduler",
    started_at: "2026-11-01T02:00:00Z",
    finished_at: "2026-11-03T15:00:00Z",
    verdict: "proven",
    verdict_reason: "Every one of the 412 declared table(s) was extracted and proven complete.",
    timeline: [
      { stage: "inventory", status: "done", started_at: "2026-11-01T02:00:00Z", finished_at: "2026-11-01T02:00:03Z", seconds: 3 },
      { stage: "extract", status: "done", started_at: "2026-11-01T02:00:03Z", finished_at: "2026-11-03T06:00:03Z", seconds: 52 * H },
      { stage: "verify", status: "done", started_at: "2026-11-03T06:00:03Z", finished_at: "2026-11-03T14:00:03Z", seconds: 8 * H },
      { stage: "seal", status: "done", started_at: "2026-11-03T14:00:03Z", finished_at: "2026-11-03T14:00:09Z", seconds: 6 },
      { stage: "sign", status: "pending", started_at: null, finished_at: null, seconds: null },
    ],
    completeness: {
      tables_declared: 412,
      tables_extracted: 412,
      tables_proven: 412,
      tables_not_proven: 0,
      tables_incomplete: 0,
      tables_failed: 0,
      value_methods: { full_checksum: 290, key_diff: 30, population_digest: 92, sampled: 0, count_only: 0, none: 0 },
      statement: "412 of 412 declared table(s) extracted. 412 proven complete (rows, key sets, every value and control totals). Value check: full checksum on 290, key diff on 30, population digest on 92.",
    },
    tables,
    seal: { algorithm: "sha256-merkle-rfc6962", merkle_root: `sha256:${sha(4242)}`, parts_count: parts.length, total_bytes: total, storage: "object_store", parts },
    duration: {
      basis: "measured",
      measured_seconds: 61 * H,
      extract_seconds: 52 * H,
      verify_seconds: 8 * H,
      window_seconds: 30 * D,
      fits_window: true,
      margin_pct: 91.5,
      statement: "Measured 61 h for 2.4 TB (extract 52 h, verify 8 h) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected.",
    },
    trend: trendFixture(),
    signoff: {
      required_roles: ["engineering", "data_owner", "compliance"],
      approvals: [
        { role: "engineering", user: "a.okafor@example.com", at: "2026-11-04T09:12:00Z", note: "Checked the 3 largest tables by hand.", signer_role: "admin", authority: "workspace admin", version: 7, content_sha256: sha(12) },
      ],
      invalidated_approvals: [],
      status: "open",
      blocked_reason: null,
      signature: null,
      signed_at: null,
    },
    evidence: { signed: false, content_sha256: null, anchor: null, export: "/api/v1/exit/rehearsals/exr_core00000006/evidence" },
    ...over,
  };
}

export function signedRehearsal(anchor: AnchorReceipt = ANCHORED): ExitRehearsal {
  const base = provenRehearsal();
  return {
    ...base,
    version: 10,
    timeline: base.timeline.map((s) => (s.stage === "sign" ? { ...s, status: "done", started_at: "2026-11-04T10:02:00Z", finished_at: "2026-11-04T10:02:00Z", seconds: 0 } : s)),
    signoff: {
      ...base.signoff,
      status: "signed",
      approvals: [
        ...base.signoff.approvals,
        { role: "data_owner", user: "r.silva@example.com", at: "2026-11-04T09:40:00Z", note: "", signer_role: "admin", authority: "workspace admin", version: 8, content_sha256: sha(12) },
        { role: "compliance", user: "j.weber@example.com", at: "2026-11-04T10:02:00Z", note: "Evidence for the DORA exit plan file.", signer_role: "admin", authority: "workspace admin", version: 9, content_sha256: sha(12) },
      ],
      signature: `hmac-sha256:${sha(99)}`,
      signed_at: "2026-11-04T10:02:00Z",
    },
    evidence: { signed: true, content_sha256: sha(77), anchor, export: "/api/v1/exit/rehearsals/exr_core00000006/evidence" },
  };
}

function sealOf(tables: RehearsalTable[], root: number): ExitRehearsal["seal"] {
  const parts = tables.map((t) => t.part).filter((p): p is NonNullable<RehearsalTable["part"]> => p != null);
  return { algorithm: "sha256-merkle-rfc6962", merkle_root: `sha256:${sha(root)}`, parts_count: parts.length, total_bytes: parts.reduce((s, p) => s + p.size_bytes, 0), storage: "object_store", parts };
}

export function notProvenRehearsal(): ExitRehearsal {
  const base = provenRehearsal();
  const tables = base.tables.slice(0, 40).map((t, i) => (i >= 26 ? provenTable(i, "sampled") : t));
  const sampled = tables.filter((t) => t.value_parity.method === "sampled").length;
  return {
    ...base,
    id: "exr_docs00000002",
    system_id: "exs_docs00000001",
    system_name: "Document store",
    n: 2,
    trigger: "manual",
    actor: "m.chen@example.com",
    verdict: "not_proven",
    verdict_reason: `${40 - sampled} of 40 table(s) proven; ${sampled} could not be proven in full (see each table's reason).`,
    tables,
    seal: sealOf(tables, 31),
    completeness: {
      tables_declared: 40,
      tables_extracted: 40,
      tables_proven: 40 - sampled,
      tables_not_proven: sampled,
      tables_incomplete: 0,
      tables_failed: 0,
      value_methods: { full_checksum: 26, sampled, key_diff: 0, population_digest: 0, count_only: 0, none: 0 },
      statement: `40 of 40 declared table(s) extracted. ${40 - sampled} proven complete (rows, key sets, every value and control totals). Value check: full checksum on 26, sampled on ${sampled}.`,
    },
    duration: { ...base.duration, measured_seconds: 3 * H, extract_seconds: 2 * H + 40 * 60, verify_seconds: 20 * 60, window_seconds: 30 * D, margin_pct: 99.6, statement: "Measured 3 h for 96 GB (extract 2 h 40 min, verify 20 min) against a window of 720.0 h: this exit fits the window. The figure is measured, not projected." },
    trend: { points: [], growth_pct_rows: null, projection: null },
    signoff: {
      ...base.signoff,
      approvals: [],
      status: "blocked",
      blocked_reason: `Only a proven rehearsal can be signed; this one is not proven: ${40 - sampled} of 40 table(s) proven; ${sampled} could not be proven in full (see each table's reason).`,
    },
  };
}

export function incompleteRehearsal(): ExitRehearsal {
  const base = provenRehearsal();
  const bad: RehearsalTable = {
    ...provenTable(0, "population_digest"),
    name: "fact_orders",
    status: "incomplete",
    reason: "The extract holds 1 row(s) fewer than the source.; Control totals of amount differ.",
    rows_source: 1_204_331,
    rows_extracted: 1_204_330,
    key_set: { source_sha256: sha(1), extract_sha256: sha(2), match: false },
    row_set: { source_sha256: sha(3), extract_sha256: sha(4), match: false },
    control_totals: [
      { column: "amount", source: { sum: "2287.50", min: "1.25", max: "75" }, extract: { sum: "2212.50", min: "1.25", max: "73.75" }, match: false },
      { column: "order_id", source: { sum: "1830", min: "1", max: "60" }, extract: { sum: "1830", min: "1", max: "60" }, match: true },
    ],
    differences: [
      { kind: "missing_rows", rows: 1, rows_at_least: true, detail: "The extract holds 1 row(s) fewer than the source." },
      { kind: "control_total", rows: null, rows_at_least: false, detail: "Control totals of amount differ." },
    ],
  };
  const tables = [bad, ...base.tables.slice(1, 6)];
  return {
    ...base,
    id: "exr_wh000000003",
    system_id: "exs_warehouse001",
    system_name: "Data warehouse",
    n: 3,
    trigger: "manual",
    actor: "m.chen@example.com",
    verdict: "incomplete",
    verdict_reason: "1 of 6 table(s) differ from the source: fact_orders.",
    tables,
    seal: sealOf(tables, 32),
    completeness: { tables_declared: 6, tables_extracted: 6, tables_proven: 5, tables_not_proven: 0, tables_incomplete: 1, tables_failed: 0, value_methods: { full_checksum: 5, population_digest: 1 }, statement: "6 of 6 declared table(s) extracted. 5 proven complete (rows, key sets, every value and control totals). Value check: full checksum on 5, population digest on 1." },
    duration: { ...base.duration, measured_seconds: 40 * H, extract_seconds: 34 * H, verify_seconds: 6 * H, window_seconds: 36 * H, fits_window: false, margin_pct: -11.1, statement: "Measured 40 h for 1.1 TB (extract 34 h, verify 6 h) against a window of 36.0 h: this exit does not fit the window. The figure is measured, not projected." },
    trend: { points: [], growth_pct_rows: null, projection: null },
    signoff: { ...base.signoff, approvals: [], status: "blocked", blocked_reason: "Only a proven rehearsal can be signed; this one is incomplete: 1 of 6 table(s) differ from the source: fact_orders." },
  };
}

/** As the API sends it while running: tables carry only the identity fields (see realRunningRaw). */
export function runningRehearsalRaw(): ExitRehearsal {
  const base = provenRehearsal();
  return {
    ...base,
    id: "exr_hr000000004",
    system_id: "exs_hr0000000001",
    system_name: "HR records",
    n: 4,
    trigger: "manual",
    actor: "m.chen@example.com",
    finished_at: null,
    verdict: "running",
    verdict_reason: "The rehearsal is still extracting or verifying.",
    timeline: [
      { stage: "inventory", status: "done", started_at: "2026-11-11T11:00:00Z", finished_at: "2026-11-11T11:00:02Z", seconds: 2 },
      { stage: "extract", status: "running", started_at: "2026-11-11T11:00:02Z", finished_at: null, seconds: null },
      { stage: "verify", status: "pending", started_at: null, finished_at: null, seconds: null },
      { stage: "seal", status: "pending", started_at: null, finished_at: null, seconds: null },
      { stage: "sign", status: "pending", started_at: null, finished_at: null, seconds: null },
    ],
    tables: ["employees", "payroll", "leave"].map((name, i) => ({ name, schema: null, job_id: `6af5${i}`, key_columns: ["id"], status: "running", reason: i === 0 ? "Verifying." : "Extracting (running)." }) as unknown as RehearsalTable),
    completeness: { tables_declared: 3, tables_extracted: 0, tables_proven: 0, tables_not_proven: 0, tables_incomplete: 0, tables_failed: 0, value_methods: {}, statement: "The rehearsal is still extracting or verifying." },
    seal: { algorithm: "sha256-merkle-rfc6962", merkle_root: null, parts_count: 0, total_bytes: 0, storage: null, parts: [] },
    duration: { basis: "measured", measured_seconds: null, extract_seconds: null, verify_seconds: null, window_seconds: 30 * D, fits_window: null, margin_pct: null, statement: "Not measured yet: the rehearsal is still running." },
    trend: { points: [], growth_pct_rows: null, projection: null },
    signoff: { required_roles: ["engineering", "data_owner"], approvals: [], invalidated_approvals: [], status: "blocked", blocked_reason: "The rehearsal is still running; sign-off opens when it is proven.", signature: null, signed_at: null },
    evidence: { signed: false, content_sha256: null, anchor: null, export: "/api/v1/exit/rehearsals/exr_hr000000004/evidence" },
  };
}

export function verifyFixture(over: Partial<ExitVerifyResult> = {}): ExitVerifyResult {
  return {
    ok: true,
    errors: [],
    kind: "exit_evidence",
    content_sha256: sha(77),
    signature_ok: true,
    merkle_ok: true,
    merkle_root: `sha256:${sha(4242)}`,
    anchor: { status: "anchored", digest: sha(77), provider: "s3", retention: { mode: "COMPLIANCE", retain_until: "2033-11-03T10:02:00Z" }, checked_at: "2026-11-11T12:00:00Z", reason: null },
    ...over,
  };
}

/** Page C at the mockup: orders reversible, customers not (hashed key), payments partly (narrowing). */
export function planReversibilityFixture(over: Partial<ReversibilityReport> = {}): ReversibilityReport {
  return {
    scope: "plan",
    subject_id: "plan_orders_cutover",
    generated_at: "2026-11-11T11:58:00Z",
    verdict: "partly_reversible",
    verdict_reason: "3 table(s): 6 column(s) invertible, 1 conditional and not shown to fit, 4 lossy.",
    require_reversibility: false,
    gate: { id: "g24_reversibility", status: "skip", message: "Reversibility is not required for this transfer." },
    data_checked: { rows: 25, source: "plan_sample" },
    counts: { invertible: 6, conditional: 1, lossy: 4 },
    lossy_columns: [
      { table: "customers", column: "customer_id", reason_code: "hash_pii", reason: "customer_id is hashed (hash_pii); the original value cannot be recovered from the destination." },
      { table: "customers", column: "email", reason_code: "hash_pii", reason: "email is hashed (hash_pii); the original value cannot be recovered from the destination." },
      { table: "customers", column: "phone", reason_code: "mask_pii", reason: "phone is masked (mask_pii); the masked characters cannot be recovered." },
      { table: "payments", column: "amount", reason_code: "narrowing", reason: "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 4 of 25 observed row(s) would lose data." },
    ],
    tables: [
      {
        name: "orders",
        verdict: "reversible",
        reason: "Every mapping is lossless: widened types, renamed columns, no transforms that drop information.",
        rollback: "restore_from_destination",
        rollback_note: "orders can be restored from the destination (every mapping is lossless). Rollback plan: DOCUMENT_ONLY runbook; Datawrap does not undo committed rows.",
        columns: [
          { source: "id", target: "id", source_type: "INTEGER", target_type: "BIGINT", transforms: [], invertibility: "invertible", reason_code: "widen", reason: "id widens INTEGER to BIGINT; every value fits.", check: null },
          { source: "cust", target: "customer_name", source_type: "VARCHAR(50)", target_type: "VARCHAR(200)", transforms: [], invertibility: "invertible", reason_code: "widen", reason: "cust widens VARCHAR(50) to VARCHAR(200); every value fits (renamed to customer_name).", check: null },
          { source: "placed", target: "placed", source_type: "DATE", target_type: "TIMESTAMP", transforms: [], invertibility: "invertible", reason_code: "lossless_cast", reason: "placed casts DATE to TIMESTAMP without losing information.", check: null },
          { source: "status", target: "status", source_type: "TEXT", target_type: "TEXT", transforms: ["trim"], invertibility: "conditional", reason_code: "normalize", reason: "status is normalised (trim); it is invertible only where the value already had no surrounding spaces. Fits the 25 observed row(s); not proven for rows not sampled.", check: { status: "fits", rows_checked: 25, rows_not_fitting: 0 } },
        ],
      },
      {
        name: "customers",
        verdict: "not_reversible",
        reason: "customer_id is hashed (hash_pii); the original value cannot be recovered from the destination; the key is lossy, so rows cannot be matched back.",
        rollback: "keep_source_as_fallback",
        rollback_note: "customers needs the source kept as the fallback until sign-off.",
        columns: [
          { source: "customer_id", target: "customer_id", source_type: "TEXT", target_type: "TEXT", transforms: ["hash_pii"], invertibility: "lossy", reason_code: "hash_pii", reason: "customer_id is hashed (hash_pii); the original value cannot be recovered from the destination.", check: null },
          { source: "email", target: "email", source_type: "TEXT", target_type: "TEXT", transforms: ["hash_pii"], invertibility: "lossy", reason_code: "hash_pii", reason: "email is hashed (hash_pii); the original value cannot be recovered from the destination.", check: null },
          { source: "phone", target: "phone", source_type: "TEXT", target_type: "TEXT", transforms: ["mask_pii"], invertibility: "lossy", reason_code: "mask_pii", reason: "phone is masked (mask_pii); the masked characters cannot be recovered.", check: null },
          { source: "country", target: "country", source_type: "CHAR(2)", target_type: "CHAR(2)", transforms: [], invertibility: "invertible", reason_code: "identity", reason: "country is copied unchanged.", check: null },
        ],
      },
      {
        name: "payments",
        verdict: "partly_reversible",
        reason: "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 4 of 25 observed row(s) would lose data.",
        rollback: "keep_source_as_fallback",
        rollback_note: "payments needs the source kept as the fallback until sign-off.",
        columns: [
          { source: "payment_id", target: "payment_id", source_type: "BIGINT", target_type: "BIGINT", transforms: [], invertibility: "invertible", reason_code: "identity", reason: "payment_id is copied unchanged.", check: null },
          { source: "amount", target: "amount", source_type: "NUMERIC(12,4)", target_type: "NUMERIC(12,2)", transforms: [], invertibility: "lossy", reason_code: "narrowing", reason: "amount narrows NUMERIC(12,4) to NUMERIC(12,2); values that do not fit lose data; 4 of 25 observed row(s) would lose data.", check: { status: "does_not_fit", rows_checked: 25, rows_not_fitting: 4 } },
          { source: "ref", target: "ref", source_type: "TEXT", target_type: "INTEGER", transforms: [], invertibility: "conditional", reason_code: "cast", reason: "ref casts TEXT to INTEGER; invertible only where the text round-trips (\"007\" does not). Not checked: no observed values.", check: { status: "not_checked", rows_checked: 0, rows_not_fitting: 0 } },
        ],
      },
    ],
    rollback_note: "The rollback plan for this cutover: orders can be restored from the destination; customers and payments need the source kept as the fallback until sign-off.",
    ...over,
  };
}

/** The same plan with "require reversibility" on: the gate blocks Execute. */
export function blockedReversibilityFixture(): ReversibilityReport {
  return planReversibilityFixture({
    require_reversibility: true,
    gate: {
      id: "g24_reversibility",
      status: "block",
      message: "This transfer must be reversible, and it is not: customers.customer_id, customers.email, customers.phone, payments.amount. Remove the lossy transform, widen the destination, or turn off require reversibility.",
    },
  });
}
