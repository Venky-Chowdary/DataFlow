/**
 * Operator-visible fields the engine stores on the job or under
 * destination_summary. One reader — Jobs, Theater, the result dashboard,
 * and the SSE normalizer all use this. Do not grow a second health model.
 *
 * Stream order matches services.row_conservation.account_job: a non-empty
 * destination_summary.streams list is the run. job.streams is the fallback
 * (CDC checkpoints promote it before the summary list exists).
 */

import type { CdcStreamHealth, RejectedDetail } from "./types";

type SummaryCarrier = {
  streams?: unknown;
  rejected_rows?: unknown;
  coerced_null_rows?: unknown;
  rejected_details?: unknown;
  rejected_details_total?: unknown;
  warnings?: unknown;
  warnings_suppressed?: unknown;
  foreign_keys?: unknown;
  schema_fidelity?: unknown;
  identity_alignment?: unknown;
  multi_stream?: unknown;
  table?: unknown;
};

export type JobEvidenceCarrier = {
  streams?: unknown;
  /** Names lifted onto list payloads. Detail jobs use destination_summary.streams. */
  stream_names?: unknown;
  source_name?: string | null;
  source_type?: string | null;
  destination_database?: string | null;
  destination_collection?: string | null;
  destination_type?: string | null;
  rejected_rows?: unknown;
  coerced_null_rows?: unknown;
  rejected_details?: unknown;
  rejected_details_total?: unknown;
  destination_summary?: SummaryCarrier | null;
  mapping_proof?: unknown;
  transfer_request?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function summaryOf(job: JobEvidenceCarrier | null | undefined): SummaryCarrier | null {
  const ds = job?.destination_summary;
  return isRecord(ds) ? ds : null;
}

function finiteNumber(value: unknown): number | null {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function streamList(value: unknown): CdcStreamHealth[] {
  if (!Array.isArray(value)) return [];
  const out: CdcStreamHealth[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const name = String(item.name ?? item.stream ?? "").trim();
    if (!name) continue;
    const processed = finiteNumber(item.records_processed);
    const lag = finiteNumber(item.cdc_lag_seconds);
    const bytes = finiteNumber(item.replication_lag_bytes);
    const watermark = item.watermark == null ? null : String(item.watermark);
    const error = item.error == null || String(item.error).trim() === "" ? null : String(item.error);
    out.push({
      name,
      status: item.status == null ? undefined : String(item.status),
      records_processed: processed ?? undefined,
      cdc_lag_seconds: lag,
      replication_lag_bytes: bytes,
      watermark: watermark || null,
      error,
      row_accounting: isRecord(item.row_accounting)
        ? (item.row_accounting as CdcStreamHealth["row_accounting"])
        : undefined,
    });
  }
  return out;
}

/** Per-stream health for the run. Empty when neither place recorded a named stream. */
export function readJobStreams(job: JobEvidenceCarrier | null | undefined): CdcStreamHealth[] {
  const fromSummary = streamList(summaryOf(job)?.streams);
  if (fromSummary.length) return fromSummary;
  return streamList(job?.streams);
}

/**
 * Table names for this run, in engine order.
 *
 * Detail jobs read the stream health list. List payloads only carry
 * `stream_names` — the names, never the per-stream ledgers.
 */
export function readJobStreamNames(job: JobEvidenceCarrier | null | undefined): string[] {
  const fromHealth = readJobStreams(job).map((stream) => stream.name);
  if (fromHealth.length) return fromHealth;
  if (!Array.isArray(job?.stream_names)) return [];
  const names: string[] = [];
  for (const item of job.stream_names) {
    const name = String(item ?? "").trim();
    if (!name || names.includes(name)) continue;
    names.push(name);
  }
  return names;
}

/** Visible list of stream names. Extra names stay in the count, not an unbounded string. */
export function formatStreamNames(names: string[], max = 4): string {
  const clean = names.map((name) => name.trim()).filter(Boolean);
  if (clean.length <= max) return clean.join(", ");
  return `${clean.slice(0, max).join(", ")} +${clean.length - max}`;
}

function destRouteLabel(job: JobEvidenceCarrier): string {
  const database = String(job.destination_database || "").trim();
  const collection = String(job.destination_collection || "").trim();
  const paired = [database, collection].filter(Boolean).join(".");
  return paired || String(job.destination_type || "").trim() || "destination";
}

/**
 * Route an operator can trust.
 *
 * A multi-table job's stored source_name and destination_collection are the
 * restored endpoint (the last table). Naming only that table hides the rest.
 */
export function formatJobRoute(job: JobEvidenceCarrier | null | undefined): string {
  if (!job) return "source → destination";
  const names = readJobStreamNames(job);
  if (names.length >= 2) {
    const dest = String(job.destination_database || "").trim()
      || String(job.destination_type || "").trim()
      || "destination";
    return `${formatStreamNames(names)} → ${dest} (${names.length} tables)`;
  }
  const source = String(job.source_name || "").trim()
    || String(job.source_type || "").trim()
    || "source";
  return `${source} → ${destRouteLabel(job)}`;
}

/**
 * True when `name` is the auto title of the restored endpoint, not an
 * operator rename. Those titles name the last table of a multi-table job.
 */
export function isRestoredEndpointTitle(
  name: string,
  job: JobEvidenceCarrier | null | undefined,
): boolean {
  const folded = name.trim().toLowerCase();
  if (!folded || !job) return false;
  const source = String(job.source_name || "").trim();
  const collection = String(job.destination_collection || "").trim();
  const database = String(job.destination_database || "").trim();
  const candidates: string[] = [];
  if (source) candidates.push(source);
  for (const arrow of ["→", "->"]) {
    if (source && collection) candidates.push(`${source} ${arrow} ${collection}`);
    if (source && database && collection) candidates.push(`${source} ${arrow} ${database}.${collection}`);
    if (source && database) candidates.push(`${source} ${arrow} ${database}`);
  }
  return candidates.some((candidate) => candidate.toLowerCase() === folded);
}

function countFrom(top: unknown, nested: unknown): number {
  const topN = finiteNumber(top);
  if (topN != null) return topN;
  return finiteNumber(nested) ?? 0;
}

/** Quarantine count. Top-level wins when stamped; otherwise the summary (running checkpoints). */
export function readRejectedRows(job: JobEvidenceCarrier | null | undefined): number {
  return countFrom(job?.rejected_rows, summaryOf(job)?.rejected_rows);
}

export function readCoercedNullRows(job: JobEvidenceCarrier | null | undefined): number {
  return countFrom(job?.coerced_null_rows, summaryOf(job)?.coerced_null_rows);
}

/** Finding sample. Prefer a non-empty top-level list, then the summary sample. */
export function readRejectedDetails(
  job: JobEvidenceCarrier | null | undefined,
): RejectedDetail[] | undefined {
  if (Array.isArray(job?.rejected_details) && job.rejected_details.length) {
    return job.rejected_details as RejectedDetail[];
  }
  const nested = summaryOf(job)?.rejected_details;
  if (Array.isArray(nested) && nested.length) return nested as RejectedDetail[];
  if (Array.isArray(job?.rejected_details)) return job.rejected_details as RejectedDetail[];
  return undefined;
}

export function readRejectedDetailsTotal(job: JobEvidenceCarrier | null | undefined): number {
  const top = finiteNumber(job?.rejected_details_total);
  if (top != null) return top;
  const nested = finiteNumber(summaryOf(job)?.rejected_details_total);
  if (nested != null) return nested;
  return readRejectedDetails(job)?.length ?? 0;
}

export type WriterWarnings = {
  messages: string[];
  /** Engine dropped this many past the sample. They are not in `messages`. */
  suppressed: number;
};

/** Destination writer messages. They live on destination_summary, not the job root. */
export function readWriterWarnings(job: JobEvidenceCarrier | null | undefined): WriterWarnings {
  const summary = summaryOf(job);
  const raw = summary?.warnings;
  const messages = Array.isArray(raw)
    ? raw.map((item) => String(item).trim()).filter(Boolean)
    : [];
  const suppressed = finiteNumber(summary?.warnings_suppressed);
  return { messages, suppressed: suppressed != null && suppressed > 0 ? suppressed : 0 };
}

export type ForeignKeyDecisionView = {
  name: string;
  status: string;
  reason: string;
  destTable: string;
  referencedTable: string;
  integrityViolation: boolean;
};

export type ForeignKeyCarryView = {
  verdict: string;
  carried: number;
  integrityViolations: number;
  cycle: string[];
  /** Null when the job predates cycle_resolved — treat a cycle as unresolved. */
  cycleResolved: boolean | null;
  cycleNote: string;
  error: string;
  decisions: ForeignKeyDecisionView[];
};

/**
 * Post-load foreign-key carry (`services.foreign_key_orchestration.summarize`).
 * Returns null when the run did not record a carry.
 */
export function readForeignKeyCarry(job: JobEvidenceCarrier | null | undefined): ForeignKeyCarryView | null {
  const raw = summaryOf(job)?.foreign_keys;
  if (!isRecord(raw)) return null;
  const decisions: ForeignKeyDecisionView[] = [];
  if (Array.isArray(raw.decisions)) {
    for (const item of raw.decisions) {
      if (!isRecord(item)) continue;
      decisions.push({
        name: String(item.name || ""),
        status: String(item.status || "unknown"),
        reason: String(item.reason || item.source_detail || ""),
        destTable: String(item.dest_table || item.table || ""),
        referencedTable: String(item.referenced_table || ""),
        integrityViolation: item.integrity_violation === true,
      });
    }
  }
  const cycle = Array.isArray(raw.cycle)
    ? raw.cycle.map((name) => String(name).trim()).filter(Boolean)
    : [];
  const carried = finiteNumber(raw.carried) ?? decisions.filter((d) => d.status === "carried").length;
  const integrityViolations = finiteNumber(raw.integrity_violations)
    ?? decisions.filter((d) => d.integrityViolation).length;
  const verdict = String(raw.verdict || "");
  const error = String(raw.error || "");
  if (!verdict && !error && decisions.length === 0 && cycle.length === 0) return null;
  return {
    verdict,
    carried,
    integrityViolations,
    cycle,
    cycleResolved: raw.cycle_resolved == null ? null : Boolean(raw.cycle_resolved),
    cycleNote: String(raw.cycle_note || ""),
    error,
    decisions,
  };
}

/**
 * Decisions the operator must see. A measured table with no foreign keys is
 * `skipped` — that is a quiet result, same as `carried`. Unsupported, unknown,
 * and still-planned constraints are the findings.
 */
export function foreignKeyProblems(carry: ForeignKeyCarryView): ForeignKeyDecisionView[] {
  return carry.decisions.filter(
    (d) => d.integrityViolation || (d.status !== "carried" && d.status !== "skipped"),
  );
}

export type JobMappingRow = {
  stream: string;
  source: string;
  target: string;
  sourceType: string;
  targetType: string;
  confidence: number | null;
};

function mappingRows(value: unknown, stream = ""): JobMappingRow[] {
  if (!Array.isArray(value)) return [];
  const out: JobMappingRow[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const source = String(item.source ?? item.source_column ?? "").trim();
    const target = String(item.target ?? item.target_column ?? "").trim();
    if (!source && !target) continue;
    const named = String(item.stream ?? stream).trim();
    out.push({
      stream: named,
      source,
      target,
      sourceType: String(item.source_type ?? ""),
      targetType: String(item.target_type ?? item.dest_type ?? ""),
      confidence: finiteNumber(item.confidence),
    });
  }
  return out;
}

/**
 * Column map for this run. Proof rows win, then the top-level request map,
 * then each selected stream contract. Multi-table jobs leave the top-level
 * list empty and keep the map on the contract.
 */
export function readJobMappings(job: JobEvidenceCarrier | null | undefined): JobMappingRow[] {
  const proof = isRecord(job?.mapping_proof) ? mappingRows(job.mapping_proof.mappings) : [];
  if (proof.length) return proof;
  const request = isRecord(job?.transfer_request) ? job.transfer_request : null;
  const top = mappingRows(request?.mappings);
  if (top.length) return top;
  const contracts = request?.stream_contracts;
  if (!Array.isArray(contracts)) return [];
  const out: JobMappingRow[] = [];
  for (const contract of contracts) {
    if (!isRecord(contract) || contract.selected === false) continue;
    const name = String(contract.name ?? contract.stream ?? "").trim();
    out.push(...mappingRows(contract.mappings, name));
  }
  return out;
}

export type SchemaFidelityItem = {
  aspect: string;
  name: string;
  status: string;
  reason: string;
};

export type SchemaFidelityView = {
  carried: number;
  unsupported: number;
  unknown: number;
  skipped: number;
  items: SchemaFidelityItem[];
};

/**
 * Source constraint names the later ALTER carry proved.
 *
 * Create-new fidelity marks that foreign key unsupported because CREATE
 * TABLE cannot add it before the parent is loaded. A carried decision names
 * the source constraint in ``source_detail``. That later proof is the one
 * the warning list should follow.
 */
function carriedForeignKeyNames(summary: SummaryCarrier | null): Set<string> {
  const names = new Set<string>();
  const raw = summary?.foreign_keys;
  if (!isRecord(raw) || !Array.isArray(raw.decisions)) return names;
  for (const decision of raw.decisions) {
    if (!isRecord(decision) || decision.status !== "carried") continue;
    const detail = String(decision.source_detail || "");
    const head = detail.split(":")[0].trim().toLowerCase();
    if (head && head !== "*") names.add(head);
    const decisionName = String(decision.name || "").trim().toLowerCase();
    if (decisionName && decisionName !== "*") names.add(decisionName);
  }
  return names;
}

/** Create-new DDL fidelity. Carried and measured-absent rows are quiet. */
export function readSchemaFidelity(job: JobEvidenceCarrier | null | undefined): SchemaFidelityView | null {
  const summary = summaryOf(job);
  const raw = summary?.schema_fidelity;
  if (!isRecord(raw)) return null;
  const carriedKeys = carriedForeignKeyNames(summary);
  const items: SchemaFidelityItem[] = [];
  let quietedForeignKeys = 0;
  if (Array.isArray(raw.items)) {
    for (const item of raw.items) {
      if (!isRecord(item)) continue;
      const status = String(item.status || "");
      if (!status || status === "carried" || status === "skipped") continue;
      const aspect = String(item.aspect || "");
      const name = String(item.name || "");
      if (
        status === "unsupported"
        && aspect === "foreign_key"
        && carriedKeys.has(name.trim().toLowerCase())
      ) {
        quietedForeignKeys += 1;
        continue;
      }
      items.push({
        aspect,
        name,
        status,
        reason: String(item.reason || ""),
      });
    }
  }
  const carried = finiteNumber(raw.carried_count) ?? 0;
  const unsupported = Math.max(0, (finiteNumber(raw.unsupported_count) ?? 0) - quietedForeignKeys);
  const unknown = finiteNumber(raw.unknown_count) ?? 0;
  const skipped = finiteNumber(raw.skipped_count) ?? 0;
  if (!items.length && unsupported === 0 && unknown === 0) return null;
  return { carried, unsupported, unknown, skipped, items };
}

export type IdentityAlignmentView = {
  aligned: boolean | null;
  reason: string;
  writePassRows: number | null;
  rereadRows: number | null;
  /** True when this object is the last stream, not a job-wide digest. */
  lastStreamOnly: boolean;
  streamName: string;
};

/**
 * Write-pass vs independent re-read.
 *
 * `identity_hash_aligned: null` with reason `write_pass_not_fingerprinted`
 * is expected on a same-engine re-read. It is not a mismatch. On a
 * multi-table job the object is the last stream's alignment.
 */
export function readIdentityAlignment(
  job: JobEvidenceCarrier | null | undefined,
): IdentityAlignmentView | null {
  const summary = summaryOf(job);
  const raw = summary && isRecord(summary.identity_alignment) ? summary.identity_alignment : null;
  if (!raw) return null;
  const aligned = raw.identity_hash_aligned === true
    ? true
    : raw.identity_hash_aligned === false
      ? false
      : null;
  const reason = String(raw.reason || "").trim();
  const writePassRows = finiteNumber(raw.write_pass_rows);
  const rereadRows = finiteNumber(raw.reread_rows);
  if (aligned == null && !reason && writePassRows == null && rereadRows == null) return null;
  const names = readJobStreamNames(job);
  const multi = names.length >= 2 || summary?.multi_stream === true;
  const table = typeof summary?.table === "string" ? summary.table.trim() : "";
  const streamName = (table && names.includes(table) ? table : "")
    || names[names.length - 1]
    || table;
  return {
    aligned,
    reason,
    writePassRows,
    rereadRows,
    lastStreamOnly: multi,
    streamName: multi ? streamName : "",
  };
}

function rowPhrase(count: number | null): string {
  if (count == null) return "";
  return `${count.toLocaleString()} row${count === 1 ? "" : "s"}`;
}

export function identityAlignmentTone(view: IdentityAlignmentView): "ok" | "warn" | "muted" {
  if (view.aligned === false) return "warn";
  if (view.aligned === true) return "ok";
  return "muted";
}

/** One sentence. Does not invent a mismatch when the write pass was not fingerprinted. */
export function identityAlignmentSentence(view: IdentityAlignmentView): string {
  const where = view.lastStreamOnly
    ? `Last stream ${view.streamName || "table"}: `
    : "";
  const reread = rowPhrase(view.rereadRows);
  if (view.reason === "write_pass_not_fingerprinted") {
    const count = reread ? ` (${reread})` : "";
    const scope = view.lastStreamOnly
      ? " This is not a hash mismatch, and it is not a digest of the other tables."
      : " This is not a hash mismatch.";
    return `${where}The write pass was not fingerprinted. The independent re-read${count} owns the digest.${scope}`;
  }
  if (view.aligned === true) {
    const count = reread ? ` (${reread})` : "";
    return `${where}Identity hashes match the independent re-read${count}.`;
  }
  if (view.aligned === false) {
    const why = view.reason ? ` (${view.reason})` : "";
    const counts = [
      reread ? `re-read ${reread}` : "",
      view.writePassRows != null ? `write pass ${rowPhrase(view.writePassRows)}` : "",
    ].filter(Boolean).join("; ");
    return `${where}Identity hashes did not align${why}.${counts ? ` ${counts}.` : ""}`;
  }
  return `${where}Identity alignment is ${view.reason || "unmeasured"}. Not a measured mismatch.`;
}

const SINGLE_TABLE_REPLACE = "Destination will be cleared and fully replaced with source data.";
const EACH_TABLE_REPLACE = "Each selected table is cleared and fully replaced with that table's source rows.";

/**
 * Stored explanations were written from the restored endpoint, so a
 * multi-table job reads as one table. New runs already name every stream.
 * This only rewrites that stored opening. It does not invent columns.
 */
export function presentStoredExplanation(
  text: string | null | undefined,
  job: JobEvidenceCarrier | null | undefined,
): string {
  const raw = String(text || "").replace(/\r\n/g, "\n").trim();
  if (!raw) return "";
  const names = readJobStreamNames(job);
  if (names.length < 2) return raw;
  const first = raw.split("\n")[0] || "";
  const alreadyNamed = names.every((name) => first.includes(name));
  let next = raw;
  if (!alreadyNamed) {
    next = next.replace(/^Transfer:.*$/m, `Transfer: ${formatJobRoute(job)}`);
  }
  if (next.includes(SINGLE_TABLE_REPLACE)) {
    next = next.replace(SINGLE_TABLE_REPLACE, EACH_TABLE_REPLACE);
  }
  if (/^Source inferred/m.test(next) && !/last stream \(/i.test(next)) {
    const summary = summaryOf(job);
    const table = typeof summary?.table === "string" ? summary.table.trim() : "";
    const last = (table && names.includes(table) ? table : "") || names[names.length - 1];
    const note = `Column sample and schema mapping below are the last stream (${last}), not every table.`;
    next = next.replace(/^Source inferred/m, `${note}\nSource inferred`);
  }
  return next;
}

const EVENT_STAMP = /^(\d{1,2}:\d{2}:\d{2}(?:\s*[AP]M)?)\s*[—\-–]\s*(.*)$/i;

/**
 * Stored event lines were written before the multi-table scope was named.
 * The reconcile sentence used the job row total as if the checksum covered it.
 * New runs already say the digest is the last stream. This rewrites only
 * those stored sentences. It does not invent a table a line never named.
 */
export function presentStoredEventLog(
  lines: readonly string[] | null | undefined,
  job: JobEvidenceCarrier | null | undefined,
): string[] {
  const raw = Array.isArray(lines) ? lines.map((line) => String(line)) : [];
  const names = readJobStreamNames(job);
  if (names.length < 2 || raw.length === 0) return raw;
  const summary = summaryOf(job);
  const table = typeof summary?.table === "string" ? summary.table.trim() : "";
  const last = (table && names.includes(table) ? table : "") || names[names.length - 1];
  const tables = names.length;
  return raw.map((line) => rewriteStoredEventLine(line, last, tables));
}

function rewriteStoredEventLine(line: string, last: string, tables: number): string {
  const stamped = line.match(EVENT_STAMP);
  const body = stamped ? stamped[2] : line;
  const next = rewriteStoredEventBody(body, last, tables);
  if (next === body) return line;
  return stamped ? `${stamped[1]} — ${next}` : next;
}

function rewriteStoredEventBody(body: string, last: string, tables: number): string {
  const checksum = body.match(
    /^All rows written — reconciling destination \(([\d,]+) rows: counts \+ checksum proof\)…$/,
  );
  if (checksum) {
    return (
      `All rows written — reconciling destination (${checksum[1]} rows written across ${tables} tables). `
      + `Checksum proof is the last stream (${last}), not this total…`
    );
  }
  const pulse = body.match(
    /^Reconciling data \((\d+)s\) — verifying row counts and checksums for ([\d,]+) rows…$/,
  );
  if (pulse) {
    return (
      `Reconciling data (${pulse[1]}s) — ${pulse[2]} rows written across the job. `
      + `Checksum is the last stream (${last}), not the whole job…`
    );
  }
  const streaming = body.match(/^Streaming (.+) rows in batches…$/);
  if (streaming) {
    return `Streaming ${streaming[1]} rows on the restored endpoint, then each of ${tables} tables…`;
  }
  if (body === "Analyzing source table…") {
    return "Analyzing the restored endpoint…";
  }
  const batch = body.match(/^Writing batch (\d+\/\d+) \(([\d,]+) rows\)…$/);
  if (batch) {
    return `Writing batch ${batch[1]} (${batch[2]} rows) — one table's batch, not the job total…`;
  }
  return body;
}

const WROTE_ONE_TABLE = /^Wrote [\d,]+ rows on (.+)…$/;

/**
 * The load phase keeps the last status update. On a multi-table run that
 * update names one table. The sentence stays, and it says that table is not
 * the only one written.
 */
export function presentJobPhases<T extends { name?: string; message?: string }>(
  phases: readonly T[] | null | undefined,
  job: JobEvidenceCarrier | null | undefined,
): T[] {
  const list = Array.isArray(phases) ? [...phases] : [];
  if (readJobStreamNames(job).length < 2) return list;
  return list.map((phase) => {
    if (phase.name !== "load") return phase;
    const message = String(phase.message || "");
    const wrote = message.match(WROTE_ONE_TABLE);
    if (!wrote || /not the only table/i.test(message)) return phase;
    return {
      ...phase,
      message: `${message.replace(/…$/, "")} — last table updated, not the only table.`,
    };
  });
}

/** Last-stream re-read notes must not read as job proof. */
export function presentChecksumNote(
  note: unknown,
  job: JobEvidenceCarrier | null | undefined,
): string {
  const text = typeof note === "string" ? note.trim() : "";
  if (!text) return "";
  if (readJobStreamNames(job).length < 2) return text;
  if (/does not earn migration_proven for the job/i.test(text)) return text;
  if (!/full_checksum|migration_proven/i.test(text)) return text;
  return `${text} This note is the last stream. It does not earn migration_proven for the job.`;
}
