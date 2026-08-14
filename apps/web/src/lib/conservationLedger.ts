/**
 * Display-only view of the engine conservation ledger.
 *
 * The identity lives in apps/api/services/row_conservation.py.
 * TypeScript must never close dest COUNT(*) with writer ack — that is the
 * AWS DMS Full Load / MISSING_TARGET lie this product refuses.
 */

export type ConservationLedger = {
  rows_read: number | null;
  rows_written: number | null;
  rows_quarantined: number;
  rows_skipped: number;
  rows_coerced_null: number;
  writer_ack: number | null;
  dest_count: number | null;
  dest_count_before: number | null;
  unaccounted: number | null;
  balanced: boolean;
  rows_read_source: string;
  rows_written_source: string;
  conservation_kind: string;
  note: string;
  writer_ack_delta: number | null;
  inserts: number | null;
  updates: number | null;
  deletes: number | null;
  dest_delta: number | null;
  unique_batch_keys: number | null;
  dest_preexisting: number | null;
  active_count: number | null;
  inferred_deletes: number | null;
  reactivated: number | null;
  events_read: number | null;
  identity_count: number | null;
  vector_rows: number | null;
  current_count: number | null;
  history_rows: number | null;
  missing_keys: number | null;
  extra_keys: number | null;
  leftover_deleted: number | null;
  stream_count: number | null;
  measured_streams: number | null;
  summable: boolean | null;
  per_stream: StreamLedgerSlice[] | null;
};

export type StreamLedgerSlice = {
  stream: string;
  measured: boolean;
  balanced: boolean;
  conservation_kind: string | null;
  dest_count: number | null;
  active_count: number | null;
  rows_read: number | null;
};

export type LedgerCarrier = {
  status?: string | null;
  records_processed?: number | null;
  records_transferred?: number | null;
  row_accounting?: ConservationLedger | Record<string, unknown> | null;
};

const UNMEASURED_SOURCES = new Set(["unmeasured", ""]);
const UNMEASURED_KINDS = new Set(["unmeasured", ""]);
const ARTIFACT_READBACK = "artifact_readback";
const IDENTITY_READBACK = "identity_readback";
const CURRENT_READBACK = "current_readback";

function num(value: unknown): number | null {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

/** Parse the engine payload. Returns null when the server did not stamp a ledger. */
export function readConservationLedger(
  source: LedgerCarrier | Record<string, unknown> | null | undefined,
): ConservationLedger | null {
  if (!source || typeof source !== "object") return null;
  const raw = (source as LedgerCarrier).row_accounting;
  if (!isRecord(raw)) return null;
  const kind = String(raw.conservation_kind ?? "");
  const writtenSource = String(raw.rows_written_source ?? "");
  if (!kind && !writtenSource && raw.dest_count == null && raw.balanced == null) {
    return null;
  }
  return {
    rows_read: num(raw.rows_read),
    rows_written: num(raw.rows_written),
    rows_quarantined: num(raw.rows_quarantined) ?? 0,
    rows_skipped: num(raw.rows_skipped) ?? 0,
    rows_coerced_null: num(raw.rows_coerced_null) ?? 0,
    writer_ack: num(raw.writer_ack),
    dest_count: num(raw.dest_count),
    dest_count_before: num(raw.dest_count_before),
    unaccounted: num(raw.unaccounted),
    balanced: Boolean(raw.balanced),
    rows_read_source: String(raw.rows_read_source ?? ""),
    rows_written_source: writtenSource,
    conservation_kind: kind,
    note: String(raw.note ?? ""),
    writer_ack_delta: num(raw.writer_ack_delta),
    inserts: num(raw.inserts),
    updates: num(raw.updates),
    deletes: num(raw.deletes),
    dest_delta: num(raw.dest_delta),
    unique_batch_keys: num(raw.unique_batch_keys),
    dest_preexisting: num(raw.dest_preexisting),
    active_count: num(raw.active_count),
    inferred_deletes: num(raw.inferred_deletes),
    reactivated: num(raw.reactivated),
    events_read: num(raw.events_read),
    identity_count: num(raw.identity_count),
    vector_rows: num(raw.vector_rows),
    current_count: num(raw.current_count),
    history_rows: num(raw.history_rows),
    missing_keys: num(raw.missing_keys),
    extra_keys: num(raw.extra_keys),
    leftover_deleted: num(raw.leftover_deleted),
    stream_count: num(raw.stream_count),
    measured_streams: num(raw.measured_streams),
    summable: raw.summable == null ? null : Boolean(raw.summable),
    per_stream: parsePerStream(raw.per_stream),
  };
}

function parsePerStream(value: unknown): StreamLedgerSlice[] | null {
  if (!Array.isArray(value) || value.length === 0) return null;
  return value
    .filter((item): item is Record<string, unknown> => isRecord(item))
    .map((item) => ({
      stream: String(item.stream ?? item.name ?? ""),
      measured: Boolean(item.measured),
      balanced: Boolean(item.balanced),
      conservation_kind: item.conservation_kind == null ? null : String(item.conservation_kind),
      dest_count: num(item.dest_count),
      active_count: num(item.active_count),
      rows_read: num(item.rows_read),
    }));
}

function isArtifactLedger(ledger: ConservationLedger | null | undefined): boolean {
  return Boolean(ledger && ledger.rows_written_source === ARTIFACT_READBACK);
}

function isVectorLedger(ledger: ConservationLedger | null | undefined): boolean {
  return Boolean(
    ledger &&
      (ledger.conservation_kind === "vector" || ledger.rows_written_source === IDENTITY_READBACK),
  );
}

function isScd2Ledger(ledger: ConservationLedger | null | undefined): boolean {
  return Boolean(
    ledger &&
      (ledger.conservation_kind === "scd2" || ledger.rows_written_source === CURRENT_READBACK),
  );
}

function isAppendLedger(ledger: ConservationLedger | null | undefined): boolean {
  return Boolean(ledger && ledger.conservation_kind === "append_delta");
}

/** Engine dest Δ for Full Append — never recomputed from dest after − before. */
function appendDeltaValue(ledger: ConservationLedger): number | null {
  if (ledger.dest_delta != null) return ledger.dest_delta;
  if (ledger.rows_written != null) return ledger.rows_written;
  return null;
}

export function isDestMeasured(ledger: ConservationLedger | null | undefined): boolean {
  if (!ledger) return false;
  if (UNMEASURED_KINDS.has(ledger.conservation_kind)) return false;
  if (UNMEASURED_SOURCES.has(ledger.rows_written_source)) return false;
  if (ledger.conservation_kind === "job_rollup") {
    if (ledger.rows_written_source === "per_stream") return ledger.balanced;
    return ledger.dest_count != null || ledger.active_count != null;
  }
  if (ledger.conservation_kind === "mirror") {
    return ledger.active_count != null;
  }
  if (ledger.conservation_kind === "vector") {
    return ledger.dest_count != null && ledger.rows_written_source === IDENTITY_READBACK;
  }
  if (ledger.conservation_kind === "scd2") {
    return ledger.dest_count != null && ledger.rows_written_source === CURRENT_READBACK;
  }
  if (ledger.dest_count == null) return false;
  return true;
}

export function conservationKindLabel(kind: string | null | undefined): string {
  switch (String(kind || "")) {
    case "overwrite":
      return "Overwrite · dest COUNT(*)";
    case "append_delta":
      return "Append · dest delta";
    case "keyed":
      return "Keyed · inserts − deletes (keys, not events)";
    case "mirror":
      return "Mirror · active population";
    case "vector":
      return "Vector · identities, not chunks";
    case "scd2":
      return "SCD2 · current rows, not history";
    case "job_rollup":
      return "Job · every stream closed";
    case "empty_pass":
      return "Empty pass · measured zero";
    case "unmeasured":
      return "Dest unmeasured";
    default:
      return kind ? String(kind) : "Conservation";
  }
}

function fmt(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return Number(value).toLocaleString();
}

/** Engine numbers rendered as the identity — never recomputed. */
export function ledgerEquation(ledger: ConservationLedger): string {
  const kind = ledger.conservation_kind;
  if (kind === "keyed") {
    return `dest Δ ${fmt(ledger.dest_delta)} = inserts ${fmt(ledger.inserts)} − deletes ${fmt(ledger.deletes)}`;
  }
  if (kind === "job_rollup") {
    if (ledger.active_count != null) {
      return `job active ${fmt(ledger.active_count)} across ${fmt(ledger.stream_count)} stream(s)`;
    }
    if (ledger.dest_count != null) {
      return `job dest ${fmt(ledger.dest_count)} = sum of ${fmt(ledger.stream_count)} stream COUNT(*)`;
    }
    return `job closed iff every stream is closed (${fmt(ledger.measured_streams)}/${fmt(ledger.stream_count)} measured)`;
  }
  if (kind === "mirror") {
    return `read ${fmt(ledger.rows_read)} = active ${fmt(ledger.active_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
  }
  if (kind === "vector") {
    return `read ${fmt(ledger.rows_read)} = identities ${fmt(ledger.dest_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
  }
  if (kind === "scd2") {
    return `read ${fmt(ledger.rows_read)} = current ${fmt(ledger.dest_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
  }
  if (kind === "append_delta") {
    return `dest Δ ${fmt(ledger.dest_delta)} = COUNT(*) after ${fmt(ledger.dest_count)} − before ${fmt(ledger.dest_count_before)}`;
  }
  if (kind === "empty_pass") {
    return "0 = 0 (measured empty pass)";
  }
  if (kind === "unmeasured") {
    return "dest COUNT unmeasured — writer ack is not destination proof";
  }
  if (ledger.rows_written_source === ARTIFACT_READBACK) {
    return `read ${fmt(ledger.rows_read)} = artifact ${fmt(ledger.dest_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
  }
  return `read ${fmt(ledger.rows_read)} = dest ${fmt(ledger.dest_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
}

export function writerAckDisagrees(source: unknown): boolean {
  const ledger = resolveLedger(source);
  if (!ledger) return false;
  if (ledger.writer_ack_delta != null) return ledger.writer_ack_delta !== 0;
  if (isAppendLedger(ledger)) {
    const identity = appendDeltaValue(ledger);
    if (ledger.writer_ack == null || identity == null) return false;
    return ledger.writer_ack !== identity;
  }
  if (ledger.writer_ack == null || ledger.dest_count == null) return false;
  return ledger.writer_ack !== ledger.dest_count;
}

function resolveLedger(source: unknown): ConservationLedger | null {
  if (!source || typeof source !== "object") return null;
  const obj = source as Record<string, unknown>;
  if ("row_accounting" in obj) return readConservationLedger(obj as LedgerCarrier);
  if ("conservation_kind" in obj || "rows_written_source" in obj) {
    return readConservationLedger({ row_accounting: obj });
  }
  return readConservationLedger(obj as LedgerCarrier);
}

export type RowMetric = {
  value: string;
  label: string;
  title: string;
  measured: boolean;
  tone?: "ok" | "warn" | "danger" | "muted";
};

function isRunningStatus(status: string | null | undefined): boolean {
  const s = String(status || "").toLowerCase();
  return s === "running" || s === "pending" || s === "queued";
}

/**
 * Primary operator number: independent dest COUNT(*) when the engine measured it.
 * Never falls back to writer ack / records_processed.
 */
export function destHeadline(source: LedgerCarrier | null | undefined): RowMetric {
  const ledger = readConservationLedger(source);
  const running = isRunningStatus(source?.status);
  if (isDestMeasured(ledger) && ledger) {
    if (ledger.conservation_kind === "job_rollup") {
      const unbalanced = ledger.balanced === false;
      if (ledger.active_count != null) {
        return {
          value: Number(ledger.active_count).toLocaleString(),
          label: running ? "Active so far" : "Active at dest",
          title: ledger.note || "Sum of dest-engine active populations across streams of the same kind.",
          measured: true,
          tone: unbalanced ? "danger" : "ok",
        };
      }
      if (ledger.dest_count != null) {
        return {
          value: Number(ledger.dest_count).toLocaleString(),
          label: running ? "Dest so far" : "At destination",
          title: ledger.note || "Sum of dest-engine COUNT(*) across streams of the same kind. Last-table COUNT is not the job.",
          measured: true,
          tone: unbalanced ? "danger" : "ok",
        };
      }
      return {
        value: "—",
        label: running ? "Per-stream" : "Per-stream dest",
        title: ledger.note || "Dest COUNT(*) is not summed across mixed or keyed kinds. Open each stream.",
        measured: true,
        tone: unbalanced ? "danger" : "ok",
      };
    }
    if (ledger.conservation_kind === "mirror") {
      const unbalanced = ledger.balanced === false;
      return {
        value: Number(ledger.active_count).toLocaleString(),
        label: running ? "Active so far" : "Active at dest",
        title: ledger.note || "Dest-engine COUNT(*) WHERE NOT _deleted. Physical COUNT(*) does not drop.",
        measured: true,
        tone: unbalanced ? "danger" : "ok",
      };
    }
    const unbalanced = ledger.balanced === false;
    if (isArtifactLedger(ledger)) {
      return {
        value: Number(ledger.dest_count).toLocaleString(),
        label: running ? "Artifact so far" : "In export artifact",
        title: ledger.note || "Independent record count of the written file. Writer acknowledgement is diagnostic. Cell fidelity unproven.",
        measured: true,
        tone: unbalanced ? "danger" : "ok",
      };
    }
    if (isVectorLedger(ledger)) {
      return {
        value: Number(ledger.dest_count).toLocaleString(),
        label: running ? "Identities so far" : "Identities at dest",
        title: ledger.note || "Dest-engine COUNT(DISTINCT source_id). Physical vector COUNT(*) / collection rowCount is chunk cardinality, not source-row conservation.",
        measured: true,
        tone: unbalanced ? "danger" : "ok",
      };
    }
    if (isScd2Ledger(ledger)) {
      return {
        value: Number(ledger.dest_count).toLocaleString(),
        label: running ? "Current so far" : "Current at dest",
        title: ledger.note || "Dest-engine COUNT(*) WHERE is_current. Physical history COUNT(*) grows on every attribute change.",
        measured: true,
        tone: unbalanced ? "danger" : "ok",
      };
    }
    if (isAppendLedger(ledger)) {
      const appended = appendDeltaValue(ledger);
      if (appended != null) {
        return {
          value: Number(appended).toLocaleString(),
          label: running ? "Appended so far" : "Appended this run",
          title: ledger.note || "Dest COUNT(*) growth this run. Pre-existing dest rows remain. Whole-table checksums are not comparable.",
          measured: true,
          tone: unbalanced ? "danger" : "warn",
        };
      }
    }
    return {
      value: Number(ledger.dest_count).toLocaleString(),
      label: running ? "Dest so far" : "At destination",
      title: ledger.note || "Independent destination COUNT(*)",
      measured: true,
      tone: unbalanced ? "danger" : "ok",
    };
  }
  if (running) {
    return {
      value: "—",
      label: "Dest COUNT",
      title: "Destination COUNT(*) pending until reconcile — writer ack is not dest proof",
      measured: false,
      tone: "muted",
    };
  }
  return {
    value: "—",
    label: "Dest unmeasured",
    title: "Independent dest COUNT(*) was not captured. Writer acknowledgement is not destination proof.",
    measured: false,
    tone: "muted",
  };
}

/** Writer acknowledgement — diagnostic only. */
export function writerHeadline(source: LedgerCarrier | null | undefined): RowMetric {
  const ledger = readConservationLedger(source);
  const running = isRunningStatus(source?.status);
  const ack = ledger?.writer_ack
    ?? num(source?.records_processed)
    ?? num(source?.records_transferred)
    ?? 0;
  const disagrees = writerAckDisagrees(ledger);
  return {
    value: Number(ack).toLocaleString(),
    label: running ? "Written so far" : "Writer ack",
    title: disagrees
      ? `Writer counted ${Number(ack).toLocaleString()} rows dest COUNT(*) does not hold — DMS Full Load hole`
      : running
        ? "Writer acknowledgement — not destination COUNT(*)"
        : "Writer acknowledgement (diagnostic). Dest COUNT(*) is the conservation figure.",
    measured: false,
    tone: disagrees ? "warn" : undefined,
  };
}

/** Compact Jobs list / Overview / search caption. Never falls dest back to writer ack. */
export function destMetricCompact(metric: RowMetric): string {
  if (!metric.measured) return `${metric.value} ${metric.label.toLowerCase()}`;
  if (metric.label.toLowerCase().includes("per-stream")) return "per-stream dest";
  if (metric.label.toLowerCase().includes("active")) return `${metric.value} active`;
  if (metric.label.toLowerCase().includes("artifact")) return `${metric.value} in artifact`;
  if (metric.label.toLowerCase().includes("identit")) return `${metric.value} identities`;
  if (metric.label.toLowerCase().includes("current")) return `${metric.value} current`;
  if (metric.label.toLowerCase().includes("append")) return `${metric.value} appended`;
  return `${metric.value} at dest`;
}

export function formatJobRowMetric(source: LedgerCarrier | null | undefined): RowMetric {
  const dest = destHeadline(source);
  if (dest.measured) return dest;
  const writer = writerHeadline(source);
  return {
    ...writer,
    value: writer.value,
    label: writer.label,
  };
}

export function destProvenCount(source: LedgerCarrier | null | undefined): number | null {
  const ledger = readConservationLedger(source);
  if (!isDestMeasured(ledger) || !ledger) return null;
  if (ledger.conservation_kind === "mirror") {
    return ledger.active_count;
  }
  if (ledger.conservation_kind === "job_rollup") {
    if (ledger.active_count != null) return ledger.active_count;
    if (ledger.dest_count != null) return ledger.dest_count;
    return null;
  }
  if (ledger.dest_count == null) return null;
  return ledger.dest_count;
}

/** Operator-facing complete copy — dest COUNT when measured, never “rows transferred”. */
export function conservationCompleteCopy(
  source: LedgerCarrier | null | undefined,
  opts?: { quarantine?: boolean },
): string {
  const dest = destHeadline(source);
  const writer = writerHeadline(source);
  const ledger = readConservationLedger(source);
  const mirror = ledger?.conservation_kind === "mirror";
  const job = ledger?.conservation_kind === "job_rollup";
  const artifact = isArtifactLedger(ledger);
  const vector = isVectorLedger(ledger);
  const scd2 = isScd2Ledger(ledger);
  const append = isAppendLedger(ledger);
  if (opts?.quarantine) {
    if (dest.measured) {
      return mirror
        ? `${dest.value} active at destination; some rows held out or coerced to NULL`
        : job && dest.value === "—"
          ? "Per-stream dest; some rows held out or coerced to NULL"
          : artifact
            ? `${dest.value} in export artifact; some rows held out or coerced to NULL`
            : vector
              ? `${dest.value} identities at dest; some rows held out or coerced to NULL`
              : scd2
                ? `${dest.value} current at dest; some rows held out or coerced to NULL`
                : append
                  ? `${dest.value} appended this run; some rows held out or coerced to NULL`
                : `${dest.value} at destination; some rows held out or coerced to NULL`;
    }
    return `${writer.value} writer-acked (dest COUNT unmeasured); some rows held out or coerced to NULL`;
  }
  if (dest.measured) {
    if (mirror) return `${dest.value} active at destination`;
    if (job && dest.value === "—") return "Every stream ledger is closed — dest COUNT not summed";
    if (artifact) return `${dest.value} in export artifact`;
    if (vector) return `${dest.value} identities at dest`;
    if (scd2) return `${dest.value} current at dest`;
    if (append) {
      const before = ledger?.dest_count_before;
      const after = ledger?.dest_count;
      if (before != null && after != null) {
        return `${dest.value} appended this run (dest ${Number(before).toLocaleString()} → ${Number(after).toLocaleString()})`;
      }
      return `${dest.value} appended this run`;
    }
    return `${dest.value} at destination`;
  }
  return `${writer.value} writer-acked — dest COUNT unmeasured`;
}

export type LedgerIdentityCell = { label: string; value: string };

/** Display-only identity cells from engine fields — not a second algorithm. */
export function ledgerIdentityCells(ledger: ConservationLedger): LedgerIdentityCell[] {
  if (ledger.conservation_kind === "job_rollup") {
    const cells: LedgerIdentityCell[] = [
      { label: "Streams", value: fmt(ledger.stream_count) },
      { label: "Measured", value: fmt(ledger.measured_streams) },
    ];
    if (ledger.active_count != null) {
      cells.push({ label: "Active", value: fmt(ledger.active_count) });
    } else {
      cells.push({ label: "Dest COUNT(*)", value: fmt(ledger.dest_count) });
    }
    cells.push({ label: "Writer ack", value: fmt(ledger.writer_ack) });
    for (const stream of ledger.per_stream || []) {
      const value = stream.active_count != null
        ? fmt(stream.active_count)
        : fmt(stream.dest_count);
      cells.push({
        label: stream.stream || "stream",
        value: stream.measured ? value : "—",
      });
    }
    return cells;
  }
    if (ledger.conservation_kind === "keyed") {
    const cells: LedgerIdentityCell[] = [
      { label: "Inserts", value: fmt(ledger.inserts) },
      { label: "Updates", value: fmt(ledger.updates) },
      { label: "Deletes", value: fmt(ledger.deletes) },
      { label: "Dest Δ", value: fmt(ledger.dest_delta) },
      { label: "Dest before", value: fmt(ledger.dest_count_before) },
      { label: "Dest after", value: fmt(ledger.dest_count) },
    ];
    if (ledger.events_read != null || ledger.unique_batch_keys != null) {
      cells.push(
        { label: "Events", value: fmt(ledger.events_read) },
        { label: "Keys", value: fmt(ledger.unique_batch_keys) },
      );
    }
    return cells;
  }
  if (ledger.conservation_kind === "mirror") {
    return [
      { label: "Active", value: fmt(ledger.active_count) },
      { label: "Inferred deletes", value: fmt(ledger.inferred_deletes) },
      { label: "Reactivated", value: fmt(ledger.reactivated) },
      { label: "Physical COUNT(*)", value: fmt(ledger.dest_count) },
    ];
  }
  if (ledger.conservation_kind === "vector") {
    return [
      { label: "Identities", value: fmt(ledger.dest_count) },
      { label: "Vectors", value: fmt(ledger.vector_rows) },
      { label: "Writer ack", value: fmt(ledger.writer_ack) },
      { label: "Held out", value: fmt(ledger.rows_quarantined) },
      { label: "Skipped", value: fmt(ledger.rows_skipped) },
    ];
  }
  if (ledger.conservation_kind === "scd2") {
    return [
      { label: "Current", value: fmt(ledger.current_count ?? ledger.dest_count) },
      { label: "History", value: fmt(ledger.history_rows) },
      { label: "Writer ack", value: fmt(ledger.writer_ack) },
      { label: "Held out", value: fmt(ledger.rows_quarantined) },
      { label: "Skipped", value: fmt(ledger.rows_skipped) },
    ];
  }
  if (ledger.conservation_kind === "append_delta") {
    return [
      { label: "Read", value: fmt(ledger.rows_read) },
      { label: "Dest after", value: fmt(ledger.dest_count) },
      { label: "Dest before", value: fmt(ledger.dest_count_before) },
      { label: "Dest Δ", value: fmt(ledger.dest_delta) },
    ];
  }
  const cells: LedgerIdentityCell[] = [
    { label: "Read", value: fmt(ledger.rows_read) },
    {
      label: isArtifactLedger(ledger) ? "Artifact records" : "Dest COUNT(*)",
      value: fmt(ledger.dest_count),
    },
    { label: "Held out", value: fmt(ledger.rows_quarantined) },
    { label: "Skipped", value: fmt(ledger.rows_skipped) },
  ];
  if (ledger.missing_keys != null || ledger.extra_keys != null || ledger.leftover_deleted != null) {
    cells.push(
      { label: "Missing keys", value: fmt(ledger.missing_keys) },
      { label: "Extra dest keys", value: fmt(ledger.extra_keys) },
    );
    if (ledger.leftover_deleted != null) {
      cells.push({ label: "Leftover deleted", value: fmt(ledger.leftover_deleted) });
    }
  }
  return cells;
}
