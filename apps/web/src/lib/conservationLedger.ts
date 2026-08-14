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
};

export type LedgerCarrier = {
  status?: string | null;
  records_processed?: number | null;
  records_transferred?: number | null;
  row_accounting?: ConservationLedger | Record<string, unknown> | null;
};

const UNMEASURED_SOURCES = new Set(["unmeasured", ""]);
const UNMEASURED_KINDS = new Set(["unmeasured", ""]);

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
  };
}

export function isDestMeasured(ledger: ConservationLedger | null | undefined): boolean {
  if (!ledger) return false;
  if (ledger.dest_count == null) return false;
  if (UNMEASURED_KINDS.has(ledger.conservation_kind)) return false;
  if (UNMEASURED_SOURCES.has(ledger.rows_written_source)) return false;
  return true;
}

export function conservationKindLabel(kind: string | null | undefined): string {
  switch (String(kind || "")) {
    case "overwrite":
      return "Overwrite · dest COUNT(*)";
    case "append_delta":
      return "Append · dest delta";
    case "keyed":
      return "Keyed · inserts − deletes";
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
  if (kind === "append_delta") {
    return `dest Δ ${fmt(ledger.dest_delta)} = COUNT(*) after ${fmt(ledger.dest_count)} − before ${fmt(ledger.dest_count_before)}`;
  }
  if (kind === "empty_pass") {
    return "0 = 0 (measured empty pass)";
  }
  if (kind === "unmeasured") {
    return "dest COUNT unmeasured — writer ack is not destination proof";
  }
  return `read ${fmt(ledger.rows_read)} = dest ${fmt(ledger.dest_count)} + held out ${fmt(ledger.rows_quarantined)} + skipped ${fmt(ledger.rows_skipped)}`;
}

export function writerAckDisagrees(source: unknown): boolean {
  const ledger = resolveLedger(source);
  if (!ledger) return false;
  if (ledger.writer_ack_delta != null) return ledger.writer_ack_delta !== 0;
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
    const unbalanced = ledger.balanced === false;
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

/** Compact Jobs list / Overview cell. Dest COUNT when measured; else honest writer label. */
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
  if (!isDestMeasured(ledger) || ledger?.dest_count == null) return null;
  return ledger.dest_count;
}
