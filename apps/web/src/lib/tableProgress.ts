/**
 * Per-table progress on the Run screen (UI pass 2, item 3).
 *
 * Each table of a multi-table run has three stages: Read, Write, Verify. The
 * engine streams a table (reads and writes it batch by batch), so Read and
 * Write are active together and finish together; Verify (Gate-8) runs once
 * every table has loaded. Sources, all already on the job document:
 *
 * - `stream_progress`: the table loading now (`running`, with the row counter
 *   at its start) and the tables this pass finished (`completed`, rows);
 * - `destination_summary.streams`: each table's outcome once the run ends (a
 *   finished pass clears `stream_progress`);
 * - the job's status and phase (reconcile = Verify running).
 *
 * Nothing is guessed: a count is shown only when the engine reported it.
 */
import { readJobStreamNames } from "./jobEvidence";
import type { JobProgress } from "./types";
import { isJobSuccess, isJobTerminal } from "./uiUtils";

export type StageState = "waiting" | "active" | "done" | "failed" | "not_run" | "not_compared" | "undone";
export type TableState =
  | "queued" | "loading" | "verifying" | "loaded" | "checked" | "mismatch" | "failed" | "stopped" | "rolled_back"
  | "restore_failed" | "not_run";

export interface TableProgressRow {
  name: string;
  state: TableState;
  read: StageState;
  write: StageState;
  verify: StageState;
  /** Rows written to this table so far; null when the engine has not said. */
  rows: number | null;
  /** The rows were written by an earlier pass this run resumed after. */
  rowsFromEarlierPass?: boolean;
  /** Why, in the engine's words (a restore that failed). */
  note?: string;
}

const key = (name: string) => name.trim().toLowerCase();

export const TABLE_STATE_WORD: Record<TableState, string> = {
  queued: "Queued",
  loading: "Loading",
  verifying: "Verifying",
  loaded: "Loaded",
  checked: "Loaded and compared",
  mismatch: "Gate-8 found a difference",
  failed: "Failed",
  stopped: "Stopped",
  rolled_back: "Rolled back",
  restore_failed: "Restore failed",
  not_run: "Not run",
};

export const STAGE_WORD: Record<StageState, string> = {
  waiting: "waiting",
  active: "in progress",
  done: "done",
  failed: "failed",
  not_run: "not run",
  not_compared: "not compared",
  undone: "rolled back",
};

interface StreamRecord { name: string; status: string; rows: number | null; earlier: number | null }

/** destination_summary.streams as the engine wrote it (incl. rows carried from an earlier pass). */
function streamRecords(summary: unknown): Map<string, StreamRecord> {
  const out = new Map<string, StreamRecord>();
  const streams = summary && typeof summary === "object" ? (summary as { streams?: unknown }).streams : null;
  if (!Array.isArray(streams)) return out;
  for (const raw of streams) {
    if (!raw || typeof raw !== "object") continue;
    const r = raw as Record<string, unknown>;
    const name = String(r.name ?? "").trim();
    if (!name) continue;
    const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : null);
    out.set(key(name), { name, status: String(r.status ?? "").toLowerCase(), rows: num(r.records_processed), earlier: num(r.rows_from_earlier_pass) });
  }
  return out;
}

type ProgressJob = Pick<JobProgress, "status" | "phase" | "records_processed" | "stream_progress" | "reconciliation" | "error_details">
  & Parameters<typeof readJobStreamNames>[0];

interface UndoEntry { stream: string; decision: string; note: string }

/**
 * The failed pass's record, written with the failed status (error_details):
 * the table it stopped on, and what the undo did to each table. Entries name
 * their stream; an older job's entries name only the destination table,
 * matched to a stream name here (re-review D).
 */
function stopRecord(job: ProgressJob, names: string[]): { stopped: string | null; undo: Map<string, UndoEntry> } {
  const details = job.error_details && typeof job.error_details === "object" ? job.error_details as Record<string, unknown> : {};
  const stopped = typeof details.stopped_stream === "string" && details.stopped_stream ? key(details.stopped_stream) : null;
  const undo = new Map<string, UndoEntry>();
  const entries = Array.isArray(details.multi_stream_undo) ? details.multi_stream_undo : [];
  for (const raw of entries) {
    if (!raw || typeof raw !== "object") continue;
    const e = raw as Record<string, unknown>;
    const table = key(String(e.table ?? ""));
    const named = key(String(e.stream ?? ""));
    const stream = named || names.map(key).find((n) => n === table || table.endsWith(`.${n}`) || n.endsWith(`.${table}`)) || "";
    if (!stream) continue;
    undo.set(stream, { stream, decision: String(e.decision ?? ""), note: String(e.note ?? "") });
  }
  return { stopped, undo };
}

/**
 * The table Gate-8 compared. On a multi-table run Gate-8 runs once, on the
 * last table (apps/api/services/reconcile_coverage.py): the report names it
 * (`compared_stream`); an older report is the last table of the run.
 */
function comparedTable(job: ProgressJob, names: string[]): string | null {
  const recon = job.reconciliation as Record<string, unknown> | undefined;
  if (!recon || typeof recon !== "object" || !Object.keys(recon).length) return null;
  const named = String(recon.compared_stream ?? "").trim();
  if (named) return key(named);
  const streams = streamRecords(job.destination_summary);
  const last = [...streams.values()].pop()?.name ?? names[names.length - 1];
  return last ? key(last) : null;
}

export function tableProgressRows(job: ProgressJob, options: { reconciling?: boolean } = {}): TableProgressRow[] {
  const names = readJobStreamNames(job);
  if (names.length < 2) return [];
  const status = String(job.status || "");
  const terminal = isJobTerminal(status) || isJobSuccess(status);
  const success = isJobSuccess(status);
  const reconciling = Boolean(options.reconciling) && !terminal;
  const recon = job.reconciliation as { passed?: unknown } | undefined;
  const compared = success ? comparedTable(job, names) : null;
  const progress = job.stream_progress || {};
  const records = streamRecords(job.destination_summary);
  const { stopped, undo } = terminal && !success ? stopRecord(job, names) : { stopped: null, undo: new Map<string, UndoEntry>() };
  // While Gate-8 runs it is comparing the run's last table (reconcile_coverage binds it there).
  const verifyingTable = reconciling ? key(names[names.length - 1]) : null;
  let stopMarked = false;
  let pastStop = false;

  return names.map((name) => {
    const k = key(name);
    const entry = progress[k];
    const record = records.get(k);
    const carried = record && (record.rows ?? 0) === 0 && (record.earlier ?? 0) > 0;
    const finishedRows = entry?.status === "completed" && typeof entry.rows === "number"
      ? entry.rows
      : record && record.status !== "failed" ? (carried ? record.earlier : record.rows) : null;
    const extra = carried ? { rowsFromEarlierPass: true } : {};

    if (!terminal) {
      if (entry?.status === "completed") {
        if (!reconciling) return { name, state: "loaded", read: "done", write: "done", verify: "waiting", rows: finishedRows, ...extra };
        // Gate-8 compares one table, the last; the others are not compared (re-review residual #1).
        return k === verifyingTable
          ? { name, state: "verifying", read: "done", write: "done", verify: "active", rows: finishedRows, ...extra }
          : { name, state: "loaded", read: "done", write: "done", verify: "not_compared", rows: finishedRows, ...extra };
      }
      if (entry?.status === "running") {
        const counter = Number(job.records_processed ?? 0);
        // The counter restarts for each table at its first batch; until then it still holds the last table's rows.
        const rows = typeof entry.counter_at_start === "number" && counter === entry.counter_at_start ? null : counter;
        return { name, state: "loading", read: "active", write: "active", verify: "waiting", rows };
      }
      return { name, state: "queued", read: "waiting", write: "waiting", verify: "waiting", rows: null };
    }

    if (success) {
      if (compared === k) {
        const passed = recon?.passed === true;
        const failed = recon?.passed === false;
        return {
          name,
          state: passed ? "checked" : failed ? "mismatch" : "loaded",
          read: "done", write: "done",
          verify: passed ? "done" : failed ? "failed" : "not_run",
          rows: finishedRows, ...extra,
        };
      }
      // Gate-8 did not compare this table: say so, never "done".
      return { name, state: "loaded", read: "done", write: "done", verify: compared ? "not_compared" : "not_run", rows: finishedRows, ...extra };
    }

    // The run ended before every table finished.
    const failedRun = status === "failed";
    if (stopped) {
      if (k === stopped) {
        pastStop = true;
        return failedRun
          ? { name, state: "failed", read: "failed", write: "failed", verify: "not_run", rows: null }
          : { name, state: "stopped", read: "not_run", write: "not_run", verify: "not_run", rows: null };
      }
      const undone = undo.get(k);
      if (undone && undone.decision === "restore_failed") {
        return { name, state: "restore_failed", read: "done", write: "failed", verify: "not_run", rows: null, note: undone.note };
      }
      if (undone && undone.decision && undone.decision !== "retained") {
        return { name, state: "rolled_back", read: "done", write: "undone", verify: "not_run", rows: null, note: undone.note };
      }
      if (!pastStop && entry?.status === "completed") {
        return { name, state: "loaded", read: "done", write: "done", verify: "not_run", rows: finishedRows, ...extra };
      }
      return { name, state: "not_run", read: "not_run", write: "not_run", verify: "not_run", rows: null };
    }
    // An older job with no stop record: the first table without a finish record is where it stopped.
    if (entry?.status === "completed" || record?.status === "completed") {
      return { name, state: "loaded", read: "done", write: "done", verify: "not_run", rows: finishedRows, ...extra };
    }
    if (!stopMarked) {
      stopMarked = true;
      return {
        name,
        state: failedRun ? "failed" : "stopped",
        read: failedRun ? "failed" : "not_run",
        write: failedRun ? "failed" : "not_run",
        verify: "not_run",
        rows: null,
      };
    }
    return { name, state: "not_run", read: "not_run", write: "not_run", verify: "not_run", rows: null };
  });
}

const ORDER: Record<TableState, number> = {
  failed: 0, restore_failed: 1, mismatch: 2, loading: 3, verifying: 4, stopped: 5, rolled_back: 6, queued: 7, not_run: 8, loaded: 9, checked: 10,
};

/** Active first, then queued, then done (the done ones collapse under "N done"). */
export function orderTableRows(rows: TableProgressRow[]): { active: TableProgressRow[]; done: TableProgressRow[] } {
  const sorted = rows.map((row, i) => ({ row, i })).sort((a, b) => ORDER[a.row.state] - ORDER[b.row.state] || a.i - b.i).map((x) => x.row);
  return {
    active: sorted.filter((r) => r.state !== "loaded" && r.state !== "checked"),
    done: sorted.filter((r) => r.state === "loaded" || r.state === "checked"),
  };
}

/** What a polite live region says when a table changes state (never per-tick counts). */
export function tableStateAnnouncements(prev: Map<string, TableState>, rows: TableProgressRow[]): string[] {
  const out: string[] = [];
  for (const row of rows) {
    const before = prev.get(row.name);
    if (before && before !== row.state) out.push(`${row.name}: ${TABLE_STATE_WORD[row.state].toLowerCase()}`);
  }
  return out;
}
