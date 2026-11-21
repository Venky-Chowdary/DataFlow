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
import { readJobStreamNames, readJobStreams } from "./jobEvidence";
import type { JobProgress } from "./types";
import { isJobSuccess, isJobTerminal } from "./uiUtils";

export type StageState = "waiting" | "active" | "done" | "failed" | "not_run";
export type TableState = "queued" | "loading" | "verifying" | "loaded" | "checked" | "failed" | "stopped" | "not_run";

export interface TableProgressRow {
  name: string;
  state: TableState;
  read: StageState;
  write: StageState;
  verify: StageState;
  /** Rows written to this table so far; null when the engine has not said. */
  rows: number | null;
}

const key = (name: string) => name.trim().toLowerCase();

export const TABLE_STATE_WORD: Record<TableState, string> = {
  queued: "Queued",
  loading: "Loading",
  verifying: "Verifying",
  loaded: "Loaded",
  checked: "Loaded and checked",
  failed: "Failed",
  stopped: "Stopped",
  not_run: "Not run",
};

export const STAGE_WORD: Record<StageState, string> = {
  waiting: "waiting",
  active: "in progress",
  done: "done",
  failed: "failed",
  not_run: "not run",
};

export function tableProgressRows(
  job: Pick<JobProgress, "status" | "phase" | "records_processed" | "stream_progress" | "reconciliation">
    & Parameters<typeof readJobStreamNames>[0],
  options: { reconciling?: boolean } = {},
): TableProgressRow[] {
  const names = readJobStreamNames(job);
  if (names.length < 2) return [];
  const status = String(job.status || "");
  const terminal = isJobTerminal(status) || isJobSuccess(status);
  const success = isJobSuccess(status);
  const reconciling = Boolean(options.reconciling) && !terminal;
  const verified = Boolean(job.reconciliation && typeof job.reconciliation === "object" && Object.keys(job.reconciliation).length);
  const progress = job.stream_progress || {};
  const health = new Map(readJobStreams(job).map((s) => [key(s.name), s]));
  let stopMarked = false;

  return names.map((name) => {
    const entry = progress[key(name)];
    const final = health.get(key(name));
    const finalStatus = String(final?.status || "").toLowerCase();
    const finishedRows = entry?.status === "completed" && typeof entry.rows === "number"
      ? entry.rows
      : final && typeof final.records_processed === "number" && finalStatus !== "failed" ? final.records_processed : null;
    const finished = entry?.status === "completed" || (terminal && Boolean(final) && finalStatus !== "failed" && finalStatus !== "running");

    if (finished) {
      if (reconciling) return { name, state: "verifying", read: "done", write: "done", verify: "active", rows: finishedRows };
      if (!terminal) return { name, state: "loaded", read: "done", write: "done", verify: "waiting", rows: finishedRows };
      return success && verified
        ? { name, state: "checked", read: "done", write: "done", verify: "done", rows: finishedRows }
        : { name, state: "loaded", read: "done", write: "done", verify: "not_run", rows: finishedRows };
    }
    if (!terminal && entry?.status === "running") {
      const counter = Number(job.records_processed ?? 0);
      // The counter restarts for each table at its first batch; until then it still holds the last table's rows.
      const rows = typeof entry.counter_at_start === "number" && counter === entry.counter_at_start ? null : counter;
      return { name, state: "loading", read: "active", write: "active", verify: "waiting", rows };
    }
    if (!terminal) return { name, state: "queued", read: "waiting", write: "waiting", verify: "waiting", rows: null };
    // The run ended before this table finished: the first such table is where it stopped.
    if (!stopMarked && (finalStatus === "failed" || !success)) {
      stopMarked = true;
      const failed = status === "failed" || finalStatus === "failed";
      return {
        name,
        state: failed ? "failed" : "stopped",
        read: failed ? "failed" : "not_run",
        write: failed ? "failed" : "not_run",
        verify: "not_run",
        rows: null,
      };
    }
    return { name, state: "not_run", read: "not_run", write: "not_run", verify: "not_run", rows: null };
  });
}

const ORDER: Record<TableState, number> = {
  failed: 0, loading: 1, verifying: 2, stopped: 3, queued: 4, not_run: 5, loaded: 6, checked: 7,
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
