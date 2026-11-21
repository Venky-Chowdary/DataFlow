/**
 * One verdict vocabulary for every screen (UI pass 2, item 1).
 *
 * The five verdicts are the Trust Report's (docs/use/verification.md, decided
 * by apps/api/services/parity_report.py). Two more states are not verdicts and
 * say so: "Not checked yet" (the run has not finished) and "Checking" (the
 * verdict is being read). Nothing here decides a verdict; it only presents the
 * one the API stated.
 *
 * One colour per verdict, everywhere: proven and explained are the only green
 * ones, unexplained is red, no difference found is blue (a sample bounds the
 * rate, it proves nothing), unverifiable is neutral. Colour is never the only
 * signal: each state has its own icon and word.
 */
import type { IconName } from "../components/icons/iconMap";
import type { ParityVerdict } from "./parity";
import { plural } from "./plural";
import type { TransferJob } from "./types";

export type VerdictState = ParityVerdict | "not_checked" | "checking" | "not_loaded";

export type VerdictTone = "success" | "danger" | "info" | "neutral";

export interface VerdictPresentation {
  state: VerdictState;
  /** The word on the chip. */
  word: string;
  icon: IconName;
  tone: VerdictTone;
  /** One line: what the word means (tooltip and screen-reader description). */
  meaning: string;
  /** True for the five Trust Report verdicts; false for the waiting states. */
  isVerdict: boolean;
}

const PRESENTATION: Record<VerdictState, Omit<VerdictPresentation, "state">> = {
  proven: {
    word: "Proven",
    icon: "verdict-proven",
    tone: "success",
    meaning: "Every row was compared and the destination matches the source.",
    isVerdict: true,
  },
  explained: {
    word: "Explained",
    icon: "verdict-explained",
    tone: "success",
    meaning: "Every row was compared; each difference is explained by a rule.",
    isVerdict: true,
  },
  no_difference_found: {
    word: "No difference found",
    icon: "verdict-no-difference",
    tone: "info",
    meaning: "The rows checked match, but not every row was compared.",
    isVerdict: true,
  },
  unexplained: {
    word: "Unexplained",
    icon: "verdict-unexplained",
    tone: "danger",
    meaning: "Some rows differ and no rule explains them.",
    isVerdict: true,
  },
  unverifiable: {
    word: "Unverifiable",
    icon: "verdict-unverifiable",
    tone: "neutral",
    meaning: "The evidence cannot decide whether the destination matches.",
    isVerdict: true,
  },
  not_checked: {
    word: "Not checked yet",
    icon: "verdict-not-checked",
    tone: "neutral",
    meaning: "Verification runs after the load finishes.",
    isVerdict: false,
  },
  checking: {
    word: "Checking",
    icon: "spinner",
    tone: "neutral",
    meaning: "Reading the verdict for this run.",
    isVerdict: false,
  },
  not_loaded: {
    word: "Not loaded",
    icon: "verdict-not-checked",
    tone: "neutral",
    meaning: "The verdict could not be read. Open the Trust Report to see it.",
    isVerdict: false,
  },
};

/** Every word a verdict chip can show; a guard test keeps other labels out. */
export const VERDICT_CHIP_WORDS: readonly string[] = Object.values(PRESENTATION).map((p) => p.word);

export function isParityVerdict(value: unknown): value is ParityVerdict {
  return value === "proven" || value === "explained" || value === "no_difference_found"
    || value === "unexplained" || value === "unverifiable";
}

export function verdictPresentation(state: VerdictState): VerdictPresentation {
  return { state, ...PRESENTATION[state] };
}

/** "Unexplained · 4 rows": the count only where it adds a fact (unexplained rows). */
export function verdictCountSuffix(state: VerdictState, unexplainedRows?: number | null): string {
  if (state !== "unexplained" || unexplainedRows == null || unexplainedRows <= 0) return "";
  return plural(unexplainedRows, "row");
}

/** What the API returned for one list row (`GET /parity/verdicts`). */
export interface ListVerdict {
  verdict: ParityVerdict;
  unexplained: number;
  method?: string;
}

/** Where a page of list verdicts stands. */
export type ListVerdictLoad = "loading" | "ready" | "error";

const LIVE = new Set(["running", "pending", "queued", "retrying"]);

/**
 * The state of one Jobs row. Order matters: a live run is never given a
 * verdict; a run Gate-8 never reconciled is unverifiable (the API's own rule,
 * parity_report._verdict); otherwise the verdict the API stated, or the
 * honest waiting state while it loads.
 */
export function jobVerdictState(
  job: Pick<TransferJob, "status" | "verification_summary"> & { cancel_requested?: boolean },
  listed: ListVerdict | undefined,
  load: ListVerdictLoad,
): VerdictState {
  const status = String(job.status || "");
  if (LIVE.has(status)) return "not_checked";
  if (listed && isParityVerdict(listed.verdict)) return listed.verdict;
  const summary = job.verification_summary;
  if (!summary || typeof summary !== "object" || Object.keys(summary).length === 0) return "unverifiable";
  if (load === "error") return "not_loaded";
  return "checking";
}

/** Sort order: what needs a person first. */
export const VERDICT_RANK: Record<VerdictState, number> = {
  unexplained: 0,
  unverifiable: 1,
  not_loaded: 2,
  no_difference_found: 3,
  checking: 4,
  not_checked: 5,
  explained: 6,
  proven: 7,
};

/** Job ids whose verdict the list needs: finished runs that Gate-8 reconciled. */
export function jobsNeedingVerdicts(jobs: Pick<TransferJob, "_id" | "status" | "verification_summary">[]): string[] {
  const ids: string[] = [];
  for (const job of jobs) {
    if (LIVE.has(String(job.status || ""))) continue;
    const summary = job.verification_summary;
    if (!summary || typeof summary !== "object" || Object.keys(summary).length === 0) continue;
    if (!ids.includes(job._id)) ids.push(job._id);
  }
  return ids.slice(0, 100);
}

/** A verdict read for one job state, and when it was read (ms). Errors are never cached. */
export interface CachedListVerdict {
  stamp: string;
  verdict: ListVerdict;
  /** Date.now() when it arrived: the newer of a list read and a report read wins. */
  at: number;
}

/**
 * The job state a verdict belongs to. Parity Studio decisions and re-reads
 * do not touch updated_at, so the list's parity_stamp (version and stored
 * summary time) is part of it (review UI2 #2).
 */
export function listVerdictStamp(job: Pick<TransferJob, "status" | "updated_at" | "completed_at" | "parity_stamp">): string {
  return [job.status || "", job.updated_at || "", job.completed_at || "", job.parity_stamp || ""].join("|");
}

/** Ids whose verdict is missing or was read for another state of the job. */
export function verdictIdsToFetch(jobs: TransferJob[], cache: ReadonlyMap<string, CachedListVerdict>): string[] {
  const byId = new Map(jobs.map((job) => [job._id, job]));
  return jobsNeedingVerdicts(jobs).filter((id) => {
    const hit = cache.get(id);
    const job = byId.get(id);
    return !hit || !job || hit.stamp !== listVerdictStamp(job);
  });
}

/**
 * Rows with their verdict merged in (`parity_verdict`). The verdict of a
 * Trust Report opened in this session (`reports`, keyed by job id) is the
 * source of truth when it is newer than the list's: the grid row and the
 * drawer never show two different words. `unavailable` ids (read failed, or
 * the API left them out) get null: "Not loaded", retried on the next read.
 */
export function mergeListVerdicts(
  jobs: TransferJob[],
  cache: ReadonlyMap<string, CachedListVerdict>,
  reports: ReadonlyMap<string, { verdict: ListVerdict; at: number }> = new Map(),
  unavailable: ReadonlySet<string> = new Set(),
): TransferJob[] {
  let changed = false;
  const out = jobs.map((job) => {
    const hit = cache.get(job._id);
    const listed = hit && hit.stamp === listVerdictStamp(job) ? hit : null;
    const report = reports.get(job._id);
    const pick = report && (!listed || report.at >= listed.at) ? report.verdict : listed?.verdict;
    if (pick) {
      changed = true;
      return { ...job, parity_verdict: pick };
    }
    if (unavailable.has(job._id)) {
      changed = true;
      return { ...job, parity_verdict: null };
    }
    return job;
  });
  return changed ? out : jobs;
}
