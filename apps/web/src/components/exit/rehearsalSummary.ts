/**
 * What a rehearsal page says, decided from the API's payload in one place so
 * every state is consistent: the step states (a step after a failure never
 * reads "Done"), the one-line outcome, the failure in the user's terms (with
 * the administrator detail kept apart), the duration sentence and the header's
 * primary action.
 *
 * The API's timeline (services/exit_rehearsal.timeline) marks verify "done"
 * whenever the rehearsal finished, even when verify() ran over zero extracted
 * tables, and marks inventory / seal with the same start and end time (0 s).
 * Those are not measurements, so they are not shown as such.
 */
import { plural } from "../../lib/plural";
import {
  formatDate,
  formatDuration,
  formatWindow,
  stageLabel,
  type ExitRehearsal,
  type RehearsalStage,
  type RehearsalTable,
} from "../../lib/exitReadiness";

export type StepState = "done" | "running" | "failed" | "held" | "waiting" | "not_started" | "not_run";

export interface RehearsalStep {
  stage: RehearsalStage;
  label: string;
  state: StepState;
  /** "Done", "Failed", "Not run"… */
  stateLabel: string;
  /** A measured duration or a short fact ("0 of 1 extracted"); empty when there is none. */
  detail: string;
}

const STATE_LABEL: Record<StepState, string> = {
  done: "Done",
  running: "Running",
  failed: "Failed",
  held: "Held",
  waiting: "Waiting",
  not_started: "Not started",
  not_run: "Not run",
};

type TimelineStep = ExitRehearsal["timeline"][number];

/** A measured duration, with a sub-second one read as "under 1 s" rather than "0 s". */
export function formatMeasured(seconds: number | null | undefined): string {
  return seconds != null && seconds >= 0 && seconds < 0.5 ? "under 1 s" : formatDuration(seconds);
}

/** A duration only when the step was really timed: both clocks present and not the same instant. */
export function measuredStepDuration(step: Pick<TimelineStep, "seconds" | "started_at" | "finished_at">): string {
  if (step.seconds == null || !step.started_at || !step.finished_at || step.started_at === step.finished_at) return "";
  return formatMeasured(step.seconds);
}

function step(stage: RehearsalStage, state: StepState, detail = ""): RehearsalStep {
  return { stage, label: stageLabel(stage), state, stateLabel: STATE_LABEL[state], detail };
}

/**
 * The stepper. While running, the API's states map one to one (pending reads
 * "Not started"). Once finished: an extract that failed or was held with
 * nothing extracted leaves verify, seal and sign-off "Not run"; sign-off is
 * only ever open for a proven rehearsal.
 */
export function rehearsalSteps(r: ExitRehearsal): RehearsalStep[] {
  const stages: RehearsalStage[] = ["inventory", "extract", "verify", "seal", "sign"];
  const byStage = new Map(r.timeline.map((s) => [s.stage, s]));
  const at = (stage: RehearsalStage): TimelineStep =>
    byStage.get(stage) ?? { stage, status: "pending", started_at: null, finished_at: null, seconds: null };
  const { completeness: c } = r;

  if (r.verdict === "running") {
    return stages.map((stage) => {
      const s = at(stage);
      if (s.status === "done") return step(stage, "done", measuredStepDuration(s));
      if (s.status === "running") return step(stage, "running");
      if (s.status === "failed") return step(stage, "failed");
      return step(stage, "not_started");
    });
  }

  const extracted = c.tables_extracted;
  const out: RehearsalStep[] = [];
  out.push(step("inventory", "done", measuredStepDuration(at("inventory"))));

  const extract = at("extract");
  let nothingExtracted = false;
  if (extract.status === "failed") {
    const held = r.verdict === "needs_pii_review";
    out.push(step("extract", held ? "held" : "failed", `${extracted.toLocaleString("en-US")} of ${plural(c.tables_declared, "table")} extracted`));
    nothingExtracted = extracted === 0;
  } else {
    out.push(step("extract", "done", measuredStepDuration(extract)));
  }

  const verify = at("verify");
  if (nothingExtracted) out.push(step("verify", "not_run", "Nothing was extracted"));
  else if (verify.status === "done") out.push(step("verify", "done", measuredStepDuration(verify)));
  else out.push(step("verify", "not_run"));

  const sealed = Boolean(r.seal.merkle_root);
  if (!nothingExtracted && sealed) out.push(step("seal", "done", plural(r.seal.parts_count, "part")));
  else out.push(step("seal", "not_run"));

  if (r.signoff.status === "signed") {
    out.push(step("sign", "done", r.signoff.signed_at ? formatDate(r.signoff.signed_at) : ""));
  } else if (r.verdict === "proven") {
    const signedRoles = new Set(r.signoff.approvals.map((a) => a.role)).size;
    const required = r.signoff.required_roles.length;
    out.push(step("sign", "waiting", required ? `${signedRoles} of ${required} signed` : ""));
  } else {
    out.push(step("sign", "not_run"));
  }
  return out;
}

/* ── Failures in the user's terms ───────────────────────────── */

/** The API's refusal when no export bucket is configured (exit_rehearsal.extract_destination_refusal). */
export function isNoExportBucket(reason: string | null | undefined): boolean {
  const text = String(reason || "");
  return /export bucket/i.test(text) && /none is configured|no export bucket/i.test(text);
}

export const NO_BUCKET_TITLE = "Extract failed: no export bucket is set for this workspace.";
export const NO_BUCKET_BODY = "An administrator needs to set one up. Then re-run the rehearsal.";
/** What a table that hit the export-bucket refusal says in its own row. */
export const NO_BUCKET_TABLE = "Not extracted: no export bucket is set for this workspace.";

function names(tables: RehearsalTable[]): string {
  const list = tables.map((t) => t.name);
  if (list.length <= 3) return list.join(", ");
  return `${list.slice(0, 3).join(", ")} and ${list.length - 3} more`;
}

export interface RehearsalProblem {
  tone: "danger" | "warning" | "held";
  title: string;
  /** One short line, or empty. */
  body: string;
  /** Server wording for administrators (env var names, references); empty when there is none. */
  adminDetail: string;
  /** The one failed table's extract job, when there is exactly one to open. */
  jobId: string | null;
  /** The failure is the missing export bucket: the per-table rows say so briefly. */
  noBucket: boolean;
}

/**
 * The single callout for a finished rehearsal that did not prove the exit, or
 * null (proven, running). Counts come from the completeness block, worded with
 * plural(); the API's own sentences stay for administrators.
 */
export function rehearsalProblem(r: ExitRehearsal): RehearsalProblem | null {
  const c = r.completeness;
  const declared = c.tables_declared;
  const failed = r.tables.filter((t) => t.status === "failed");
  if (r.verdict === "failed") {
    if (failed.length > 0 && failed.every((t) => isNoExportBucket(t.reason))) {
      return { tone: "danger", title: NO_BUCKET_TITLE, body: NO_BUCKET_BODY, adminDetail: failed[0].reason, jobId: null, noBucket: true };
    }
    const count = failed.length || c.tables_failed;
    const title = c.tables_extracted === 0
      ? `Extract failed: no table was extracted (${names(failed)}).`
      : `Extract failed for ${count.toLocaleString("en-US")} of ${plural(declared, "table")}: ${names(failed)}.`;
    const withJob = failed.filter((t) => t.job_id);
    const jobId = failed.length === 1 && withJob.length === 1 ? withJob[0].job_id : null;
    return {
      tone: "danger",
      title,
      body: jobId ? "The extract job has the error. Fix the cause, then re-run the rehearsal." : "Each failed table lists why below. Fix the cause, then re-run the rehearsal.",
      adminDetail: "",
      jobId,
      noBucket: false,
    };
  }
  if (r.verdict === "needs_pii_review") {
    const held = r.tables.filter((t) => t.status === "needs_pii_review");
    const n = held.length || (c.tables_needs_pii_review ?? 0);
    return {
      tone: "held",
      title: `The PII gate held ${plural(n, "table")}: ${names(held)}.`,
      body: `Nothing was bypassed. Acknowledge PII for ${r.system_name}, then re-run the rehearsal.`,
      adminDetail: "",
      jobId: null,
      noBucket: false,
    };
  }
  if (r.verdict === "incomplete") {
    const bad = r.tables.filter((t) => t.status === "incomplete");
    return {
      tone: "danger",
      title: `${(bad.length || c.tables_incomplete).toLocaleString("en-US")} of ${plural(declared, "table")} differ from the source: ${names(bad)}.`,
      body: "The differences are listed under each table.",
      adminDetail: "",
      jobId: null,
      noBucket: false,
    };
  }
  if (r.verdict === "not_proven") {
    return {
      tone: "warning",
      title: `${c.tables_proven.toLocaleString("en-US")} of ${plural(declared, "table")} proven; ${c.tables_not_proven.toLocaleString("en-US")} could not be proven in full.`,
      body: "Each table says why.",
      adminDetail: "",
      jobId: null,
      noBucket: false,
    };
  }
  return null;
}

/* ── Duration in one sentence ───────────────────────────────── */

export interface DurationSummary {
  /** One plain sentence. */
  text: string;
  /** Whether a measured duration exists (the bar and the breakdown are worth drawing). */
  measured: boolean;
  /** Whether it was compared with the window (only a proven run is). */
  compared: boolean;
  tone: "ok" | "bad" | "none";
}

export function durationSummary(r: ExitRehearsal): DurationSummary {
  const d = r.duration;
  const window = formatWindow(d.window_seconds);
  const windowWords = d.window_seconds > 0 ? `the ${window} window` : "a window (none is declared)";
  // A run that extracted nothing timed no exit, whatever its job clocks say (a held extract reads 0 s).
  if (d.measured_seconds == null || (r.verdict !== "running" && r.completeness.tables_extracted === 0)) {
    if (r.verdict === "running") return { text: `Measured when the rehearsal finishes, against ${windowWords}.`, measured: false, compared: false, tone: "none" };
    const why = r.completeness.tables_extracted === 0 ? "nothing was extracted" : "an extract job carries no start or end time";
    return { text: `Not measured: ${why}. The window is ${window}.`, measured: false, compared: false, tone: "none" };
  }
  const took = formatMeasured(d.measured_seconds);
  if (d.fits_window == null) {
    return { text: `Took ${took}. Only a proven rehearsal is compared with ${windowWords}.`, measured: true, compared: false, tone: "none" };
  }
  if (d.fits_window) {
    const spare = d.margin_pct == null ? "" : `, ${Math.floor(d.margin_pct)}% to spare`;
    return { text: `${took} of ${windowWords}: fits${spare}.`, measured: true, compared: true, tone: "ok" };
  }
  return {
    text: `${took} against ${windowWords}: over by ${formatDuration(d.measured_seconds - d.window_seconds)}.`,
    measured: true,
    compared: true,
    tone: "bad",
  };
}

/* ── Header ─────────────────────────────────────────────────── */

export type PrimaryAction = "rerun" | "pii" | "signoff" | null;

/**
 * The header's one primary action: review PII before anything else, re-run a
 * rehearsal that no longer proves the definition or did not prove the exit,
 * otherwise go to sign-off while a proven rehearsal awaits it.
 */
export function primaryAction(r: ExitRehearsal, opts: { canRerun: boolean; canReviewPii: boolean }): PrimaryAction {
  if (r.verdict === "running") return null;
  if (r.verdict === "needs_pii_review" && opts.canReviewPii) return "pii";
  const stale = r.current === false;
  if (opts.canRerun && (stale || r.verdict === "failed" || r.verdict === "incomplete" || r.verdict === "not_proven" || r.verdict === "needs_pii_review")) {
    return "rerun";
  }
  if (r.verdict === "proven" && r.signoff.status !== "signed") return "signoff";
  return null;
}

/** "Nov 19, 2026, 2:02 PM · took 22 s · by qa@example.com" parts, for the header's meta line. */
export function elapsedSeconds(r: Pick<ExitRehearsal, "started_at" | "finished_at">): number | null {
  if (!r.started_at || !r.finished_at) return null;
  const a = Date.parse(r.started_at);
  const b = Date.parse(r.finished_at);
  return Number.isFinite(a) && Number.isFinite(b) && b >= a ? (b - a) / 1000 : null;
}

/** The definition-changed notice, said once (<= 20 words). */
export const STALE_LINE = "The system's definition changed after this rehearsal, so it no longer proves the current one.";

/**
 * A refused start in the user's terms. The API answers 422 with the
 * export-bucket sentence before it records anything (D384); that sentence
 * names deployment settings, so it is kept for administrators.
 */
export function startErrorCopy(message: string): { title: string; body: string; adminDetail: string } {
  if (isNoExportBucket(message)) {
    return { title: "No export bucket is set for this workspace, so nothing can be extracted.", body: "An administrator needs to set one up first.", adminDetail: message };
  }
  return { title: "Could not start the rehearsal.", body: message, adminDetail: "" };
}

/**
 * Whether a changed definition demotes the status: only a proof goes stale
 * (Proven → Not proven). A failed or incomplete rehearsal keeps its own status,
 * so the board never reads "Failed, 0 of 1 table extracted" beside "Not proven".
 */
export function staleProof(r: { state: string; stale?: boolean | null }): boolean {
  return Boolean(r.stale) && r.state === "proven";
}
