import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  ACTIVE_POLL_MS,
  IDLE_POLL_MS,
  historyPollMs,
  runLabel,
  runReason,
  scheduleStatusBadge,
  schedulesPollMs,
  skippedBeatNote,
} from "./scheduleRunStatus.js";
import type { PipelineSchedule, ScheduleRun } from "./types.js";

/**
 * QA round 5 — "Scheduled runs fail silently or show stale status".
 * The card shows the status the API derived from the latest run record, with
 * its reason, and the page keeps re-reading so a run that ends or dies is
 * shown without a manual reload.
 */
function sched(extra: Partial<PipelineSchedule>): PipelineSchedule {
  return {
    id: "s1",
    name: "nightly",
    enabled: true,
    running: false,
    last_status: null,
    ...extra,
  } as PipelineSchedule;
}

function run(extra: Partial<ScheduleRun>): ScheduleRun {
  return {
    job_id: "",
    status: "completed",
    attempt: 0,
    started_at: "",
    finished_at: "",
    duration_seconds: 0,
    records_transferred: 0,
    rejected_rows: 0,
    coerced_null_rows: 0,
    error: "",
    ...extra,
  } as ScheduleRun;
}

describe("scheduleStatusBadge", () => {
  it("shows a worker-lost failure with its reason, not a stale success", () => {
    const badge = scheduleStatusBadge(
      sched({ last_status: "failed", last_status_reason: "Worker lost: the worker running job j1 stopped" }),
    );
    assert.ok(badge);
    assert.equal(badge.label, "Failed");
    assert.match(badge.className, /df2-badge-error/);
    assert.match(badge.title, /Worker lost/);
  });

  it("shows the run in flight from current_run", () => {
    const badge = scheduleStatusBadge(
      sched({ last_status: "completed", current_run: { run_id: "r2", status: "pending", job_id: "" } }),
    );
    assert.equal(badge?.label, "Starting");
  });

  it("puts an open approval first", () => {
    const badge = scheduleStatusBadge(
      sched({ last_status: "completed", needs_approval: true, approval_finding: "drift on col a" }),
    );
    assert.equal(badge?.status, "needs_approval");
    assert.equal(badge?.title, "drift on col a");
  });

  it("has no badge before the first run", () => {
    assert.equal(scheduleStatusBadge(sched({})), null);
  });
});

describe("skipped beats", () => {
  it("names the reason and how often it repeated", () => {
    assert.equal(
      skippedBeatNote(sched({ last_skip: { reason: "Waiting on an approval: x", reason_code: "approval_pending", occurrences: 3 } })),
      "Skipped 3×: Waiting on an approval: x",
    );
    assert.equal(skippedBeatNote(sched({})), "");
    assert.equal(runLabel(run({ status: "skipped", occurrences: 2 })), "Skipped ×2");
  });

  it("a history row states its reason even with no job error", () => {
    assert.equal(runReason(run({ status: "failed", error: "", reason: "Worker lost: no job" })), "Worker lost: no job");
    assert.equal(runReason(run({ status: "failed", error: "boom" })), "boom");
  });
});

describe("polling", () => {
  it("re-reads fast while a run is open and slowly at rest", () => {
    assert.equal(historyPollMs([run({ status: "running" })]), ACTIVE_POLL_MS);
    assert.equal(historyPollMs([run({ status: "failed" })]), IDLE_POLL_MS);
    assert.equal(schedulesPollMs([sched({ running: true })]), ACTIVE_POLL_MS);
    assert.equal(schedulesPollMs([sched({ current_run: { run_id: "r", status: "pending" } })]), ACTIVE_POLL_MS);
    assert.equal(schedulesPollMs([sched({ last_status: "failed" })]), IDLE_POLL_MS);
    assert.ok(IDLE_POLL_MS > 0, "the list never stops re-reading");
  });
});
