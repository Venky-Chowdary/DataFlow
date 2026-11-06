/**
 * QA C9 / Mongo D9: an armed schedule with no next run is never a bare "—".
 * QA Browser 7: the drawer stopped showing Running and re-read History once
 * the run ended — guarded here on the rows the API actually returns around a
 * Run now / Sync (polled live: running → completed with `running: false`,
 * `current_run: null` and the finished record, with no scheduler beat needed).
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  ACTIVE_POLL_MS,
  IDLE_POLL_MS,
  destinationRecoveryOffered,
  historyRefreshKey,
  nextRunView,
  scheduleStatusBadge,
  schedulesPollMs,
} from "./scheduleRunStatus.js";
import type { PipelineSchedule } from "./types.js";

const armed = {
  id: "s1",
  enabled: true,
  state: "scheduled",
  stalled_reason: "",
  next_run_at: "2026-11-06T14:00:00Z",
  last_run_at: "2026-11-06T13:00:00Z",
  last_status: "completed",
  run_count: 1,
  running: false,
  current_run: null,
} as unknown as PipelineSchedule;

describe("nextRunView", () => {
  it("formats a real next run", () => {
    assert.deepEqual(nextRunView(armed), { label: null, title: "", stalled: false });
  });

  it("says Held with the reason when the schedule is stalled", () => {
    const view = nextRunView({
      ...armed,
      state: "stalled",
      next_run_at: null,
      stalled_reason: "Parked on a finding (DESTINATION_CONTINUITY): the table no longer exists.",
    } as PipelineSchedule);
    assert.equal(view.stalled, true);
    assert.equal(view.label, "Held — action needed");
    assert.match(view.title, /DESTINATION_CONTINUITY/);
  });

  it("says Due now, not a dash, for an armed schedule without a stored slot", () => {
    assert.equal(nextRunView({ ...armed, next_run_at: null } as PipelineSchedule).label, "Due now");
  });

  it("says Paused for a disabled schedule", () => {
    assert.equal(nextRunView({ ...armed, enabled: false, state: "paused", next_run_at: null } as PipelineSchedule).label, "Paused");
  });
});

describe("destinationRecoveryOffered", () => {
  it("is offered for an open DESTINATION_CONTINUITY finding (detail and list shapes)", () => {
    const detail = { ...armed, approval_request: { code: "DESTINATION_CONTINUITY", status: "open" } };
    assert.equal(destinationRecoveryOffered(detail as unknown as PipelineSchedule), true);
    const summary = { ...armed, needs_approval: true, approval_code: "DESTINATION_CONTINUITY" };
    assert.equal(destinationRecoveryOffered(summary as unknown as PipelineSchedule), true);
  });

  it("is not offered for another finding or a resolved one", () => {
    const other = { ...armed, needs_approval: true, approval_code: "SOURCE_DRIFT" };
    assert.equal(destinationRecoveryOffered(other as unknown as PipelineSchedule), false);
    const resolved = { ...armed, approval_request: { code: "DESTINATION_CONTINUITY", status: "approved" } };
    assert.equal(destinationRecoveryOffered(resolved as unknown as PipelineSchedule), false);
  });
});

describe("drawer after a sync finishes (QA Browser 7)", () => {
  const during = {
    ...armed,
    running: true,
    last_status: "running",
    run_count: 1,
    current_run: { run_id: "r2", status: "running", job_id: "j2" },
  } as unknown as PipelineSchedule;
  const after = {
    ...armed,
    running: false,
    last_status: "completed",
    last_run_at: "2026-11-06T13:05:03Z",
    run_count: 2,
    current_run: null,
  } as unknown as PipelineSchedule;

  it("polls fast while the run is open and slow once it ends", () => {
    assert.equal(schedulesPollMs([during]), ACTIVE_POLL_MS);
    assert.equal(schedulesPollMs([after]), IDLE_POLL_MS);
  });

  it("drops the Running badge on the first read after the run ends", () => {
    assert.equal(scheduleStatusBadge(during)?.label, "Running");
    assert.notEqual(scheduleStatusBadge(after)?.label, "Running");
    assert.equal(scheduleStatusBadge(after)?.status, "completed");
  });

  it("re-keys History so the finished run is re-read at once", () => {
    assert.notEqual(historyRefreshKey(during), historyRefreshKey(after));
  });
});
