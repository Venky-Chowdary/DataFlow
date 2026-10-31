/**
 * QA (557f556c): "Schedule drawer does not auto-refresh: the last run, next
 * run and History stay stale until the page is reloaded."
 *
 * Round 5 polls the list (which feeds the drawer's last/next run) and History
 * on its own timer. Two gaps remained: History, idle-polling every 30 s, did
 * not re-read when the schedule it belongs to started or finished a run; and
 * a single failed background read stopped both timers for good (see
 * poller.test.ts).
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { historyRefreshKey } from "./scheduleRunStatus.js";
import type { PipelineSchedule } from "./types.js";

const base = {
  id: "s1",
  last_run_at: "2026-10-30T09:00:00Z",
  last_status: "succeeded",
  next_run_at: "2026-10-30T10:00:00Z",
  run_count: 3,
  current_run: null,
} as unknown as PipelineSchedule;

describe("historyRefreshKey", () => {
  it("changes when a run starts", () => {
    const running = { ...base, current_run: { run_id: "r4", status: "running" } } as unknown as PipelineSchedule;
    assert.notEqual(historyRefreshKey(base), historyRefreshKey(running));
  });

  it("changes when a run finishes", () => {
    const done = { ...base, last_run_at: "2026-10-30T10:00:05Z", last_status: "failed", run_count: 4 } as unknown as PipelineSchedule;
    assert.notEqual(historyRefreshKey(base), historyRefreshKey(done));
  });

  it("does not change on an unrelated re-read", () => {
    assert.equal(historyRefreshKey(base), historyRefreshKey({ ...base } as PipelineSchedule));
  });
});
