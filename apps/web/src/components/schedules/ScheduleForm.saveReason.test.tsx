/**
 * QA D344 / QA batch 1 item 6: the form says why Save is disabled and marks
 * the field it needs; a new schedule without a mapping contract asks the API
 * to plan it (mapping + Validate) instead of saving a paused draft.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ScheduleForm } from "./ScheduleForm.js";
import type { Connector, PipelineSchedule } from "../../lib/types.js";
import { scheduleCreatePlanFields } from "../../lib/scheduleFormReadiness.js";

const connectors = [
  { id: "src", name: "Orders DB", type: "postgresql" },
  { id: "dst", name: "Warehouse", type: "mysql" },
] as unknown as Connector[];

function schedule(over: Partial<PipelineSchedule> = {}): PipelineSchedule {
  return {
    id: "sch_1", name: "Nightly orders", source_connector_id: "src", source_table: "orders",
    dest_connector_id: "dst", dest_table: "orders_copy", interval: "daily", cron: "", timezone: "UTC",
    sync_mode: "incremental_append", validation_mode: "strict", schema_policy: "manual_review",
    backfill_new_fields: false, cursor_column: "", primary_key: "", cursor_value: "", workspace_id: "ws1",
    max_retries: 0, retry_backoff_seconds: 60, notify_on_failure: true, notify_on_success: false,
    enabled: true, last_run_at: null, next_run_at: null, last_job_id: null, last_status: null,
    run_count: 0, running: false, created_at: "2026-11-01T00:00:00Z", duplicate_policy: "upsert",
    mappings: [{ source: "id", target: "id" }], mapping_count: 1,
    ...over,
  } as PipelineSchedule;
}

describe("schedule form: why Save is disabled (D344)", () => {
  it("states the missing primary key beside the disabled button and marks the field", () => {
    const html = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={schedule()} onSubmit={() => {}} onCancel={() => {}} />,
    );
    assert.match(html, /id="sched-save-why"[^>]*>To save: enter the primary key, or set Duplicate policy to Append\.</);
    assert.match(html, /aria-describedby="sched-save-why"/);
    assert.match(html, /Primary key \(required\)/);
    assert.match(html, /id="sched-pk"[^>]*aria-invalid="true"/);
  });

  it("shows no reason once the form can be saved", () => {
    const html = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={schedule({ primary_key: "id" })} onSubmit={() => {}} onCancel={() => {}} />,
    );
    assert.doesNotMatch(html, /sched-save-why/);
    assert.doesNotMatch(html, /aria-invalid="true"/);
  });

  it("an append schedule does not require a key", () => {
    const html = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={schedule({ duplicate_policy: "append" })} onSubmit={() => {}} onCancel={() => {}} />,
    );
    assert.doesNotMatch(html, /sched-save-why/);
  });
});

describe("schedule form: a new schedule is planned, not parked (QA batch 1)", () => {
  it("asks the API to plan a new schedule that has no mapping contract", () => {
    assert.deepEqual(scheduleCreatePlanFields(false, 0), { plan_mappings: true });
    assert.deepEqual(scheduleCreatePlanFields(false, 3), {});
    assert.deepEqual(scheduleCreatePlanFields(true, 0), {});
  });

  it("the new-schedule form says Save plans the route", () => {
    const html = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={null} onSubmit={() => {}} onCancel={() => {}} />,
    );
    assert.match(html, /Save plans this schedule the way Transfer Studio does/);
    assert.doesNotMatch(html, /Save creates a paused draft/);
  });

  it("offers Plan mappings on a mapping-less schedule being edited", () => {
    const html = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={schedule({ primary_key: "id", mappings: [], mapping_count: 0 })}
        onSubmit={() => {}} onCancel={() => {}} onPlanMappings={() => {}} onOpenStudio={() => {}} />,
    );
    assert.match(html, />Plan mappings</);
    assert.match(html, /Run now cannot start it/);
  });
});
