/**
 * Per-table progress on the Run screen (UI pass 2, item 3).
 * Run: npx tsx --test apps/web/src/lib/tableProgress.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TableProgressList } from "../components/transfer/TableProgressList.tsx";
import { orderTableRows, tableProgressRows, tableStateAnnouncements } from "./tableProgress.ts";

const names = ["people", "orders", "items"];
const base = { stream_names: names, records_processed: 0 } as const;
const states = (rows: ReturnType<typeof tableProgressRows>) => rows.map((r) => `${r.name}:${r.state}:${r.read}/${r.write}/${r.verify}:${r.rows ?? "-"}`);

describe("tableProgressRows", () => {
  it("is empty for a single-table run", () => {
    assert.deepEqual(tableProgressRows({ status: "running", stream_names: ["only"] }), []);
  });

  it("shows the finished table, the one loading now and the queued one", () => {
    const rows = tableProgressRows({
      ...base, status: "running", records_processed: 40,
      stream_progress: {
        people: { status: "completed", rows: 6 },
        orders: { status: "running", counter_at_start: 6 },
      },
    });
    assert.deepEqual(states(rows), [
      "people:loaded:done/done/waiting:6",
      "orders:loading:active/active/waiting:40",
      "items:queued:waiting/waiting/waiting:-",
    ]);
  });

  it("does not show the previous table's count before the new table's first batch", () => {
    const rows = tableProgressRows({
      ...base, status: "running", records_processed: 6,
      stream_progress: { people: { status: "completed", rows: 6 }, orders: { status: "running", counter_at_start: 6 } },
    });
    assert.equal(rows[1].rows, null);
  });

  it("during reconcile only the table Gate-8 is comparing reads Verifying; the others Not compared", () => {
    const loaded = { people: { status: "completed", rows: 6 }, orders: { status: "completed", rows: 3 }, items: { status: "completed", rows: 5 } };
    const verifying = tableProgressRows({ ...base, status: "running", phase: "reconcile", stream_progress: loaded }, { reconciling: true });
    assert.deepEqual(states(verifying), [
      "people:loaded:done/done/not_compared:6",
      "orders:loaded:done/done/not_compared:3",
      "items:verifying:done/done/active:5",
    ]);
  });

  // Review UI2 #1: Gate-8 compares the last table of a multi-table run only.
  const streams = [{ name: "people", status: "completed", records_processed: 6 }, { name: "orders", status: "completed", records_processed: 3 }, { name: "items", status: "completed", records_processed: 5 }];
  it("marks Verify done only on the table Gate-8 compared; the others read Not compared", () => {
    const finished = tableProgressRows({ ...base, status: "completed", reconciliation: { passed: true, compared_stream: "items" }, destination_summary: { streams } });
    assert.deepEqual(states(finished), ["people:loaded:done/done/not_compared:6", "orders:loaded:done/done/not_compared:3", "items:checked:done/done/done:5"]);
    const older = tableProgressRows({ ...base, status: "completed", reconciliation: { passed: true }, destination_summary: { streams } });
    assert.deepEqual(older.map((r) => r.verify), ["not_compared", "not_compared", "done"], "no compared_stream: the last table");
  });

  it("shows a recorded mismatch on the compared table, never all green", () => {
    const rows = tableProgressRows({ ...base, status: "completed_with_warnings", reconciliation: { passed: false, compared_stream: "items" }, destination_summary: { streams } });
    assert.deepEqual(states(rows).at(-1), "items:mismatch:done/done/failed:5");
    assert.ok(rows.every((r) => r.state !== "checked"));
  });

  it("a resumed table shows the rows its earlier pass wrote", () => {
    const resumed = tableProgressRows({ ...base, status: "completed", reconciliation: { passed: true, compared_stream: "items" },
      destination_summary: { streams: [{ name: "people", status: "completed", records_processed: 0, rows_from_earlier_pass: 6 }, ...streams.slice(1)] } });
    assert.equal(resumed[0].rows, 6);
    assert.equal(resumed[0].rowsFromEarlierPass, true);
  });

  // Review UI2 #7: undo drops earlier tables' records; the stopped table is named.
  it("blames the table the run stopped on, from the record written with the failed status", () => {
    const rows = tableProgressRows({ ...base, status: "failed", stream_progress: {}, error_details: {
      stopped_stream: "orders",
      multi_stream_undo: [{ table: "people", stream: "people", decision: "restored", note: "restored from its pre-run backup" }],
    } });
    assert.deepEqual(states(rows), [
      "people:rolled_back:done/undone/not_run:-",
      "orders:failed:failed/failed/not_run:-",
      "items:not_run:not_run/not_run/not_run:-",
    ]);
  });

  it("a backup that could not be restored reads Restore failed with the reason, never Rolled back", () => {
    const rows = tableProgressRows({ ...base, status: "failed", stream_progress: {}, error_details: {
      stopped_stream: "orders",
      multi_stream_undo: [{ table: "people", decision: "restore_failed", note: "its pre-run backup could not be restored; the backup was kept" }],
    } });
    assert.equal(rows[0].state, "restore_failed");
    assert.match(rows[0].note || "", /could not be restored/);
  });

  it("an older job's undo entries (destination tables only) are matched to stream names", () => {
    const rows = tableProgressRows({ ...base, status: "failed", stream_progress: {}, error_details: {
      stopped_stream: "orders",
      multi_stream_undo: [{ table: "public.people", decision: "cleared" }],
    } });
    assert.equal(rows[0].state, "rolled_back");
  });

  it("after a failure with no stop record, stages after the stop read Not run, never Done", () => {
    const rows = tableProgressRows({ ...base, status: "failed", stream_progress: { people: { status: "completed", rows: 6 } } });
    assert.deepEqual(states(rows), [
      "people:loaded:done/done/not_run:6",
      "orders:failed:failed/failed/not_run:-",
      "items:not_run:not_run/not_run/not_run:-",
    ]);
    const cancelled = tableProgressRows({ ...base, status: "cancelled", error_details: { stopped_stream: "orders" }, stream_progress: { people: { status: "completed", rows: 6 } } });
    assert.equal(cancelled[1].state, "stopped");
  });
});

describe("list order and announcements", () => {
  it("puts active tables first and collapses the done ones", () => {
    const rows = tableProgressRows({
      ...base, status: "running", records_processed: 9,
      stream_progress: { people: { status: "completed", rows: 6 }, orders: { status: "running", counter_at_start: 6 } },
    });
    const { active, done } = orderTableRows(rows);
    assert.deepEqual(active.map((r) => r.name), ["orders", "items"]);
    assert.deepEqual(done.map((r) => r.name), ["people"]);
    const html = renderToStaticMarkup(React.createElement(TableProgressList, { rows }));
    assert.match(html, /1 of 3 tables loaded/);
    assert.match(html, /aria-expanded="false"[^>]*>.*1 done/);
    assert.match(html, /Write<span class="df2-sr-only">: in progress<\/span>/);
    assert.match(html, /9 rows written/);
  });

  it("announces state changes only", () => {
    const rows = tableProgressRows({ ...base, status: "running", stream_progress: { people: { status: "completed", rows: 6 } } });
    assert.deepEqual(tableStateAnnouncements(new Map([["people", "loading"], ["orders", "queued"]]), rows), ["people: loaded"]);
  });
});
