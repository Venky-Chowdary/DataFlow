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

  it("Verify runs for every loaded table during reconcile, and is done once Gate-8 recorded", () => {
    const loaded = { people: { status: "completed", rows: 6 }, orders: { status: "completed", rows: 3 }, items: { status: "completed", rows: 5 } };
    const verifying = tableProgressRows({ ...base, status: "running", phase: "reconcile", stream_progress: loaded }, { reconciling: true });
    assert.ok(verifying.every((r) => r.state === "verifying" && r.verify === "active"));
    const finished = tableProgressRows({
      ...base, status: "completed", reconciliation: { passed: true },
      destination_summary: { streams: [{ name: "people", status: "completed", records_processed: 6 }, { name: "orders", status: "completed", records_processed: 3 }, { name: "items", status: "completed", records_processed: 5 }] },
    });
    assert.deepEqual(states(finished), ["people:checked:done/done/done:6", "orders:checked:done/done/done:3", "items:checked:done/done/done:5"]);
  });

  it("after a failure, stages after the stop read Not run, never Done", () => {
    const rows = tableProgressRows({ ...base, status: "failed", stream_progress: { people: { status: "completed", rows: 6 } } });
    assert.deepEqual(states(rows), [
      "people:loaded:done/done/not_run:6",
      "orders:failed:failed/failed/not_run:-",
      "items:not_run:not_run/not_run/not_run:-",
    ]);
    const cancelled = tableProgressRows({ ...base, status: "cancelled", stream_progress: { people: { status: "completed", rows: 6 } } });
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
