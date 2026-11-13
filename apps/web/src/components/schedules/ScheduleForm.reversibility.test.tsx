/**
 * Schedules carry `require_reversibility` (contract 10.1.9): the form offers it
 * beside the signed-contract requirement, saves it, and the pipeline drawer
 * shows it the way it shows the contract requirement.
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ScheduleForm } from "./ScheduleForm.js";
import { PipelineDetailDrawer } from "../PipelineDetailDrawer.js";
import type { Connector, PipelineSchedule, ScheduleInput } from "../../lib/types.js";

const LABEL = "Require reversibility before each run (fail-closed)";

function schedule(over: Partial<PipelineSchedule> = {}): PipelineSchedule {
  return {
    id: "sch_1",
    name: "Nightly orders",
    source_connector_id: "src",
    source_table: "orders",
    dest_connector_id: "dst",
    dest_table: "orders_copy",
    interval: "daily",
    cron: "",
    timezone: "UTC",
    sync_mode: "full_refresh",
    validation_mode: "strict",
    schema_policy: "fail",
    backfill_new_fields: false,
    cursor_column: "",
    primary_key: "",
    cursor_value: "",
    workspace_id: "ws1",
    max_retries: 2,
    retry_backoff_seconds: 30,
    notify_on_failure: true,
    notify_on_success: false,
    enabled: true,
    last_run_at: null,
    next_run_at: null,
    last_job_id: null,
    last_status: null,
    run_count: 0,
    running: false,
    created_at: "2026-11-01T00:00:00Z",
    duplicate_policy: "append",
    ...over,
  } as PipelineSchedule;
}

const connectors = [
  { id: "src", name: "Orders DB", type: "postgresql" },
  { id: "dst", name: "Warehouse", type: "postgresql" },
] as unknown as Connector[];

function checkbox(html: string): string {
  const at = html.indexOf(LABEL);
  assert.ok(at > 0, "the form offers the reversibility requirement");
  const input = html.slice(html.lastIndexOf("<input", at), at);
  return input;
}

describe("schedule form: require reversibility", () => {
  it("is offered, off by default, and on for a schedule that requires it", () => {
    const off = renderToStaticMarkup(<ScheduleForm connectors={connectors} intervals={null} initial={schedule()} onSubmit={() => {}} onCancel={() => {}} />);
    assert.doesNotMatch(checkbox(off), /checked/);
    const on = renderToStaticMarkup(
      <ScheduleForm connectors={connectors} intervals={null} initial={schedule({ require_reversibility: true })} onSubmit={() => {}} onCancel={() => {}} />,
    );
    assert.match(checkbox(on), /checked/);
    assert.match(on, /Blocks a scheduled run unless every table it writes can give the source data back/);
  });
});

describe("schedule form: saving the requirement (jsdom)", () => {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
  const saved = { fetch: globalThis.fetch };

  before(() => {
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
      IS_REACT_ACT_ENVIRONMENT: true,
    });
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
    // The shared Drawer focuses its panel on the next frame.
    Object.assign(dom.window, {
      requestAnimationFrame: (cb: FrameRequestCallback) => setTimeout(() => cb(0), 0) as unknown as number,
      cancelAnimationFrame: (id: number) => clearTimeout(id),
    });
    globalThis.fetch = (async () => new Response("{}", { status: 200 })) as typeof fetch;
  });
  after(() => {
    globalThis.fetch = saved.fetch;
  });

  it("sends require_reversibility with the schedule", async () => {
    const { createRoot } = await import("react-dom/client");
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    const submitted: Array<Partial<ScheduleInput>> = [];
    await React.act(async () => {
      root.render(<ScheduleForm connectors={connectors} intervals={null} initial={schedule()} onSubmit={(v) => submitted.push(v)} onCancel={() => {}} />);
    });
    const box = [...host.querySelectorAll("label")].find((l) => l.textContent?.includes(LABEL))?.querySelector("input");
    assert.ok(box);
    await React.act(async () => {
      box!.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
    });
    const form = host.querySelector("form")!;
    await React.act(async () => {
      form.dispatchEvent(new dom.window.Event("submit", { bubbles: true, cancelable: true }));
    });
    assert.equal(submitted.length, 1);
    assert.equal(submitted[0].require_reversibility, true);
    await React.act(async () => root.unmount());
    host.remove();
  });

  it("the pipeline drawer reads Required · enforced, or Not required, beside the contract", async () => {
    const { createRoot } = await import("react-dom/client");
    for (const [sched, expected] of [
      [schedule({ require_reversibility: true }), /Reversibility\s*Required · enforced/],
      [schedule(), /Reversibility\s*Not required/],
    ] as const) {
      const host = document.createElement("div");
      document.body.appendChild(host);
      const root = createRoot(host);
      await React.act(async () => {
        root.render(
          <PipelineDetailDrawer open schedule={sched} tab="Overview" setTab={() => {}} onClose={() => {}} onRun={() => {}}
            onEdit={() => {}} onDelete={() => {}} onToggle={() => {}} />,
        );
      });
      const facts = document.body.querySelector('[aria-label="Pipeline summary"]');
      assert.ok(facts, "the drawer shows its facts");
      assert.match(facts!.textContent || "", expected);
      assert.match(facts!.textContent || "", /Data contract/);
      await React.act(async () => root.unmount());
      host.remove();
    }
  });
});

