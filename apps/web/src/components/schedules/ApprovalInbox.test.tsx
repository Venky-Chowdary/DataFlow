import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import type { ScheduleApprovalInboxItem } from "../../lib/types";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  MutationObserver: dom.window.MutationObserver,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);

let root: Root | null = null;
let host: HTMLDivElement | null = null;
const item = (overrides: Partial<ScheduleApprovalInboxItem> = {}): ScheduleApprovalInboxItem => ({
  schedule_id: "schedule-a",
  schedule_name: "Orders Postgres → orders_feed",
  workspace_id: "workspace-a",
  source: "Orders Postgres (demo)",
  destination: "orders_feed",
  sync_mode: "upsert",
  enabled: true,
  approval: {
    id: "approval-a",
    status: "open",
    kind: "run_refused",
    code: "RUN_REFUSED",
    finding: "Upsert needs a business key. Declare a primary key, or choose append to allow duplicate rows.",
    corrective_action: "Open Transfer Studio and declare a primary key.",
    approvable: false,
    requested_scopes: [],
    occurrences: 2,
    created_at: "2026-11-08T00:00:00Z",
  },
  ...overrides,
});

async function render(
  items: ScheduleApprovalInboxItem[],
  options: {
    compact?: boolean;
    onRunNow?: (scheduleId: string) => void;
    onOpenStudio?: (scheduleId: string) => void;
    onReviewMapping?: (scheduleId: string) => void;
  } = {},
) {
  const { act, createElement, useState } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { ToastProvider } = await import("../Toast");
  const { ApprovalInbox } = await import("./ApprovalInbox");
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  function InboxHarness() {
    const [hidden, setHidden] = useState(false);
    if (hidden) {
      return createElement("button", { type: "button", onClick: () => setHidden(false) }, `Decisions (${items.length})`);
    }
    return createElement(ApprovalInbox, {
      items,
      onDecided: () => {},
      onDismiss: () => setHidden(true),
      compact: options.compact,
      onRunNow: options.onRunNow,
      onOpenStudio: options.onOpenStudio,
      onReviewMapping: options.onReviewMapping,
    });
  }
  await act(async () => root!.render(createElement(ToastProvider, null, createElement(InboxHarness))));
}

function button(name: string | RegExp) {
  const match = Array.from(host!.querySelectorAll<HTMLButtonElement>("button"))
    .find((candidate) => {
      const text = candidate.textContent?.trim() || "";
      const label = candidate.getAttribute("aria-label") || "";
      return typeof name === "string"
        ? text === name || label === name
        : name.test(text) || name.test(label);
    });
  assert.ok(match, `Expected button ${String(name)}`);
  return match;
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
});

describe("schedule decision inbox", () => {
  it("minimizes and closes without resolving findings, then restores them", async () => {
    await render([item()]);
    assert.ok(host!.querySelector(".df2-approval-list"));

    const { act } = await import("react");
    await act(async () => button("Minimize").click());
    assert.equal(host!.querySelector(".df2-approval-list"), null);
    assert.match(host!.textContent || "", /1 open decision/);

    await act(async () => button("Expand").click());
    assert.ok(host!.querySelector(".df2-approval-list"));
    await act(async () => button("Close waiting on a decision panel").click());
    assert.equal(host!.querySelector(".df2-approval-inbox"), null);
    await act(async () => button(/Decisions \(1\)/).click());
    assert.ok(host!.querySelector(".df2-approval-inbox"));
    assert.ok(host!.querySelector(".df2-approval-list"));
    assert.match(host!.textContent || "", /Upsert needs a business key/);
  });

  it("keeps the explicit run-now action beside the destination continuity finding", async () => {
    const runNow: string[] = [];
    const parked = item({
      approval: {
        ...item().approval,
        id: "dest-hash",
        finding: "Decision Artifact content_hash mismatch — Map/DDL drifted since Validate.",
        corrective_action: "Run now if Map is unchanged.",
      },
    });
    await render([parked], { compact: true, onRunNow: (scheduleId) => runNow.push(scheduleId) });
    assert.ok(host!.querySelector(".df2-approval-inbox.is-compact"));
    assert.match(host!.textContent || "", /Decision Artifact content_hash mismatch/);
    assert.match(host!.textContent || "", /Run now if Map is unchanged/);
    const { act } = await import("react");
    await act(async () => button("Run now").click());
    assert.deepEqual(runNow, ["schedule-a"]);
  });

  it("offers Transfer Studio for a missing upsert business key", async () => {
    const opened: string[] = [];
    await render([item()], { compact: true, onOpenStudio: (scheduleId) => opened.push(scheduleId) });
    assert.match(host!.textContent || "", /declare a primary key for upsert or skip-duplicates/i);
    const { act } = await import("react");
    await act(async () => button("Open Transfer Studio").click());
    assert.deepEqual(opened, ["schedule-a"]);
  });

  it("names the route by connection and offers connection tests for a DNS finding (D435)", async () => {
    const dns = item({
      source: "c81fd66d-be79-4cf2-8720-5987b9e26069:r72_up_src2",
      destination: "42ca781c-83ec-4afa-9dba-60bf69983083:R72_UP_ORA2",
      source_connector: { id: "c81fd66d-be79-4cf2-8720-5987b9e26069", name: "QAR72-MY", type: "mysql", deleted: false },
      source_table: "r72_up_src2",
      destination_connector: { id: "42ca781c-83ec-4afa-9dba-60bf69983083", name: "QAR72-ORA", type: "oracle", deleted: false },
      dest_table: "R72_UP_ORA2",
      remedy: {
        target: "connection",
        code: "network.dns_not_found",
        corrective_action: "Check the spelling of the host name on the connection. Open the schedule's connection (Test, or Edit connection) to confirm it connects, then run the schedule again.",
      },
      approval: {
        ...item().approval,
        id: "dns",
        finding: "Host not found: the connection's host name didn't resolve (DNS).",
        corrective_action: "Check the spelling of the host name on the connection.",
      },
    });
    await render([dns]);
    const route = host!.querySelector(".df2-approval-route")!;
    assert.match(route.textContent || "", /QAR72-MY · r72_up_src2.*QAR72-ORA · R72_UP_ORA2/);
    assert.doesNotMatch(route.textContent || "", /c81fd66d/);
    assert.match(route.querySelector(".df2-approval-route-end")!.getAttribute("title") || "", /c81fd66d/);
    assert.doesNotMatch(host!.textContent || "", /Open Validate/);
    assert.ok(button("Test source connection"));
    assert.ok(button("Test destination connection"));
    assert.equal(
      Array.from(host!.querySelectorAll("button")).some((b) => b.textContent?.trim() === "Decide"),
      false,
    );
  });

  it("says a deleted connection is deleted and offers to re-select it (D435)", async () => {
    const reselect: string[] = [];
    const orphan = item({
      source_connector: { id: "gone-1", name: "", type: "", deleted: true },
      source_table: "orders",
      remedy: {
        target: "connection_deleted",
        sides: "source",
        corrective_action: "Connection deleted: the source connection of this schedule no longer exists. Re-select the connection on this schedule, or restore the deleted connection, then resume.",
      },
    });
    await render([orphan], { onReviewMapping: (id) => reselect.push(id) });
    assert.match(host!.textContent || "", /Connection deleted · orders/);
    assert.match(host!.textContent || "", /Re-select the connection on this schedule/);
    const { act } = await import("react");
    await act(async () => button("Re-select connection").click());
    assert.deepEqual(reselect, ["schedule-a"]);
  });
});
