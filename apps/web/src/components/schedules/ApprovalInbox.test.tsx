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
  } = {},
) {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { ToastProvider } = await import("../Toast");
  const { ApprovalInbox } = await import("./ApprovalInbox");
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(createElement(ToastProvider, null, createElement(ApprovalInbox, {
    items,
    onDecided: () => {},
    compact: options.compact,
    onRunNow: options.onRunNow,
    onOpenStudio: options.onOpenStudio,
  }))));
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
    assert.equal(host!.querySelector(".df2-approval-list"), null);
    await act(async () => button(/Show decisions/).click());
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
});