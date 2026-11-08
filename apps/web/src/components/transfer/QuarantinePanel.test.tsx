import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
  localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
  MutationObserver: dom.window.MutationObserver,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);

type Request = { url: string; method: string; resolve: (response: Response) => void; done: boolean };
const requests: Request[] = [];
globalThis.fetch = ((input: RequestInfo | URL, options?: RequestInit) => new Promise<Response>((resolve) => {
  requests.push({ url: String(input), method: options?.method || "GET", resolve, done: false });
})) as typeof fetch;
let root: Root | null = null;
let host: HTMLElement | null = null;
const finding = { row: 2, column: "amount", value: "bad", reason: "Invalid integer", values: { id: "2", amount: "bad" } };

async function render(jobId: string, completed: string[]) {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { ToastProvider } = await import("../Toast");
  const { QuarantinePanel } = await import("./QuarantinePanel");
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(createElement(ToastProvider, null, createElement(QuarantinePanel, {
    jobId, initiallyOpen: true, onReplayComplete: (child) => completed.push(child),
  }))));
}

async function answer(request: Request, body: Record<string, unknown>, status = 200, includeReplayEligibility = true) {
  const { act } = await import("react");
  await act(async () => {
    request.done = true;
    const responseBody = request.method === "GET" && includeReplayEligibility
      && !Object.prototype.hasOwnProperty.call(body, "replay_block_reason")
      ? { ...body, replay_block_reason: null }
      : body;
    request.resolve(new Response(JSON.stringify(responseBody), { status }));
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

function pending(jobId: string, method = "GET") {
  const request = requests.find((item) => !item.done && item.method === method && item.url.includes(`/jobs/${jobId}/quarantine`));
  assert.ok(request, `No ${method} request for ${jobId}`);
  return request;
}

function replayButton() {
  return Array.from(host!.querySelectorAll<HTMLButtonElement>("button"))
    .find((button) => /Replay quarantined rows|Promote \/ Replay rows/.test(button.textContent || ""));
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  for (const request of requests.filter((item) => !item.done)) await answer(request, {});
  requests.length = 0;
  root = null;
  host?.remove();
  host = null;
});

describe("quarantine recovery evidence and job isolation", () => {
  it("clears a resolved ledger-refresh warning after a successful manual refresh", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    await answer(pending("job-a", "POST"), {
      success: true, job_id: "child", rows_written: 1, rejected: 0, reconciliation: { passed: true },
      quarantine_closure: { verdict: "closed", open_count: 0 },
    });
    await answer(pending("job-a"), { detail: "Unavailable" }, 500);
    assert.match(host!.textContent || "", /could not be verified/);
    const refresh = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Refresh")!;
    await act(async () => refresh.click());
    await answer(pending("job-a"), {
      quarantine: [{ ...finding, retry_status: "promoted" }], source: "dlq",
      quarantine_closure: { verdict: "closed", open_count: 0, promoted_count: 1 },
    });
    assert.doesNotMatch(host!.textContent || "", /could not be verified/);
  });
  for (const failure of ["promotion", "diverging"] as const) {
    it(`keeps verified child data separate from ${failure} failure`, async () => {
      const completed: string[] = [];
      await render("job-a", completed);
      await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
      const { act } = await import("react");
      await act(async () => replayButton()!.click());
      await answer(pending("job-a", "POST"), {
        success: true, job_id: "child", rows_written: 1, rejected: 0, reconciliation: { passed: true },
        ...(failure === "promotion" ? { dest_dlq_promoted: { error: "Stamp failed" } }
          : { quarantine_closure: { verdict: "diverging", open_count: 0 } }),
      });
      await answer(pending("job-a"), {
        quarantine: [finding], source: "dlq", quarantine_closure: { verdict: "diverging", open_count: 1 },
      });
      assert.deepEqual(completed, []);
      assert.match(document.querySelector(".dt-toast")?.textContent || "", /ledger requires review/i);
    });
  }

  it("refreshes a verified partial child without calling the parent fully recovered", async () => {
    const completed: string[] = [];
    await render("job-a", completed);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    await answer(pending("job-a", "POST"), {
      success: true, job_id: "partial-child", rows_written: 1, rejected: 1, reconciliation: { passed: true },
      quarantine_closure: { verdict: "in_progress", open_count: 1 },
    });
    await answer(pending("job-a"), {
      quarantine: [finding], source: "dlq", quarantine_closure: { verdict: "in_progress", open_count: 1 },
    });
    assert.deepEqual(completed, ["partial-child"]);
    assert.doesNotMatch(host!.textContent || "", /Ledger:\s*closed/);
    assert.equal(document.querySelector(".dt-toast")?.classList.contains("dt-toast--warning"), true);
  });

  it("issues one replay request for rapid repeated activation", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const button = replayButton()!;
    const { act } = await import("react");
    await act(async () => { button.click(); button.click(); });
    assert.equal(requests.filter((request) => request.method === "POST").length, 1);
  });

  it("closes the value editor with Escape and restores its opener", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const edit = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Edit")!;
    edit.focus();
    const { act } = await import("react");
    await act(async () => edit.click());
    await act(async () => document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true })));
    assert.equal(document.querySelector('[role="dialog"]') === null, true);
    assert.equal(document.activeElement === edit, true);
  });
  for (const failure of ["refresh_error", "diverging"] as const) {
    it(`does not navigate away after verified replay with ${failure}`, async () => {
      const completed: string[] = [];
      await render("job-a", completed);
      await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
      const { act } = await import("react");
      await act(async () => replayButton()!.click());
      await answer(pending("job-a", "POST"), {
        success: true, job_id: "verified-child", rows_written: 1, rejected: 0,
        reconciliation: { passed: true }, quarantine_closure: { verdict: "closed", open_count: 0 },
      });
      await answer(pending("job-a"), failure === "refresh_error"
        ? { detail: "Refresh unavailable" }
        : { quarantine: [finding], source: "dlq", quarantine_closure: { verdict: "diverging", open_count: 1 } },
      failure === "refresh_error" ? 500 : 200);
      assert.deepEqual(completed, []);
    });
  }

  it("does not allow edits while a refresh is pending", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    const refresh = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Refresh")!;
    await act(async () => refresh.click());
    const edit = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Edit")!;
    assert.equal(edit.disabled, true);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
  });

  it("keeps an open editor from being replaced by refresh", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    const edit = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Edit")!;
    await act(async () => edit.click());
    const refresh = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Refresh")!;
    assert.equal(refresh.disabled, true);
    assert.equal(replayButton()!.disabled, true);
  });
  for (const reconciliation of [{}, { passed: false, message: "Destination mismatch" }]) {
    it(`does not show recovery success when child proof is ${"passed" in reconciliation ? "failed" : "missing"}`, async () => {
      await render("job-a", []);
      await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
      const { act } = await import("react");
      await act(async () => replayButton()!.click());
      await answer(pending("job-a", "POST"), {
        success: true, job_id: "unverified-child", rows_written: 1, rejected: 0,
        reconciliation, quarantine_closure: { verdict: "diverging", open_count: 1 },
      });
      const toast = document.querySelector(".dt-toast");
      assert.ok(toast);
      assert.equal(toast.classList.contains("dt-toast--success"), false);
      assert.match(toast.textContent || "", /proof|Gate-8|verif/i);
    });
  }

  it("reports successful completion only for verified child proof", async () => {
    const completed: string[] = [];
    await render("job-a", completed);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    await answer(pending("job-a", "POST"), {
      success: true, job_id: "verified-child", rows_written: 1, rejected: 0,
      reconciliation: { passed: true }, quarantine_closure: { verdict: "closed", open_count: 0 },
    });
    await answer(pending("job-a"), {
      quarantine: [{ ...finding, retry_status: "promoted" }], source: "dlq",
      quarantine_closure: { verdict: "closed", open_count: 0, promoted_count: 1 },
    });
    assert.deepEqual(completed, ["verified-child"]);
    assert.match(host!.textContent || "", /Ledger:\s*closed/);
  });

  it("ignores a verified replay response after navigation to another job", async () => {
    const completed: string[] = [];
    await render("job-a", completed);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", rejected_rows: 1 });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    const oldReplay = pending("job-a", "POST");
    await render("job-b", completed);
    await answer(pending("job-b"), { quarantine: [], source: "none" });
    await answer(oldReplay, {
      success: true, job_id: "old-child", rows_written: 1, rejected: 0, reconciliation: { passed: true },
    });
    assert.deepEqual(completed, []);
    assert.doesNotMatch(host!.textContent || "", /old-child/);
  });
  it("offers replay for durable control-plane DLQ payloads", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", quarantine_durable: true, rejected_rows: 1 });
    assert.ok(replayButton());
    assert.equal(replayButton()!.disabled, false);
  });

  it("blocks partial replay until the server confirms the full durable sample", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), {
      quarantine: [finding], source: "dlq", quarantine_durable: true,
      replay_block_reason: "Quarantine sample is incomplete (1 of 2 rejects).",
    });
    assert.equal(replayButton(), undefined);
    assert.match(host!.textContent || "", /Quarantine sample is incomplete/);

    const { act } = await import("react");
    const refresh = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Refresh")!;
    await act(async () => refresh.click());
    await answer(pending("job-a"), {
      quarantine: [finding, { ...finding, row: 3 }], source: "dlq", quarantine_durable: true,
      replay_block_reason: null,
    });
    assert.ok(replayButton());
  });

  it("fails closed when the server omits replay eligibility", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", quarantine_durable: true }, 200, false);
    assert.equal(replayButton(), undefined);
    assert.match(host!.textContent || "", /server did not confirm that replay can reconstruct every quarantine row/i);

    const { act } = await import("react");
    const refresh = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent === "Refresh")!;
    await act(async () => refresh.click());
    await answer(pending("job-a"), {
      quarantine: [finding], source: "dlq", quarantine_durable: true, replay_block_reason: null,
    });
    assert.ok(replayButton());
  });

  it("does not verify child proof when replay success is not literal true", async () => {
    const completed: string[] = [];
    await render("job-a", completed);
    await answer(pending("job-a"), { quarantine: [finding], source: "dlq", quarantine_durable: true });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    await answer(pending("job-a", "POST"), {
      success: "false", job_id: "malformed-child", rows_written: 1, rejected: 0,
      reconciliation: { passed: true }, quarantine_closure: { verdict: "open", open_count: 1 },
    });
    await answer(pending("job-a"), {
      quarantine: [finding], source: "dlq", quarantine_durable: true,
      quarantine_closure: { verdict: "open", open_count: 1 },
    });
    assert.deepEqual(completed, []);
    assert.match(host!.textContent || "", /Child proof unverified/);
    assert.equal(document.querySelector(".dt-toast")?.classList.contains("dt-toast--success"), false);
  });

  it("does not turn a diverging authoritative ledger into closed", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), {
      quarantine: [{ ...finding, retry_status: "promoted" }], source: "write",
      quarantine_closure: { verdict: "diverging", open_count: 0, promoted_count: 1, next_action: "Destination promotion failed" },
    });
    assert.doesNotMatch(host!.textContent || "", /Ledger:\s*closed/);
    assert.match(host!.textContent || "", /Ledger:\s*diverging/);
  });

  it("does not call completion when child replay proof fails", async () => {
    const completed: string[] = [];
    await render("job-a", completed);
    await answer(pending("job-a"), { quarantine: [finding], source: "write", rejected_rows: 1 });
    const { act } = await import("react");
    await act(async () => replayButton()!.click());
    await answer(pending("job-a", "POST"), {
      success: false, job_id: "failed-child", rows_written: 0, rejected: 1,
      reconciliation: { passed: false, message: "Destination mismatch" },
    });
    assert.deepEqual(completed, []);
  });

  it("ignores findings that arrive after the selected job changes", async () => {
    await render("job-a", []);
    const old = pending("job-a");
    await render("job-b", []);
    await answer(pending("job-b"), { quarantine: [{ ...finding, reason: "Current job finding" }], source: "write" });
    await answer(old, { quarantine: [{ ...finding, reason: "Other job finding" }], source: "write" });
    assert.match(host!.textContent || "", /Current job finding/);
    assert.doesNotMatch(host!.textContent || "", /Other job finding/);
  });

  it("clears old findings immediately while the new job loads", async () => {
    await render("job-a", []);
    await answer(pending("job-a"), { quarantine: [{ ...finding, reason: "Old job finding" }], source: "write" });
    await render("job-b", []);
    assert.doesNotMatch(host!.textContent || "", /Old job finding/);
    assert.equal(replayButton(), undefined);
  });
});