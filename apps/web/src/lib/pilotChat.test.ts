import assert from "node:assert/strict";
import test from "node:test";

import { pilotActionChipLabel, runPilotConfirm, showsFollowUpPrompts, uniquePilotPrompts } from "./pilotChat";
import type { CopilotPendingAction } from "./api";

type FetchCall = { url: string; body: Record<string, unknown> };

async function withFetch(
  response: () => Response,
  run: (calls: FetchCall[]) => Promise<void>,
): Promise<void> {
  const originalFetch = globalThis.fetch;
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const calls: FetchCall[] = [];
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: {
      setTimeout: globalThis.setTimeout,
      clearTimeout: globalThis.clearTimeout,
    } as unknown as Window,
  });
  globalThis.fetch = (async (input, init) => {
    calls.push({
      url: String(input),
      body: JSON.parse(String(init?.body || "{}")) as Record<string, unknown>,
    });
    return response();
  }) as typeof fetch;
  try {
    await run(calls);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalWindow) {
      Object.defineProperty(globalThis, "window", originalWindow);
    } else {
      Reflect.deleteProperty(globalThis, "window");
    }
  }
}

function studioAction(overrides: Partial<CopilotPendingAction> = {}): CopilotPendingAction {
  return {
    id: "studio:open_bad_data_fix",
    type: "studio",
    kind: "quarantine_and_rerun",
    label: "Client label",
    run_id: "pf_client",
    risk: "mutate",
    destructive: false,
    payload: {
      ack_id: "ack_studio",
      kind: "quarantine_and_rerun",
      run_id: "pf_client",
    },
    ...overrides,
  };
}

test("repeated pilot prompts collapse to one chip", () => {
  assert.deepEqual(
    uniquePilotPrompts(["  Give me a workspace briefing", "Give me a workspace briefing", "", "Show my recent jobs"]),
    ["Give me a workspace briefing", "Show my recent jobs"],
  );
  assert.deepEqual(uniquePilotPrompts(undefined), []);
});

test("only the newest turn offers follow-up prompts", () => {
  const suggested = ["show my jobs"];
  // Last of three.
  assert.equal(showsFollowUpPrompts(2, 3, suggested), true);
  // Earlier turns keep their answer but drop the chips, so a thread does not
  // grow a column of stale suggestions that re-ask settled questions.
  assert.equal(showsFollowUpPrompts(0, 3, suggested), false);
  assert.equal(showsFollowUpPrompts(1, 3, suggested), false);
  // A single turn is also the newest one.
  assert.equal(showsFollowUpPrompts(0, 1, suggested), true);
});

test("no prompts means no chip row", () => {
  assert.equal(showsFollowUpPrompts(0, 1, undefined), false);
  assert.equal(showsFollowUpPrompts(0, 1, []), false);
});

test("an action chip falls back to the screen name it opens", () => {
  assert.equal(pilotActionChipLabel({ label: "Open Jobs" }), "Open Jobs");
  assert.equal(pilotActionChipLabel({ screen: "jobs" }), "Open Jobs");
  assert.equal(pilotActionChipLabel({}), "Action");
});

test("Studio confirm authorizes with the ack and dispatches server values", async () => {
  await withFetch(
    () => new Response(
      JSON.stringify({
        ok: true,
        idempotent: false,
        kind: "studio_remediation",
        remediation_kind: "open_bad_data_fix",
        run_id: "pf_server",
        label: "Server label",
      }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    ),
    async (calls) => {
      const dispatched: Array<{ kind: string; label?: string; run_id?: string }> = [];
      const navigated: string[] = [];
      const toasts: Array<{ title: string; message: string; tone?: string }> = [];
      const handlers: Parameters<typeof runPilotConfirm>[1] = {
        confirm: async () => true,
        dispatchStudioAction: (action) => dispatched.push(action),
        onNavigate: (screen) => navigated.push(screen),
        toast: (toast) => toasts.push(toast),
      };

      const result = await runPilotConfirm(studioAction(), handlers);

      assert.equal(result, "cleared");
      assert.equal(calls.length, 1);
      assert.equal(calls[0].url, "/api/v1/copilot/confirm");
      assert.equal(calls[0].body.ack_id, "ack_studio");
      assert.deepEqual(dispatched, [{
        kind: "open_bad_data_fix",
        run_id: "pf_server",
        label: "Server label",
      }]);
      assert.deepEqual(navigated, ["transfer"]);
      assert.equal(toasts[0].message, "Server label");
    },
  );
});

test("Studio refusal propagates without dispatch or navigation", async () => {
  await withFetch(
    () => new Response(
      JSON.stringify({ detail: "Not allowed for this workspace" }),
      { status: 403, headers: { "Content-Type": "application/json" } },
    ),
    async (calls) => {
      const dispatched: unknown[] = [];
      const navigated: string[] = [];
      const handlers: Parameters<typeof runPilotConfirm>[1] = {
        confirm: async () => true,
        dispatchStudioAction: (action) => dispatched.push(action),
        onNavigate: (screen) => navigated.push(screen),
        toast: () => {},
      };

      await assert.rejects(runPilotConfirm(studioAction(), handlers), /not allowed/i);

      assert.equal(calls.length, 1);
      assert.deepEqual(dispatched, []);
      assert.deepEqual(navigated, []);
    },
  );
});

test("destructive Studio confirmation cancel does not call the backend", async () => {
  await withFetch(
    () => {
      throw new Error("backend must not be called");
    },
    async (calls) => {
      const dispatched: unknown[] = [];
      const navigated: string[] = [];
      const prompts: Array<{ tone?: string; confirmLabel?: string }> = [];
      const handlers: Parameters<typeof runPilotConfirm>[1] = {
        confirm: async (prompt) => {
          prompts.push(prompt);
          return false;
        },
        dispatchStudioAction: (action) => dispatched.push(action),
        onNavigate: (screen) => navigated.push(screen),
        toast: () => {},
      };

      const result = await runPilotConfirm(
        studioAction({ destructive: true }),
        handlers,
      );

      assert.equal(result, "cancelled");
      assert.equal(prompts[0].tone, "danger");
      assert.equal(calls.length, 0);
      assert.deepEqual(dispatched, []);
      assert.deepEqual(navigated, []);
    },
  );
});

test("unknown action kinds are not dispatched to Studio", async () => {
  const dispatched: unknown[] = [];
  const toasts: Array<{ title: string; message: string; tone?: string }> = [];
  const handlers: Parameters<typeof runPilotConfirm>[1] = {
    confirm: async () => true,
    dispatchStudioAction: (action) => dispatched.push(action),
    toast: (toast) => toasts.push(toast),
  };

  const result = await runPilotConfirm(
    { id: "future:action", type: "future_action", kind: "open_bad_data_fix" },
    handlers,
  );

  assert.equal(result, "unknown");
  assert.deepEqual(dispatched, []);
  assert.equal(toasts[0].title, "Unknown action");
});

test("partial CDC drift repair warns with the remaining-key count", async () => {
  await withFetch(
    () =>
      new Response(
        JSON.stringify({
          kind: "lifecycle",
          op: "repair_cdc_drift",
          job_id: "job-drift",
          screen: "jobs",
          idempotent: true,
          resolved: false,
          remaining_keys: 2,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    async () => {
      const toasts: Array<{ title: string; message: string; tone?: string }> = [];
      await runPilotConfirm(
        {
          id: "repair:job-drift",
          type: "repair_cdc_drift",
          destructive: true,
          payload: { ack_id: "ack-drift", kind: "repair_cdc_drift" },
        },
        {
          confirm: async () => true,
          toast: (toast) => toasts.push(toast),
        },
      );

      assert.equal(toasts[0].tone, "warning");
      assert.equal(
        toasts[0].title,
        "CDC drift repair partially complete (already applied)",
      );
      assert.match(toasts[0].message, /2 key\(s\) remain unresolved/);
      assert.match(toasts[0].message, /state convergence \/ reconciliation, not log replay/i);
    },
  );
});

test("resolved CDC drift repair keeps the complete title", async () => {
  await withFetch(
    () =>
      new Response(
        JSON.stringify({
          kind: "lifecycle",
          op: "repair_cdc_drift",
          job_id: "job-drift",
          screen: "jobs",
          resolved: true,
          remaining_keys: 0,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    async () => {
      const toasts: Array<{ title: string; message: string; tone?: string }> = [];
      await runPilotConfirm(
        {
          id: "repair:job-drift",
          type: "repair_cdc_drift",
          destructive: true,
          payload: { ack_id: "ack-drift", kind: "repair_cdc_drift" },
        },
        {
          confirm: async () => true,
          toast: (toast) => toasts.push(toast),
        },
      );

      assert.equal(toasts[0].tone, "success");
      assert.equal(toasts[0].title, "CDC drift repair complete");
    },
  );
});
