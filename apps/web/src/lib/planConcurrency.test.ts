import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  PLAN_CHANGED_MESSAGE,
  PlanStoreBusyError,
  PlanStoreUnreadableError,
  PlanVersionConflictError,
  headVersion,
  planWriteResponse,
  retryAfterMs,
} from "./planConcurrency.js";

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", ...headers } });

function sender(responses: Response[]) {
  let calls = 0;
  return {
    send: async () => {
      const res = responses[Math.min(calls, responses.length - 1)];
      calls += 1;
      return res;
    },
    calls: () => calls,
  };
}

describe("headVersion", () => {
  it("is the newest revision version, 0 for none", () => {
    assert.equal(headVersion({ revisions: [{ version: 1 }, { version: 3 }, { version: 2 }] }), 3);
    assert.equal(headVersion({ revisions: [] }), 0);
    assert.equal(headVersion(undefined), 0);
    assert.equal(headVersion({}), 0);
  });
});

describe("retryAfterMs", () => {
  it("honours Retry-After seconds, bounded", () => {
    assert.equal(retryAfterMs(new Response(null, { status: 503, headers: { "Retry-After": "2" } })), 2000);
    assert.equal(retryAfterMs(new Response(null, { status: 503, headers: { "Retry-After": "600" } })), 10_000);
    assert.equal(retryAfterMs(new Response(null, { status: 503 })), 2000);
    assert.equal(retryAfterMs(new Response(null, { status: 503, headers: { "Retry-After": "soon" } })), 2000);
  });
});

describe("planWriteResponse", () => {
  it("passes a success through untouched", async () => {
    const s = sender([json(200, { plan: { id: "p" } })]);
    const res = await planWriteResponse(s.send);
    assert.equal(res.status, 200);
    assert.equal(s.calls(), 1);
  });

  it("turns a 409 plan_version_conflict into PlanVersionConflictError", async () => {
    const s = sender([
      json(409, { detail: { code: "plan_version_conflict", message: "A newer mapping revision exists", base_version: 2, current_version: 3, compared: "head" } }),
    ]);
    await assert.rejects(planWriteResponse(s.send), (err: unknown) => {
      assert.ok(err instanceof PlanVersionConflictError);
      assert.equal(err.baseVersion, 2);
      assert.equal(err.currentVersion, 3);
      assert.equal(err.message, PLAN_CHANGED_MESSAGE);
      return true;
    });
    assert.equal(s.calls(), 1, "a conflict is never retried");
  });

  it("leaves other 409s to the caller", async () => {
    const s = sender([json(409, { detail: { error: "duplicate_transfer" } })]);
    const res = await planWriteResponse(s.send);
    assert.equal(res.status, 409);
  });

  it("retries plan_store_busy once after Retry-After", async () => {
    const slept: number[] = [];
    const s = sender([
      json(503, { error: "plan_store_busy", detail: "busy" }, { "Retry-After": "2" }),
      json(200, { plan: { id: "p" } }),
    ]);
    const res = await planWriteResponse(s.send, { sleep: async (ms) => void slept.push(ms) });
    assert.equal(res.status, 200);
    assert.deepEqual(slept, [2000]);
    assert.equal(s.calls(), 2);
  });

  it("gives up after one retry when the store stays busy", async () => {
    const s = sender([json(503, { error: "plan_store_busy", detail: "The plan is being saved by another request." }, { "Retry-After": "1" })]);
    await assert.rejects(planWriteResponse(s.send, { sleep: async () => {} }), PlanStoreBusyError);
    assert.equal(s.calls(), 2);
  });

  it("reports plan_store_unreadable to the operator without retrying", async () => {
    const detail = "Transfer plans cannot be loaded ... restore the newest readable backup, transfer_plans.json.bak";
    const s = sender([json(503, { error: "plan_store_unreadable", detail })]);
    await assert.rejects(planWriteResponse(s.send, { sleep: async () => assert.fail("no retry") }), (err: unknown) => {
      assert.ok(err instanceof PlanStoreUnreadableError);
      assert.match(err.message, /operator/i);
      assert.ok(err.message.includes("transfer_plans.json.bak"));
      return true;
    });
    assert.equal(s.calls(), 1);
  });
});

describe("diffPlanMappings / sameMappings", () => {
  it("compares per destination column, including columns only one side maps", async () => {
    const { diffPlanMappings, sameMappings } = await import("./planConcurrency.js");
    const rows = diffPlanMappings(
      [{ source: "a", target: "x" }, { source: "b", target: "y" }],
      [{ source: "a", target: "x" }, { source: "c", target: "y" }, { source: "d", target: "z" }],
    );
    assert.deepEqual(rows, [
      { target: "x", latestSource: "a", mineSource: "a", differs: false },
      { target: "y", latestSource: "b", mineSource: "c", differs: true },
      { target: "z", latestSource: null, mineSource: "d", differs: true },
    ]);
    assert.equal(sameMappings([{ source: "a", target: "x" }], [{ source: "a", target: "x", confidence: 0.4 }]), true);
    assert.equal(sameMappings([{ source: "a", target: "x", transform: "upper" }], [{ source: "a", target: "x" }]), false);
  });
});

describe("plan_store_lock_unavailable", () => {
  it("is an operator error, never retried", async () => {
    const s = sender([json(503, { error: "plan_store_lock_unavailable", detail: "An operator must make transfer_plans.json.lock writable" })]);
    await assert.rejects(planWriteResponse(s.send, { sleep: async () => assert.fail("no retry") }), PlanStoreUnreadableError);
    assert.equal(s.calls(), 1);
  });
});
