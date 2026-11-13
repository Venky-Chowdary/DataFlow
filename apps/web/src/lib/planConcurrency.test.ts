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
    assert.deepEqual(rows.map((r) => [r.target, r.latestSource, r.mineSource, r.differs]), [
      ["x", "a", "a", false],
      ["y", "b", "c", true],
      ["z", null, "d", true],
    ]);
    assert.equal(sameMappings([{ source: "a", target: "x" }], [{ source: "a", target: "x", confidence: 0.4 }]), true);
    assert.equal(sameMappings([{ source: "a", target: "x", transform: "upper" }], [{ source: "a", target: "x" }]), false);
  });

  it("keeps every source of a duplicated target, case-insensitively, instead of the last one", async () => {
    const { diffPlanMappings } = await import("./planConcurrency.js");
    const [row] = diffPlanMappings(
      [{ source: "email", target: "Contact" }, { source: "alt_email", target: "contact" }],
      [{ source: "email", target: "contact" }],
    );
    assert.equal(row.duplicate, true);
    assert.deepEqual(row.latestSources, ["alt_email", "email"]);
    assert.equal(row.latestSource, "alt_email + email");
    assert.equal(row.differs, true);
  });

  it("shows an omitted column as not written, keyed by its source", async () => {
    const { diffPlanMappings } = await import("./planConcurrency.js");
    const rows = diffPlanMappings(
      [{ source: "ssn", target: "ssn" }],
      [{ source: "ssn", target: "", transform: "omit", intentional_omit: true }],
    );
    assert.deepEqual(rows.map((r) => [r.target, r.latestSource, r.mineSource]), [
      ["ssn", "ssn", null],
      ["ssn (not written)", null, "omitted"],
    ]);
  });
});

describe("sameFullMappings", () => {
  it("treats override, omit, reasons, transform and confidence as part of the decision", async () => {
    const { sameFullMappings } = await import("./planConcurrency.js");
    const base = { source: "a", target: "x", confidence: 0.9, user_override: true };
    assert.equal(sameFullMappings([base], [{ ...base }]), true);
    assert.equal(sameFullMappings([base], [{ ...base, user_override: false }]), false, "the review's probe");
    assert.equal(sameFullMappings([base], [{ ...base, confidence: 0.93 }]), false);
    assert.equal(sameFullMappings([base], [{ ...base, transform: "trim" }]), false);
    const omit = { source: "ssn", target: "", transform: "omit", intentional_omit: true, omit_reason: "pii" };
    assert.equal(sameFullMappings([omit], [{ ...omit, omit_reason: "legal" }]), false);
    assert.equal(sameFullMappings([base, omit], [omit, base]), true, "order does not matter");
  });

  it("compares every field a save carries, not only target/source/transform", async () => {
    const { sameFullMappings } = await import("./planConcurrency.js");
    const row = { source: "amount", target: "amount", confidence: 0.9, transform: "", user_override: true };
    // The review's probe: another tab saved the same column as BIGINT.
    assert.equal(
      sameFullMappings([{ ...row, target_type: "BIGINT" }], [{ ...row, target_type: "INTEGER" }]),
      false,
      "target_type",
    );
    const mine = { ...row, code_crosswalk: { A: "1" }, rule_contracts: [{ kind: "not_null" }], risk_contract: { k: 1 } };
    assert.equal(sameFullMappings([row], [mine]), false, "crosswalk / rules / risk contract added");
    assert.equal(sameFullMappings([{ ...mine, code_crosswalk: { A: "2" } }], [mine]), false, "crosswalk value");
    assert.equal(sameFullMappings([{ ...mine, rule_contracts: [{ kind: "unique" }] }], [mine]), false, "rules");
    assert.equal(sameFullMappings([{ ...row, code_crosswalk_system: "ICD10" }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, risk_acknowledged: true }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, false_friend_confirmed: true }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, business_rule: { kind: "x", text: "t" } }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, create_new: true }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, assignment_strategy: "create_compatible_new" }], [row]), false);
    assert.equal(sameFullMappings([{ ...row, control_total: true }], [row]), false);
    // control_total is tri-state in the editor: an explicit "no" is a decision.
    assert.equal(sameFullMappings([{ ...row, control_total: false }], [row]), false);
    const omit = { source: "ssn", target: "", transform: "omit", intentional_omit: true, omit_reason: "pii" };
    assert.equal(sameFullMappings([{ ...omit, archive_reference: "box 4" }], [omit]), false);
    assert.equal(sameFullMappings([{ ...omit, retention_until: "2030-01-01" }], [omit]), false);
    assert.equal(sameFullMappings([{ ...omit, omit_approved_by: "dpo" }], [omit]), false);
  });

  it("ignores key order, absent-vs-empty values and the free-text reason", async () => {
    const { sameFullMappings } = await import("./planConcurrency.js");
    const row = { source: "a", target: "x", confidence: 0.9, user_override: true, code_crosswalk: { A: "1", B: "2" } };
    const reordered = { code_crosswalk: { B: "2", A: "1" }, user_override: true, confidence: 0.9, target: "x", source: "a" };
    assert.equal(sameFullMappings([row], [reordered]), true, "key order, nested too");
    assert.equal(
      sameFullMappings([row], [{ ...row, transform: "", requires_review: false, type_narrowing: undefined, retention_until: null }]),
      true,
      "undefined / null / false / empty mean unset",
    );
    assert.equal(sameFullMappings([row], [{ ...row, reason: "User reviewed" }]), true, "reason is an explanation");
    assert.equal(sameFullMappings([row], [{ ...row, confidence: 0.9000001 }]), true, "confidence is rounded");
    // What the API stores is the JSON of what was sent (undefined keys vanish).
    const sent = { ...row, risk_acknowledged: undefined, omit_reason: undefined, target_type: "INTEGER" };
    assert.equal(sameFullMappings(JSON.parse(JSON.stringify([sent])), [sent]), true, "JSON round-trip");
  });

  it("treats a reorder of duplicate rows as the same (no tie order)", async () => {
    const { sameFullMappings } = await import("./planConcurrency.js");
    const a = { source: "a", target: "x", transform: "", confidence: 0.5 };
    const b = { source: "a", target: "x", transform: "", confidence: 0.9 };
    assert.equal(sameFullMappings([a, b], [b, a]), true);
    assert.equal(sameFullMappings([a, b], [a, a]), false);
  });
});

describe("planConflictRows (the dialog's table)", () => {
  it("lists every target of the active, newest and unsaved mappings", async () => {
    const { planConflictRows } = await import("./planConcurrency.js");
    const active = [{ source: "a", target: "x" }];
    const newest = [{ source: "a", target: "x" }, { source: "n", target: "only_newest" }];
    const mine = [{ source: "b", target: "x" }, { source: "m", target: "only_mine" }];
    const rows = planConflictRows(active, mine, newest);
    const by = new Map(rows.map((r) => [r.target, r]));
    assert.deepEqual([...by.keys()].sort(), ["only_mine", "only_newest", "x"]);
    assert.equal(by.get("only_newest")!.newestSource, "n", "a target only the newest revision maps is shown");
    assert.equal(by.get("only_newest")!.latestSource, null);
    assert.equal(by.get("only_newest")!.mineSource, null);
    assert.equal(by.get("x")!.newestSource, "a");
    assert.equal(by.get("x")!.differs, true);
    assert.equal(by.get("only_mine")!.newestSource, null);
  });

  it("is the plain active-vs-mine diff when the newest revision is the active one", async () => {
    const { planConflictRows, diffPlanMappings } = await import("./planConcurrency.js");
    const active = [{ source: "a", target: "X" }];
    const mine = [{ source: "b", target: "x" }];
    assert.deepEqual(
      planConflictRows(active, mine, null).map(({ newestSource, ...row }) => [row, newestSource]),
      diffPlanMappings(active, mine).map((row) => [row, null]),
    );
  });
});

describe("headAfterOwnMap", () => {
  it("adopts the head only when it is the revision this page's Map request created", async () => {
    const { headAfterOwnMap } = await import("./planConcurrency.js");
    const ours = { version: 3, plan_summary: { client_request_id: "map-ours" } };
    const theirs = { version: 3, plan_summary: { client_request_id: "map-theirs" } };
    const anonymous = { version: 3, validation: { passed: true } as { source?: string } };
    assert.equal(headAfterOwnMap({ id: "p", revisions: [{ version: 2 }, ours] }, 2, "map-ours"), 3);
    // Another user's Map (the review's probe: v2 ui_sync, v3 pipeline) is never adopted.
    assert.equal(headAfterOwnMap({ id: "p", revisions: [{ version: 2 }, theirs] }, 2, "map-ours"), null);
    assert.equal(headAfterOwnMap({ id: "p", revisions: [{ version: 2 }, anonymous] }, 2, "map-ours"), null);
    // Ours landed, but someone saved after it: the head is not ours.
    assert.equal(headAfterOwnMap({ id: "p", revisions: [{ version: 2 }, ours, { version: 4 }] }, 2, "map-ours"), null);
    assert.equal(headAfterOwnMap({ id: "p", revisions: [ours] }, null, "map-ours"), null);
    assert.equal(headAfterOwnMap({ id: "p", revisions: [{ version: 2 }, ours] }, 2, ""), null);
  });
});

describe("isClientSideFailure", () => {
  it("is true only when no server answer arrived", async () => {
    const { isClientSideFailure } = await import("./planConcurrency.js");
    assert.equal(isClientSideFailure(new Error("Request timed out")), true);
    assert.equal(isClientSideFailure(new DOMException("Aborted", "AbortError")), true);
    assert.equal(isClientSideFailure(new TypeError("Failed to fetch")), true);
    assert.equal(isClientSideFailure(new Error("Plan mapping failed: 500")), false);
  });
});

describe("newMapRequestId", () => {
  it("is unique and matches the API's id pattern", async () => {
    const { newMapRequestId } = await import("./planConcurrency.js");
    const a = newMapRequestId();
    assert.match(a, /^[A-Za-z0-9._:-]{1,64}$/);
    assert.notEqual(a, newMapRequestId());
  });
});

describe("headIfOurs", () => {
  it("adopts the head only when the active revision holds exactly this page's mappings", async () => {
    const { headIfOurs } = await import("./planConcurrency.js");
    const mine = [{ source: "a", target: "x" }];
    const plan = (mappings: Array<{ source: string; target: string }>) => ({
      id: "p", active_version: 4, revisions: [{ version: 3, mappings: [] }, { version: 4, mappings }],
    });
    assert.equal(headIfOurs(plan(mine), mine), 4);
    assert.equal(headIfOurs(plan([{ source: "b", target: "x" }]), mine), null);
    assert.equal(headIfOurs({ id: "p", revisions: [] }, mine), null);
  });

  it("does not adopt a revision that differs only in a column's type", async () => {
    const { headIfOurs } = await import("./planConcurrency.js");
    const mine = [{ source: "amount", target: "amount", target_type: "INTEGER" }];
    const theirs = [{ source: "amount", target: "amount", target_type: "BIGINT" }];
    const plan = { id: "p", active_version: 6, revisions: [{ version: 5, mappings: mine }, { version: 6, mappings: theirs }] };
    assert.equal(headIfOurs(plan, mine), null);
  });
});

describe("plan_store_lock_unavailable", () => {
  it("is an operator error, never retried", async () => {
    const s = sender([json(503, { error: "plan_store_lock_unavailable", detail: "An operator must make transfer_plans.json.lock writable" })]);
    await assert.rejects(planWriteResponse(s.send, { sleep: async () => assert.fail("no retry") }), PlanStoreUnreadableError);
    assert.equal(s.calls(), 1);
  });
});
