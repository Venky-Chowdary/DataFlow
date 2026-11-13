import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { mapTransferPlan, syncTransferPlanMappings } from "./api.js";
import { PlanVersionConflictError } from "./planConcurrency.js";

/** The web half of PS-1: the UI save names the revision it was based on. */
function captureFetch(responses: Response[]) {
  const bodies: unknown[] = [];
  const originalFetch = globalThis.fetch;
  const g = globalThis as { window?: unknown };
  const hadWindow = "window" in g;
  if (!hadWindow) g.window = globalThis;
  let i = 0;
  globalThis.fetch = (async (_url: RequestInfo | URL, init?: RequestInit) => {
    bodies.push(init?.body ? JSON.parse(String(init.body)) : null);
    return responses[Math.min(i++, responses.length - 1)];
  }) as typeof fetch;
  return {
    bodies,
    restore: () => {
      globalThis.fetch = originalFetch;
      if (!hadWindow) delete g.window;
    },
  };
}

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const MAPPINGS = [{ source: "id", target: "id", confidence: 1 }];

describe("syncTransferPlanMappings", () => {
  it("sends base_version when the loaded plan's head is known", async () => {
    const f = captureFetch([json(200, { plan: { id: "p", revisions: [{ version: 4 }] } })]);
    try {
      await syncTransferPlanMappings("p", MAPPINGS, { baseVersion: 3 });
    } finally {
      f.restore();
    }
    assert.deepEqual(f.bodies[0], { mappings: MAPPINGS, base_version: 3 });
  });

  it("sends no base_version when the head is unknown", async () => {
    const f = captureFetch([json(200, { plan: { id: "p", revisions: [] } })]);
    try {
      await syncTransferPlanMappings("p", MAPPINGS);
    } finally {
      f.restore();
    }
    assert.deepEqual(f.bodies[0], { mappings: MAPPINGS });
  });

  it("raises PlanVersionConflictError on a stale save", async () => {
    const f = captureFetch([
      json(409, { detail: { code: "plan_version_conflict", message: "x", base_version: 3, current_version: 5 } }),
    ]);
    try {
      await assert.rejects(syncTransferPlanMappings("p", MAPPINGS, { baseVersion: 3 }), PlanVersionConflictError);
    } finally {
      f.restore();
    }
  });
});

describe("mapTransferPlan", () => {
  it("raises PlanVersionConflictError when the plan changed while Map ran", async () => {
    const f = captureFetch([
      json(409, { detail: { code: "plan_version_conflict", message: "x", base_version: 1, current_version: 2, compared: "selection" } }),
    ]);
    try {
      await assert.rejects(mapTransferPlan("p", { use_llm: false }), (err: unknown) => {
        assert.ok(err instanceof PlanVersionConflictError);
        assert.equal(err.compared, "selection");
        return true;
      });
    } finally {
      f.restore();
    }
  });
});
