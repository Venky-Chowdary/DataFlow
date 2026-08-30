/**
 * The Validate PII approval must actually reach the API, and the operator must
 * never be told the reason they cannot execute is "proof 0".
 * Run: npx --yes tsx --test apps/web/src/lib/piiAcknowledgment.test.ts
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { standingAcknowledgmentReason } from "./acknowledgments.ts";
import { blockerTitle, isInternalGateId } from "./preflightGates.ts";
import { buildDisplayBlockers, buildExecutiveSummary } from "./validateIssueGrouping.ts";
import type { PreflightResult } from "./types.ts";

function preflightWith(blockers: PreflightResult["blockers"]): PreflightResult {
  return {
    passed: false,
    passed_count: 16,
    total_gates: 16,
    readiness_score: 100,
    gates: [{ id: "g1_source", status: "pass", message: "ok", details: {} }],
    blockers,
  } as unknown as PreflightResult;
}

describe("PII acknowledgment transport and naming", () => {
  it("restates a standing reason so a sticky acknowledgment is not refused", () => {
    assert.equal(standingAcknowledgmentReason({}), "");
    assert.ok(standingAcknowledgmentReason({ compliance: true }).length >= 8);
    assert.match(
      standingAcknowledgmentReason({ compliance: true, fkRisk: true }),
      /PII\/compliance.*FK risk/,
    );
  });

  it("names a proof blocker by its message, never by its internal id", () => {
    assert.equal(
      blockerTitle("proof_0", "PII/compliance review required"),
      "PII/compliance review required",
    );
    assert.equal(blockerTitle("proof_3", ""), "Transfer proof blocker");
    assert.equal(blockerTitle("g9_data_integrity", "anything"), blockerTitle("g9_data_integrity"));
    const long = blockerTitle(
      "proof_1",
      "Append delta unverified because the destination held an unknown number of rows before this write",
    );
    assert.ok(long.length <= 73, long);
  });

  it("marks positional proof ids internal so no surface prints them", () => {
    assert.ok(isInternalGateId("proof_0"));
    assert.ok(isInternalGateId("PROOF_12"));
    assert.ok(!isInternalGateId("g6_target_ddl"));
    assert.ok(!isInternalGateId(""));
  });

  it("keeps 'proof 0' out of the operator's cannot-execute lines", () => {
    const pf = preflightWith([
      {
        id: "proof_0",
        message: "PII/compliance review required",
        details: { compliance_ack_required: true },
      },
    ] as unknown as PreflightResult["blockers"]);
    const items = buildDisplayBlockers(pf);
    assert.equal(items[0].title, "PII/compliance review required");
    const summary = buildExecutiveSummary(pf);
    assert.ok(!JSON.stringify(summary).toLowerCase().includes("proof 0"));
    assert.ok(!JSON.stringify(items).toLowerCase().includes("proof 0"));
  });

  it("does not claim PII approval unlocks Execute when another cause blocks", () => {
    const pf = preflightWith([
      {
        id: "g2_destination",
        message: "Destination unreachable",
        details: {},
      },
      {
        id: "proof_0",
        message: "PII/compliance review required",
        details: { compliance_ack_required: false },
      },
    ] as unknown as PreflightResult["blockers"]);
    const summary = buildExecutiveSummary(pf);
    assert.notEqual(summary.title, "Approve PII to unlock Execute");
    assert.ok(summary.untilLines.some((l) => /destination/i.test(l)));
  });

  it("renders no blocker heading straight from its id", () => {
    // A positional id reaching a heading is how `proof_0` shipped as a reason;
    // every blocker surface must resolve its own text through blockerTitle().
    const surfaces = [
      "../pages/JobsPage.tsx",
      "../components/PreflightTimeline.tsx",
      "../components/transfer/ValidateDashboard.tsx",
    ];
    for (const rel of surfaces) {
      const file = path.resolve(path.dirname(fileURLToPath(import.meta.url)), rel);
      const src = readFileSync(file, "utf8");
      assert.doesNotMatch(
        src,
        /<strong>\{(?:b|blocker|issue)\.(?:id|gate)\}<\/strong>/,
        `${rel} prints a raw blocker id as a heading`,
      );
    }
  });
});

describe("preflightTransferPlan", () => {
  it("posts the acknowledgment fields as a JSON body", async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const originalFetch = globalThis.fetch;
    const hadWindow = "window" in globalThis;
    if (!hadWindow) {
      (globalThis as { window?: unknown }).window = {
        setTimeout: (fn: () => void) => {
          fn();
          return 0;
        },
        clearTimeout: () => {},
        localStorage: { getItem: () => null, setItem: () => {}, removeItem: () => {} },
        location: { origin: "http://localhost" },
        addEventListener: () => {},
        dispatchEvent: () => true,
      };
    }
    globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
      calls.push({ url: String(url), init });
      const method = String(init.method || "GET").toUpperCase();
      if (method === "POST") {
        return {
          ok: true,
          status: 202,
          json: async () => ({ plan_id: "plan-1", run_id: "pf_job1", status: "running" }),
        } as unknown as Response;
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ passed: true, status: "complete", run_id: "pf_ssot" }),
      } as unknown as Response;
    }) as unknown as typeof fetch;
    try {
      const { preflightTransferPlan } = await import("./api.ts");
      const result = await preflightTransferPlan("plan-1", {
        compliance_acknowledged: true,
        acknowledgment_actor: "operator@dataflow.test",
        acknowledgment_reason: "Governance approved for this window",
      });
      assert.equal((result as { passed?: boolean }).passed, true);
    } finally {
      globalThis.fetch = originalFetch;
      if (!hadWindow) delete (globalThis as { window?: unknown }).window;
    }
    assert.equal(calls.length, 2);
    assert.match(calls[0].url, /\/transfer\/plans\/plan-1\/preflight$/);
    assert.equal(String(calls[0].init.method || "POST").toUpperCase(), "POST");
    const body = JSON.parse(String(calls[0].init.body));
    assert.equal(body.compliance_acknowledged, true);
    assert.equal(body.acknowledgment_actor, "operator@dataflow.test");
    assert.equal(body.acknowledgment_reason, "Governance approved for this window");
    assert.equal(body.schema_drift_acknowledged, false);
    assert.equal(body.async_run, true);
    assert.match(calls[1].url, /\/transfer\/plans\/plan-1\/preflight\?run_id=pf_job1/);
  });
});

describe("validate transport honesty", () => {
  it("names a 504 HTML fallback as a control-plane timeout, not a recipe refusal", async () => {
    const { isTransportFailure, validateTransportMessage } = await import("./api.ts");
    assert.equal(isTransportFailure("Plan preflight failed", 504), true);
    assert.equal(isTransportFailure("upstream timed out", 200), true);
    assert.equal(isTransportFailure("Target DDL cannot accept NUMBER(9,6)", 400), false);
    const msg = validateTransportMessage("upstream timed out", 1_000_000);
    assert.match(msg, /1,000,000 row/);
    assert.match(msg, /Re-run Validate/);
    assert.match(msg, /Execute stays locked/);
    assert.equal(
      validateTransportMessage("upstream timed out", null).includes("while checking"),
      false,
    );
    assert.equal(
      validateTransportMessage("upstream timed out", 0).includes("while checking"),
      false,
    );
    assert.match(
      validateTransportMessage("Target DDL cannot accept NUMBER(9,6)", null),
      /Target DDL/,
    );
  });
});
