/**
 * UX-6: Validate's counts come from one list, with human gate names.
 *
 * The audit's screen (PG -> MySQL into a missing table, PII columns) showed
 * "1 blocked gate", "3 blocking gates" and "Blocked by 3 gates" at once, named
 * the create-table gate "g create dest ddl" / "G CREATE", listed the DDL hold
 * twice (once per gate id carrying the same message), and gave two gates the
 * code G12.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { PreflightResult } from "./types";
import { GATE_CATALOG, gateDisplayCode, gateLabel } from "./preflightGates";
import { buildBlockingGateSummaries, buildExecutiveSummary, validateBlockedCount } from "./validateIssueGrouping";

const DDL =
  "'ux_customers' does not exist. Schema policy is manual approval, so this run would need to run: "
  + "CREATE TABLE ux_customers (id BIGINT, country_code CHAR(2)) — approve destination DDL before executing.";
const PII =
  "PII/compliance review required — full_name (name by column name); email (email by column name) (risk 0.16). "
  + "Approve the PII review on Validate and record who approved it and why. "
  + "Agents send pii_acknowledgement {approved_by, reason}.";

function auditPreflight(): PreflightResult {
  const pass = (id: string) => ({ id, status: "pass" as const, message: "ok", duration_ms: 1 });
  return {
    passed: false,
    passed_count: 16,
    total_gates: 23,
    readiness_score: 0.94,
    gates: [
      ...["g1_source", "g2_destination", "g3_schema_contract", "g4_mapping_confidence", "g5_dry_run"].map(pass),
      { id: "g_create_dest_ddl", status: "block", message: DDL, duration_ms: 3 },
    ],
    blockers: [
      { id: "g_create_dest_ddl", message: DDL },
      { id: "dest_ddl", message: DDL, guidance: { gate: "g6_target_ddl" } },
      { id: "proof_0", message: PII, details: { compliance_ack_required: true } },
    ],
  } as unknown as PreflightResult;
}

describe("UX-6: one list behind every Validate count", () => {
  it("lists the DDL hold once and the PII hold once", () => {
    const list = buildBlockingGateSummaries(auditPreflight());
    assert.equal(list.length, 2, list.map((g) => g.line).join("\n"));
    assert.equal(list.filter((g) => g.line.includes("CREATE TABLE")).length, 1);
  });

  it("every count on the screen is that list's length", () => {
    const pf = auditPreflight();
    const n = buildBlockingGateSummaries(pf).length;
    const summary = buildExecutiveSummary(pf)!;
    assert.equal(summary.blockingGateCount, n);
    assert.equal(validateBlockedCount(pf), n);
    assert.match(summary.subtitle, new RegExp(`\\b${n} blocking gates\\b`));
    assert.equal(summary.railLine, `Blocked by ${n} gates`);
  });

  it("names gates in words: no raw ids, no '(API: …)' text", () => {
    const list = buildBlockingGateSummaries(auditPreflight());
    for (const g of list) {
      assert.doesNotMatch(g.line, /\bg_create|g create dest ddl|\bG CREATE\b|\(API:|pii_acknowledgement/i, g.line);
    }
    assert.equal(gateLabel("g_create_dest_ddl"), "Create destination table");
    assert.doesNotMatch(gateDisplayCode("g_create_dest_ddl"), /CREATE/);
    // An unknown id still reads as words, not as a code.
    assert.equal(gateLabel("g_some_new_gate"), "Some new gate");
  });

  it("no two gates share a display code", () => {
    const ids = [...new Set([...GATE_CATALOG.map((g) => g.id), "g12_staging_policy", "g_create_dest_ddl", "g_upsert_dest_key"])];
    const seen = new Map<string, string>();
    for (const id of ids) {
      const code = gateDisplayCode(id);
      assert.ok(!seen.has(code), `${id} and ${seen.get(code)} both show ${code}`);
      seen.set(code, id);
    }
  });
});
