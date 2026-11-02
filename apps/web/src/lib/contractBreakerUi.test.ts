/**
 * Run: npx --yes tsx --test apps/web/src/lib/contractBreakerUi.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  breakerBadgeClass,
  breakerBlocksRuns,
  breakerLabel,
  breakerWarnLabel,
  campaignLabel,
  campaignWarnLabel,
  contractBreakerBlocksRun,
  contractIdFromBreakerFailure,
  isCircuitBreakerFailureText,
} from "./contractBreakerUi.js";

describe("contractBreakerUi", () => {
  it("labels and classes by state", () => {
    assert.equal(breakerLabel("closed"), "Breaker closed");
    assert.equal(breakerLabel("half_open"), "Breaker half-open");
    assert.equal(breakerBadgeClass("closed"), "df2-badge-live");
    assert.equal(breakerBadgeClass("open"), "df2-badge-warn");
  });

  it("blocks runs when open or half-open", () => {
    assert.equal(breakerBlocksRuns("closed"), false);
    assert.equal(breakerBlocksRuns("open"), true);
    assert.equal(breakerBlocksRuns("half_open"), true);
  });

  it("warn label only for blocking states", () => {
    assert.equal(breakerWarnLabel("closed"), "");
    assert.equal(breakerWarnLabel("open"), "Breaker open");
  });

  it("Dual Run list signal only warns when diverging", () => {
    assert.equal(campaignWarnLabel("cutover_ready"), "");
    assert.equal(campaignWarnLabel("in_progress"), "");
    assert.equal(campaignWarnLabel("diverging"), "Parallel run diverging");
    assert.equal(campaignLabel("cutover_ready"), "Parallel run ready");
  });

  it("blocks Studio Execute with a reset CTA reason", () => {
    assert.equal(contractBreakerBlocksRun("closed"), "");
    assert.match(contractBreakerBlocksRun("open"), /Reset it after you fix/);
    assert.equal(isCircuitBreakerFailureText("Circuit breaker for contract dfc-1 is OPEN"), true);
    assert.equal(
      contractIdFromBreakerFailure({
        error: "Circuit breaker for contract dfc-1 is OPEN; transfer blocked until recovery",
      }),
      "dfc-1",
    );
    assert.equal(
      contractIdFromBreakerFailure({
        errorDetails: {
          violations: [{ rule: "circuit_breaker_open", contract_id: "dfc-2" }],
        },
      }),
      "dfc-2",
    );
  });
});
