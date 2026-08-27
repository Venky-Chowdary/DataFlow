import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  coerceLastTestOk,
  connectorLooksHealthy,
  connectorPassedProbe,
  connectorNeedsAttention,
  connectorTestHealth,
  connectorTestLabel,
  statusFromLastTest,
} from "./connectorHealth";

describe("connectorHealth", () => {
  it("treats last_test_ok=true as passed even when status was error", () => {
    assert.equal(connectorTestHealth({ last_test_ok: true, status: "error" }), "passed");
    assert.equal(connectorLooksHealthy({ last_test_ok: true, status: "error" }), true);
    assert.equal(connectorNeedsAttention({ last_test_ok: true, status: "error" }), false);
    assert.equal(connectorTestLabel({ last_test_ok: true, status: "error" }), "Test passed");
  });

  it("does not count never-tested as a passed probe", () => {
    assert.equal(connectorPassedProbe({}), false);
    assert.equal(connectorPassedProbe({ last_test_ok: true }), true);
    assert.equal(connectorLooksHealthy({}), true);
  });

  it("treats last_test_ok=false as failed", () => {
    assert.equal(connectorTestHealth({ last_test_ok: false, status: "configured" }), "failed");
    assert.equal(connectorLooksHealthy({ last_test_ok: false }), false);
  });

  it("coerces string/number booleans from Mongo payloads", () => {
    assert.equal(coerceLastTestOk("true"), true);
    assert.equal(coerceLastTestOk("false"), false);
    assert.equal(coerceLastTestOk(1), true);
    assert.equal(coerceLastTestOk(0), false);
    assert.equal(connectorTestHealth({ last_test_ok: "true" as unknown as boolean }), "passed");
  });

  it("derives status from probe without sticky error", () => {
    assert.equal(statusFromLastTest(true), "configured");
    assert.equal(statusFromLastTest(false), "error");
    assert.equal(statusFromLastTest(undefined), "configured");
  });
});
