/**
 * Regression for D-BP-10: the Map step stalled on "Preparing column mappings"
 * forever with no error when the egress allowlist blocked the connector host.
 * These tests pin the surfaced copy: the blocked host + the setting to change.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { MAPPING_TIMEOUT_MS, mappingTimeoutCopy, withEgressHint } from "./mappingError";

describe("withEgressHint", () => {
  it("names the blocked host and the allowlist setting on an egress refusal", () => {
    const out = withEgressHint(
      "Connector probe refused: egress allowlist denied 127.0.0.1",
      "127.0.0.1",
      "",
    );
    assert.match(out, /egress allowlist denied 127\.0\.0\.1/);
    assert.match(out, /“127\.0\.0\.1”/);
    assert.match(out, /DATAFLOW_EGRESS_ALLOWLIST/);
  });

  it("falls back to the source host when no destination host is known", () => {
    const out = withEgressHint("non-routable private range refused", "", "10.0.0.5");
    assert.match(out, /“10\.0\.0\.5”/);
    assert.match(out, /DATAFLOW_EGRESS_ALLOWLIST/);
  });

  it("still appends the remedy when no host is known", () => {
    const out = withEgressHint("Refused connector host by egress policy");
    assert.match(out, /DATAFLOW_EGRESS_ALLOWLIST/);
    assert.match(out, /reachable host/);
  });

  it("leaves ordinary failures untouched", () => {
    const msg = "password authentication failed for user dataflow";
    assert.equal(withEgressHint(msg, "db.internal"), msg);
  });

  it("leaves empty input untouched", () => {
    assert.equal(withEgressHint(""), "");
  });
});

describe("mapping timeout fallback", () => {
  it("exports a bounded watchdog budget and operator copy", () => {
    assert.ok(MAPPING_TIMEOUT_MS >= 30_000 && MAPPING_TIMEOUT_MS <= 300_000);
    assert.match(mappingTimeoutCopy, /Column mapping did not finish/);
    assert.match(mappingTimeoutCopy, /retry/i);
  });
});
