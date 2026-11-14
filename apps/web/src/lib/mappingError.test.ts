/**
 * Regression for D-BP-10 / ERR-39: the Map step shows the API's egress refusal
 * inline (no stall), without server environment-variable names or a guessed
 * endpoint role.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { MAPPING_TIMEOUT_MS, mappingTimeoutCopy, withEgressHint } from "./mappingError";

describe("withEgressHint", () => {
  it("passes the API's classified refusal through without server setting names (ERR-39)", () => {
    const api = "Datawrap's network policy doesn't allow connections to this host. "
      + "Ask your administrator to allow this host for your workspace.";
    const out = withEgressHint(api, "127.0.0.1", "10.0.0.5");
    assert.equal(out, api);
    assert.doesNotMatch(out, /EGRESS_ALLOWLIST|destination host/);
  });

  it("never names the destination for a refused source host", () => {
    const out = withEgressHint("Refused connector host by egress policy", "", "10.0.0.5");
    assert.doesNotMatch(out, /destination host|DATAWRAP_/);
  });

  it("leaves ordinary failures untouched", () => {
    const msg = "PostgreSQL rejected the sign-in for user \"dataflow\": the username or password is wrong.";
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
