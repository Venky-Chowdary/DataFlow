/**
 * QA enterprise (557f556c) #6-#8: posture badges must match enforcement.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { SecurityPosture } from "./api";
import { ipAllowlistRow, mfaRow, sessionTimeoutRow } from "./postureCopy.js";

const base = { session_timeout_hours: 4 } as unknown as SecurityPosture;

describe("posture rows", () => {
  it("session timeout reads enforced only when the API says so", () => {
    assert.equal(sessionTimeoutRow(base).on, false);
    assert.match(sessionTimeoutRow(base).desc, /Recorded only/);
    const on = sessionTimeoutRow({ ...base, session_timeout_enforced: true });
    assert.equal(on.on, true);
    assert.match(on.desc, /signed out/);
  });

  it("IP allowlist names what is missing", () => {
    const row = ipAllowlistRow({
      ...base,
      ip_allowlist_enforced: false,
      enforcement: { ip_allowlist: { missing: ["DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE=1"] } },
    } as SecurityPosture);
    assert.equal(row.on, false);
    assert.match(row.desc, /DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE/);
  });

  it("MFA is never badged enforced", () => {
    assert.equal(mfaRow().on, false);
  });
});
