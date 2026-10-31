/**
 * QA enterprise (557f556c) #6-#8 and QA round 11 (9c75ada4) S7: posture
 * badges and the Enterprise save feedback must match enforcement, in
 * admin-facing words (no deployment variable names).
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { SecurityPosture, TenantEnforcement } from "./api";
import { ipAllowlistRow, mfaRow, sessionTimeoutRow, tenantSaveFeedback } from "./postureCopy.js";

const base = { session_timeout_hours: 4 } as unknown as SecurityPosture;
const ENV_NAME = /DATAFLOW_|DATAWRAP_/;

describe("posture rows", () => {
  it("session timeout reads enforced only when the API says so", () => {
    assert.equal(sessionTimeoutRow(base).on, false);
    assert.match(sessionTimeoutRow(base).desc, /Recorded only/);
    const on = sessionTimeoutRow({ ...base, session_timeout_enforced: true });
    assert.equal(on.on, true);
    assert.match(on.desc, /signed out/);
  });

  it("IP allowlist saved but switched off asks the platform administrator, without env names", () => {
    const row = ipAllowlistRow({
      ...base,
      ip_allowlist_enforced: false,
      enforcement: { ip_allowlist: { status: "audit_only", missing: ["IP allowlist enforcement for this deployment"] } },
    } as SecurityPosture);
    assert.equal(row.on, false);
    assert.match(row.desc, /platform administrator/);
    assert.doesNotMatch(row.desc, ENV_NAME);
  });

  it("IP allowlist enforced covers every host", () => {
    const row = ipAllowlistRow({ ...base, ip_allowlist_enforced: true } as SecurityPosture);
    assert.equal(row.on, true);
    assert.doesNotMatch(row.desc, /custom domain/);
  });

  it("MFA is never badged enforced", () => {
    assert.equal(mfaRow().on, false);
  });
});

describe("tenant save feedback", () => {
  it("all enforced: success, and it says what is enforced (never 'applied')", () => {
    const e: TenantEnforcement = {
      ip_allowlist: { enforced: true, status: "enforced" },
      session_timeout: { enforced: true, status: "enforced", hours: 4 },
      warnings: [],
    };
    const f = tenantSaveFeedback(e);
    assert.equal(f.tone, "success");
    assert.match(f.message, /IP allowlist: enforced/);
    assert.match(f.message, /Session timeout: enforced \(4h idle\)/);
    assert.doesNotMatch(f.message, /applied/);
  });

  it("anything not enforced: warning with the server's admin-facing reason", () => {
    const e: TenantEnforcement = {
      ip_allowlist: { enforced: false, status: "audit_only" },
      session_timeout: { enforced: false, status: "recorded_only", hours: 8 },
      warnings: ["The IP allowlist is saved but not enforced ... Ask your platform administrator to enable IP allowlist enforcement."],
    };
    const f = tenantSaveFeedback(e);
    assert.equal(f.tone, "warning");
    assert.match(f.title, /not everything is enforced/);
    assert.match(f.message, /platform administrator/);
    assert.ok(f.lines.some((l) => /recorded, not enforced/.test(l)));
    assert.ok(f.lines.every((l) => !ENV_NAME.test(l)));
  });

  it("a response without enforcement never claims success", () => {
    assert.equal(tenantSaveFeedback(undefined).tone, "warning");
  });
});
