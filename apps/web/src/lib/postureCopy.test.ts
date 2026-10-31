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

  it("IP allowlist with no proxy hop count reads not enforced and says who fixes it", () => {
    const row = ipAllowlistRow({
      ...base,
      ip_allowlist_enforced: false,
      enforcement: { ip_allowlist: { status: "untrusted_client_ip" } },
    } as SecurityPosture);
    assert.equal(row.on, false);
    assert.match(row.desc, /proxy hop count is not configured/);
    assert.match(row.desc, /platform administrator/);
    const f = tenantSaveFeedback({
      ip_allowlist: { enforced: false, status: "untrusted_client_ip" },
      session_timeout: { enforced: true, status: "enforced", hours: 8 },
      warnings: ["The IP allowlist is saved but not enforced: the platform's proxy hop count is not configured ..."],
    });
    assert.equal(f.tone, "warning");
    assert.ok(f.lines.includes("IP allowlist: saved, not enforced (requests are only logged)."));
  });

  it("save feedback reports MFA from the API, not a fixed 'not enforced'", () => {
    const on = tenantSaveFeedback({ mfa: { enforced: true, status: "enforced" }, warnings: [] });
    assert.ok(on.lines.includes("Two-factor authentication: enforced."));
    const off = tenantSaveFeedback({ mfa: { enforced: false, status: "available" }, warnings: [] });
    assert.ok(off.lines.includes("Two-factor authentication: available, not required."));
  });

  it("IP allowlist enforced covers every host", () => {
    const row = ipAllowlistRow({ ...base, ip_allowlist_enforced: true } as SecurityPosture);
    assert.equal(row.on, true);
    assert.doesNotMatch(row.desc, /custom domain/);
  });

  it("MFA is badged enforced only when the API says it is enforced", () => {
    assert.equal(mfaRow().on, false);
    assert.equal(mfaRow({ ...base, mfa_required: true, mfa_enforced: false } as SecurityPosture).on, false);
    assert.match(mfaRow(base).desc, /not required/);
    const on = mfaRow({
      ...base,
      mfa_required: true,
      mfa_enforced: true,
      enforcement: { mfa: { enforced: true, sso_exempt: true, members_total: 4, members_enrolled: 1 } },
    } as SecurityPosture);
    assert.equal(on.on, true);
    assert.match(on.desc, /1 of 4 members/);
    assert.match(on.desc, /SSO sessions are exempt/);
    assert.match(on.desc, /API keys are not affected/);
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
