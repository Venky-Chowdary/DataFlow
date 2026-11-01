/**
 * Run: npx --yes tsx --test apps/web/src/lib/auditChain.test.ts
 *
 * QA (557f556c): a workspace admin read "Chain verification failed — 1371
 * finding(s) in other workspaces (not shown)". A workspace verdict covers that
 * workspace's own chain (R7); other tenants' findings are the platform admin's.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  chainNotChecked,
  chainVerdictTitle,
  chainVerdictToast,
  chainVerifiedWithExceptions,
} from "./auditChain.js";

describe("chainVerdictTitle", () => {
  it("names an intact platform walk", () => {
    assert.equal(
      chainVerdictTitle({ verified: true, checked: 12, findings: [], scope: "platform" }),
      "Chain intact — 12 records re-walked",
    );
  });

  it("says the verdict is this workspace's own chain", () => {
    const title = chainVerdictTitle({ verified: true, checked: 7, findings: [], scope: "workspace" });
    assert.equal(title, "This workspace's chain is intact — 7 records re-walked");
  });

  it("never reports other workspaces' findings to a workspace admin", () => {
    // An older API still sends a withheld count; it is not this workspace's verdict.
    const report = { verified: false, checked: 8, findings: [], withheld_findings: 1371, scope: "workspace" as const };
    assert.doesNotMatch(chainVerdictTitle(report), /other workspace|1371|withheld/i);
    assert.doesNotMatch(chainVerdictToast(report), /other workspace|1371|withheld/i);
  });

  it("names this workspace's own broken records", () => {
    const title = chainVerdictTitle({
      verified: false,
      checked: 8,
      findings: [{ kind: "event_hash_mismatch", event_id: "mine" }],
      scope: "workspace",
    });
    assert.equal(title, "Chain verification failed — 1 record(s) in this workspace");
  });

  it("names the workspace count on the platform walk", () => {
    const title = chainVerdictTitle({
      verified: false,
      checked: 50,
      findings: [
        { kind: "fork", event_id: "a", workspace_id: "ws-1" },
        { kind: "fork", event_id: "b", workspace_id: "ws-2" },
        { kind: "fork", event_id: "c", workspace_id: "ws-2" },
      ],
      scope: "platform",
    });
    assert.equal(title, "Chain verification failed — 3 record(s) across 2 workspace(s)");
  });

  it("counts re-anchored records separately from findings", () => {
    const title = chainVerdictTitle({
      verified: true,
      checked: 40,
      findings: [],
      reanchored: [{ event_id: "x", kind: "event_hash_mismatch" }],
      scope: "workspace",
    });
    assert.equal(title, "This workspace's chain is intact — 40 records re-walked (1 re-anchored)");
  });
});

describe("chainVerdictToast", () => {
  it("does not leak an event id", () => {
    const msg = chainVerdictToast({
      verified: false,
      checked: 4,
      findings: [{ kind: "event_hash_mismatch", event_id: "dfc-123" }],
      scope: "workspace",
    });
    assert.match(msg, /1 record\(s\) in this workspace do not hold up/);
    assert.doesNotMatch(msg, /dfc-/);
  });
});

describe("nothing walked is never a verdict (A1, round 11)", () => {
  it("does not call an empty walk intact", () => {
    const report = { verified: null, status: "nothing_checked", checked: 0, findings: [], scope: "workspace" as const };
    assert.equal(chainNotChecked(report), true);
    assert.doesNotMatch(chainVerdictTitle(report), /intact|verified/i);
    assert.match(chainVerdictTitle(report), /Nothing to verify/);
    assert.match(chainVerdictToast(report), /nothing was verified/);
  });

  it("says the store could not be read", () => {
    const report = { verified: null, status: "store_unavailable", checked: 0, findings: [], scope: "workspace" as const };
    assert.match(chainVerdictTitle(report), /could not be read/);
    assert.doesNotMatch(chainVerdictToast(report), /intact|none altered/i);
  });

  it("names the newest window when the limit left older records out", () => {
    const title = chainVerdictTitle({
      verified: true, status: "verified", checked: 5, records_in_scope: 12, truncated_by_limit: true,
      findings: [], scope: "workspace",
    });
    assert.equal(title, "This workspace's chain is intact — 5 records re-walked (newest 5 of 12)");
  });
});

describe("records signed with a lost key (QA r12 #3)", () => {
  const report = {
    verified: false,
    status: "verified_with_exceptions",
    checked: 1983,
    findings: [{ kind: "key_mismatch", event_id: "e1" }],
    unverifiable_key_lost: { count: 326, by_key_id: { ak_ca4273e3aaaaaaaa: 326 } },
    scope: "workspace" as const,
  };

  it("is neither tampering nor verified, and names the count and the key", () => {
    assert.equal(chainVerifiedWithExceptions(report), true);
    assert.equal(chainNotChecked(report), false);
    const title = chainVerdictTitle(report);
    assert.match(title, /No tampering found — 1657 of 1983 records verified/);
    assert.match(title, /326 record\(s\) signed with key ak_ca4273e3aaaaaaaa/);
    assert.doesNotMatch(title, /failed|intact/i);
    assert.match(chainVerdictToast(report), /No tampering found; 326 record/);
  });
});
