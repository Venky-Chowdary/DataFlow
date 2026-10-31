/**
 * Run: npx --yes tsx --test apps/web/src/lib/auditChain.test.ts
 *
 * QA (557f556c): a workspace admin read "Chain verification failed — 1371
 * finding(s) in other workspaces (not shown)". A workspace verdict covers that
 * workspace's own chain (R7); other tenants' findings are the platform admin's.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { chainVerdictTitle, chainVerdictToast } from "./auditChain.js";

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
