/**
 * Run: npx --yes tsx --test apps/web/src/lib/auditChain.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { chainVerdictTitle, chainVerdictToast } from "./auditChain.js";

describe("chainVerdictTitle", () => {
  it("names an intact global walk", () => {
    assert.equal(
      chainVerdictTitle({ verified: true, checked: 12, findings: [] }),
      "Chain intact — 12 records re-walked",
    );
  });

  it("does not claim 0 local failures when only other workspaces broke", () => {
    const title = chainVerdictTitle({
      verified: false,
      checked: 8,
      findings: [],
      withheld_findings: 2,
    });
    assert.match(title, /2 finding\(s\) in other workspaces \(not shown\)/);
    assert.doesNotMatch(title, /0 record/);
  });

  it("names this workspace's findings and the withheld count together", () => {
    const title = chainVerdictTitle({
      verified: false,
      checked: 8,
      findings: [{ kind: "event_hash_mismatch", event_id: "mine" }],
      withheld_findings: 3,
    });
    assert.match(title, /1 record\(s\) in this workspace/);
    assert.match(title, /3 withheld/);
  });
});

describe("chainVerdictToast", () => {
  it("does not leak a foreign event id — withheld is a count", () => {
    const msg = chainVerdictToast({
      verified: false,
      checked: 4,
      findings: [],
      withheld_findings: 1,
    });
    assert.match(msg, /1 finding/);
    assert.doesNotMatch(msg, /event_id|dfc-|mallory/);
  });
});
