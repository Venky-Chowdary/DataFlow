/**
 * Run: npx --yes tsx --test apps/web/src/components/ui/liveEventLog.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { isNewLiveLogTail } from "./LiveEventLog.js";

describe("isNewLiveLogTail", () => {
  it("enters only the first time a tail id is seen", () => {
    assert.equal(isNewLiveLogTail(4, null), true);
    assert.equal(isNewLiveLogTail(4, 4), false);
    assert.equal(isNewLiveLogTail(5, 4), true);
  });

  it("does not enter an empty log", () => {
    assert.equal(isNewLiveLogTail(0, null), false);
    assert.equal(isNewLiveLogTail(0, 0), false);
  });
});
