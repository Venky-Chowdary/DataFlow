import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { PLATFORM_SETTINGS_REASON, platformLock } from "./platformSettings.js";

describe("platformLock", () => {
  it("locks only on an explicit editable:false and carries the API's reason", () => {
    assert.deepEqual(platformLock({ editable: false, editable_reason: "Ask the platform admin" }), {
      locked: true,
      reason: "Ask the platform admin",
    });
  });

  it("falls back to the standard reason when the API gives none", () => {
    assert.deepEqual(platformLock({ editable: false, editable_reason: null }), {
      locked: true,
      reason: PLATFORM_SETTINGS_REASON,
    });
  });

  it("stays editable when the read says so or predates the field", () => {
    assert.equal(platformLock({ editable: true, editable_reason: null }).locked, false);
    assert.equal(platformLock({}).locked, false);
    assert.equal(platformLock(null).locked, false);
  });
});
