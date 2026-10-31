import { describe, expect, it } from "vitest";
import { PLATFORM_SETTINGS_REASON, platformLock } from "./platformSettings";

describe("platformLock", () => {
  it("locks only on an explicit editable:false and carries the API's reason", () => {
    expect(platformLock({ editable: false, editable_reason: "Ask the platform admin" })).toEqual({
      locked: true,
      reason: "Ask the platform admin",
    });
  });

  it("falls back to the standard reason when the API gives none", () => {
    expect(platformLock({ editable: false, editable_reason: null })).toEqual({
      locked: true,
      reason: PLATFORM_SETTINGS_REASON,
    });
  });

  it("stays editable when the read says so or predates the field", () => {
    expect(platformLock({ editable: true, editable_reason: null }).locked).toBe(false);
    expect(platformLock({}).locked).toBe(false);
    expect(platformLock(null).locked).toBe(false);
  });
});
