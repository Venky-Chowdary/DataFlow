/**
 * Run: npx --yes tsx --test apps/web/src/lib/decimalScaleHonesty.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  DEST_SCALE_PADDING_HONESTY,
  fractionalTrailingZerosSameValue,
} from "./decimalScaleHonesty.js";

describe("fractionalTrailingZerosSameValue", () => {
  it("treats Snowsight padding as the same flights clock value", () => {
    assert.equal(fractionalTrailingZerosSameValue("9.083333", "9.083333000000"), true);
    assert.equal(fractionalTrailingZerosSameValue("12.483334", "12.483334000000"), true);
    assert.equal(fractionalTrailingZerosSameValue("8.95", "8.950000000000"), true);
  });

  it("refuses a left-shift that would actually increase the time", () => {
    assert.equal(fractionalTrailingZerosSameValue("9.083333", "908333.3"), false);
  });

  it("states the operator rule on the result screen", () => {
    assert.match(DEST_SCALE_PADDING_HONESTY, /display scale/i);
    assert.match(DEST_SCALE_PADDING_HONESTY, /did not increase/i);
    assert.match(DEST_SCALE_PADDING_HONESTY, /observed scale/i);
  });
});
