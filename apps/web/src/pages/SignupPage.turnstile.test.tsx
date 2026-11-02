/**
 * Regression for 82488bbd (loop review 8, item 10): the Turnstile widget must
 * render with the app theme and `size: "flexible"` so it stretches to the
 * field width, not the fixed light default that mismatched the dark card.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { turnstileRenderOptions } from "./SignupPage";

describe("turnstileRenderOptions", () => {
  it("follows the app theme", () => {
    assert.equal(turnstileRenderOptions("k", "dark", () => {}).theme, "dark");
    assert.equal(turnstileRenderOptions("k", "light", () => {}).theme, "light");
  });

  it("stretches to the field width", () => {
    assert.equal(turnstileRenderOptions("k", "dark", () => {}).size, "flexible");
  });

  it("passes the sitekey and wires callbacks", () => {
    const tokens: string[] = [];
    const opts = turnstileRenderOptions("site-key-1", "dark", (t) => tokens.push(t));
    assert.equal(opts.sitekey, "site-key-1");
    (opts.callback as (t: string) => void)("tok-1");
    (opts["expired-callback"] as () => void)();
    assert.deepEqual(tokens, ["tok-1", ""]);
  });
});
