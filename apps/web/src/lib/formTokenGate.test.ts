/**
 * UX-28: the API answers a signup or reset sent within 3 s of its form token, or
 * with a token it already used, with a decoy 202. The screen then said a code
 * was sent while nothing was mailed. The gate waits out the floor and hands out
 * each token once.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { FORM_TOKEN_MIN_FILL_MS, FormTokenGate } from "./signupApi.js";

function harness(tokens: Array<string | null>) {
  let clock = 1_000;
  const sleeps: number[] = [];
  const fetched: string[] = [];
  const gate = new FormTokenGate("signup", {
    fetchToken: async (purpose) => {
      fetched.push(purpose);
      return tokens.shift() ?? null;
    },
    now: () => clock,
    sleep: async (ms) => {
      sleeps.push(ms);
      clock += ms;
    },
  });
  return { gate, sleeps, fetched, advance: (ms: number) => { clock += ms; } };
}

describe("FormTokenGate", () => {
  it("matches the API's fill floor", () => {
    const api = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "api", "services", "form_token.py"), "utf8");
    assert.match(api, new RegExp(`^MIN_FILL_MS = ${FORM_TOKEN_MIN_FILL_MS}$`, "m"));
  });

  it("holds a fast submit until the token is old enough, instead of letting it be dropped", async () => {
    const h = harness(["t1", "t2"]);
    h.gate.prefetch();
    await Promise.resolve();
    h.advance(500);
    assert.equal(await h.gate.take(), "t1");
    assert.equal(h.sleeps.length, 1);
    assert.ok(h.sleeps[0] >= FORM_TOKEN_MIN_FILL_MS - 500, `waited ${h.sleeps[0]} ms`);
  });

  it("does not wait when the form took longer than the floor", async () => {
    const h = harness(["t1"]);
    h.gate.prefetch();
    await Promise.resolve();
    h.advance(10_000);
    assert.equal(await h.gate.take(), "t1");
    assert.deepEqual(h.sleeps, []);
  });

  it("never hands out the same token twice (a retry gets a fresh one)", async () => {
    const h = harness(["t1", "t2", "t3"]);
    const first = await h.gate.take();
    const second = await h.gate.take();
    assert.equal(first, "t1");
    assert.equal(second, "t2");
  });

  it("refuses to send without a token rather than letting the API fake an accept", async () => {
    const h = harness([null, null]);
    await assert.rejects(() => h.gate.take(), /Refresh the page and try again/);
  });

  it("the signup and reset forms take a token per send", () => {
    const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "SignupPage.tsx"), "utf8");
    assert.doesNotMatch(page, /fetchFormToken\(/);
    assert.equal(page.match(/await tokenGate\.take\(\)/g)?.length, 3);
  });
});
