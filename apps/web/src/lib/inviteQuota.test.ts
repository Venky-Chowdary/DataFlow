/**
 * UX-21: an email invite refused by the daily email quota showed an opaque
 * "try again later" and left the admin with no way forward.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, { window: dom.window, document: dom.window.document, localStorage: dom.window.localStorage });
const realFetch = globalThis.fetch;
globalThis.fetch = (async () =>
  new Response(
    JSON.stringify({ error: { code: "email_quota_exceeded", message: "Datawrap has reached its email sending limit for now, so no invitation email was sent and nothing was created. Try again in 6 min, or use Add directly to give them access now with a one-time password.", retry_after_sec: 301 } }),
    { status: 503, headers: { "Content-Type": "application/json" } },
  )) as typeof fetch;
const { sendWorkspaceInvite } = await import("./api.js");
after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

describe("invite refused by the email quota", () => {
  it("carries the API's code and its words", async () => {
    await assert.rejects(() => sendWorkspaceInvite("ws1", "a@example.com", "viewer"), (err: Error & { code?: string }) => {
      assert.equal(err.code, "email_quota_exceeded");
      assert.match(err.message, /email sending limit/);
      assert.match(err.message, /Add directly/);
      return true;
    });
  });

  it("Team switches to Add directly and keeps what was typed", () => {
    const team = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "settings", "TeamSettings.tsx"), "utf8");
    const block = team.slice(team.indexOf('const quota = (err as { code?: string } | null)?.code === "email_quota_exceeded";'));
    assert.match(block, /if \(quota\) \{[\s\S]*setInviteMode\("direct"\);/);
    assert.doesNotMatch(block.slice(0, 600), /setInviteEmail\(""\)/);
  });
});
