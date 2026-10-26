import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { requestEmailChange } from "./signupApi.js";

/**
 * Back-fill for 1d8580da: the dw_csrf double-submit echo must be skipped,
 * not thrown, where there is no document (Node test runners, SSR), and
 * echoed decoded when the cookie exists.
 */
describe("authedCall dw_csrf echo", () => {
  it("sends no csrf header and does not throw without a document", async () => {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const original = globalThis.fetch;
    globalThis.fetch = (async (url: any, init: any) => {
      calls.push({ url: String(url), init });
      return new Response(
        JSON.stringify({ ok: true, challenge_id: "c1", resend_after_sec: 60 }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;
    try {
      assert.equal(typeof document, "undefined");
      const res = await requestEmailChange("new@example.io");
      assert.equal((res as { ok?: boolean }).ok, true);
      const headers = new Headers(calls[0].init.headers);
      assert.equal(headers.get("x-csrf-token"), null);
    } finally {
      globalThis.fetch = original;
    }
  });

  it("echoes dw_csrf decoded when the cookie exists", async () => {
    const calls: Array<{ init: RequestInit }> = [];
    const original = globalThis.fetch;
    (globalThis as { document?: unknown }).document = {
      cookie: "other=1; dw_csrf=tok%3Dabc",
    };
    globalThis.fetch = (async (_url: any, init: any) => {
      calls.push({ init });
      return new Response(
        JSON.stringify({ ok: true, challenge_id: "c1", resend_after_sec: 60 }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof fetch;
    try {
      await requestEmailChange("new@example.io");
      const headers = new Headers(calls[0].init.headers);
      assert.equal(headers.get("x-csrf-token"), "tok=abc");
    } finally {
      globalThis.fetch = original;
      delete (globalThis as { document?: unknown }).document;
    }
  });
});
