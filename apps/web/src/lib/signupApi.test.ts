import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { requestEmailChange, verifiedSignupSession, verifyEmail } from "./signupApi.js";
import { apiBearerToken, readSession, writeSession } from "./session.js";

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

function memoryStorage(): Storage {
  const data = new Map<string, string>();
  return {
    get length() { return data.size; },
    clear: () => data.clear(),
    getItem: (k: string) => data.get(k) ?? null,
    key: (i: number) => [...data.keys()][i] ?? null,
    removeItem: (k: string) => { data.delete(k); },
    setItem: (k: string, v: string) => { data.set(k, String(v)); },
  };
}

/**
 * D153: signup -> verify -> the next protected navigation. The app guard reads
 * the stored session back with readSession(), and every API call signs with
 * apiBearerToken(); both must see the session verify-email opened.
 */
describe("verified signup session (D153)", () => {
  async function withVerifyResponse(body: unknown, run: () => Promise<void>) {
    const g = globalThis as { localStorage?: Storage; sessionStorage?: Storage };
    const original = globalThis.fetch;
    g.localStorage = memoryStorage();
    g.sessionStorage = memoryStorage();
    globalThis.fetch = (async () => new Response(JSON.stringify(body), {
      status: 200, headers: { "Content-Type": "application/json" },
    })) as typeof fetch;
    try {
      await run();
    } finally {
      globalThis.fetch = original;
      delete g.localStorage;
      delete g.sessionStorage;
    }
  }

  it("stays signed in after verify: the stored session carries the issued token", async () => {
    const expires = Math.floor(Date.now() / 1000) + 3600;
    await withVerifyResponse({
      token: "ada@example.com:123:jti:sig",
      expires_at: expires,
      user: { email: "ada@example.com", name: "Ada", role: "admin" },
      next: "onboarding",
    }, async () => {
      const result = await verifyEmail({ challengeId: "otc_1", code: "123456" });
      writeSession(verifiedSignupSession(result), true);
      const session = readSession();
      assert.ok(session, "a verified signup must read back as signed in");
      assert.equal(session.email, "ada@example.com");
      assert.equal(session.expires_at, expires);
      assert.equal(apiBearerToken(session), "ada@example.com:123:jti:sig");
    });
  });

  it("refuses a verify response without a token instead of storing an empty one", async () => {
    await withVerifyResponse({
      user: { email: "ada@example.com" },
      next: "onboarding",
    }, async () => {
      await assert.rejects(
        verifyEmail({ challengeId: "otc_1", code: "123456" }),
        (err: { code?: string }) => err.code === "session_missing",
      );
      assert.equal(readSession(), null);
    });
  });
});
