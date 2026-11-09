import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { apiFetch } from "./api.js";

/**
 * F5 "Loading Settings…": with the API hung, the shell's calls held all 6
 * HTTP/1.1 sockets per host and the Settings chunk waited ~11 s in the
 * browser queue (Dev2 CDP capture). apiFetch must never hold more than a
 * bounded share of those sockets, queue the rest in order, and drain.
 */
const BROWSER_SOCKETS_PER_HOST = 6;

type Pending = { url: string; resolve: (res: Response) => void };

function installHungFetch(): { pending: Pending[]; restore: () => void } {
  const pending: Pending[] = [];
  const originalFetch = globalThis.fetch;
  const g = globalThis as { window?: unknown };
  const hadWindow = "window" in g;
  if (!hadWindow) g.window = globalThis;
  globalThis.fetch = ((url: RequestInfo | URL, init?: RequestInit) =>
    new Promise<Response>((resolve, reject) => {
      pending.push({ url: String(url), resolve });
      init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")));
    })) as typeof fetch;
  return {
    pending,
    restore: () => {
      globalThis.fetch = originalFetch;
      if (!hadWindow) delete g.window;
    },
  };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
const ok = () => new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });

describe("apiFetch in-flight cap", () => {
  it("leaves browser sockets free while API calls hang, then drains in order", async () => {
    const { pending, restore } = installHungFetch();
    try {
      const calls = Array.from({ length: 8 }, (_, i) => apiFetch(`/api/v1/hung/${i}`, { timeoutMs: 60_000 }));
      await flush();
      assert.ok(pending.length > 0, "the first requests must be sent");
      assert.ok(
        pending.length <= BROWSER_SOCKETS_PER_HOST - 2,
        `${pending.length} API requests in flight leaves under 2 sockets for module/chunk loads`,
      );
      const cap = pending.length;

      pending[0].resolve(ok());
      await flush();
      assert.equal(pending.length, cap + 1, "a settled request hands its slot to the next queued one");
      assert.equal(pending[cap].url, `/api/v1/hung/${cap}`, "queued requests are sent in call order");

      let sent = pending.length;
      while (sent < 8) {
        pending[sent - cap].resolve(ok());
        await flush();
        assert.equal(pending.length, sent + 1);
        sent += 1;
      }
      for (const p of pending.slice(sent - cap)) p.resolve(ok());
      const results = await Promise.all(calls);
      assert.equal(results.length, 8);
      assert.deepEqual(pending.map((p) => p.url), Array.from({ length: 8 }, (_, i) => `/api/v1/hung/${i}`));
    } finally {
      restore();
    }
  });

  it("times out a queued request without sending it or leaking its slot", async () => {
    const { pending, restore } = installHungFetch();
    try {
      const holders = Array.from({ length: 8 }, (_, i) => apiFetch(`/api/v1/hold/${i}`, { timeoutMs: 60_000 }));
      await flush();
      const cap = pending.length;
      const queued = apiFetch("/api/v1/queued", { timeoutMs: 20 });
      await assert.rejects(queued, /Request timed out/);
      assert.ok(!pending.some((p) => p.url === "/api/v1/queued"), "a timed-out waiter is never sent");

      for (let i = 0; i < 8; i += 1) {
        pending[i].resolve(ok());
        await flush();
      }
      await Promise.all(holders);
      assert.equal(pending.length, 8);

      const after = Array.from({ length: cap }, (_, i) => apiFetch(`/api/v1/after/${i}`, { timeoutMs: 60_000 }));
      await flush();
      assert.equal(pending.length, 8 + cap, "every slot is free again once the calls settle");
      for (const p of pending.slice(8)) p.resolve(ok());
      await Promise.all(after);
    } finally {
      restore();
    }
  });
});
