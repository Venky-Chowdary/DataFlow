/**
 * D432: deleting a connector that schedules use names them and needs an
 * acknowledgement — the app sends the same acknowledgement REST and MCP require.
 */
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, { window: dom.window, document: dom.window.document, localStorage: dom.window.localStorage });
const realFetch = globalThis.fetch;
const calls: string[] = [];
const refusal = {
  error: { code: "platform.state_conflict", title: "Not possible right now", message: "1 schedule uses the connection" },
  detail: {
    code: "connector_in_use",
    message: "1 schedule uses the connection “QAR70-PGTMP”: “QAR70-SBIND-pgtmp-pg”. Datawrap will not delete it silently.",
    bound_schedules: [{ id: "s-1", name: "QAR70-SBIND-pgtmp-pg", enabled: false, role: "source", running: false }],
    options: [{ action: "pause" }, { action: "repoint" }, { action: "delete_schedules" }],
  },
};
globalThis.fetch = (async (input: RequestInfo | URL) => {
  const url = String(input);
  calls.push(url);
  if (url.includes("bound_schedules=pause")) {
    return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "Content-Type": "application/json" } });
  }
  return new Response(JSON.stringify(refusal), { status: 409, headers: { "Content-Type": "application/json" } });
}) as typeof fetch;
const { ConnectorInUseError, deleteConnector } = await import("./api.js");
after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

describe("deleting a connector that schedules use", () => {
  it("surfaces the bound schedules from the 409 instead of a generic failure", async () => {
    await assert.rejects(() => deleteConnector("c-1"), (err: unknown) => {
      assert.ok(err instanceof ConnectorInUseError);
      assert.deepEqual(err.boundSchedules.map((s) => s.name), ["QAR70-SBIND-pgtmp-pg"]);
      assert.match(err.message, /will not delete it silently/);
      return true;
    });
  });

  it("sends the choice and the acknowledged schedule ids", async () => {
    await deleteConnector("c-1", { action: "pause", scheduleIds: ["s-1", "s-2"] });
    const sent = new URL(calls[calls.length - 1], "http://localhost/");
    assert.equal(sent.searchParams.get("bound_schedules"), "pause");
    assert.equal(sent.searchParams.get("acknowledge_schedules"), "s-1,s-2");
  });
});
