/**
 * The Map step's witness runs (round 2, N5): a failure stays visible with a
 * retry; "Check mapping" stays offered until a check succeeded for the current
 * inputs; a warehouse is read only by an explicit check, never by a later edit.
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import React from "react";
import { useMappingWitness, type MappingWitnessInput, type MappingWitnessState } from "./useMappingWitness.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("useMappingWitness", () => {
  let dom: JSDOM;
  const saved = { fetch: globalThis.fetch };
  const calls: Array<{ trigger: string }> = [];
  let respond: () => Response = () => new Response("{}", { status: 500 });

  before(() => {
    dom = new JSDOM("<!doctype html><html><body><div id=root></div></body></html>", { url: "http://localhost/" });
    Object.assign(globalThis, {
      window: dom.window, document: dom.window.document, localStorage: dom.window.localStorage,
      sessionStorage: dom.window.sessionStorage, IS_REACT_ACT_ENVIRONMENT: true,
    });
    // Node 20 (CI) has no global navigator; react-dom/client reads it on load.
    Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
    globalThis.fetch = (async (_input: RequestInfo | URL, init: RequestInit = {}) => {
      calls.push({ trigger: JSON.parse(String(init.body || "{}")).trigger });
      return respond();
    }) as typeof fetch;
  });
  after(() => {
    globalThis.fetch = saved.fetch;
    dom.window.close();
  });

  const ok = () =>
    new Response(JSON.stringify({
      mapping_digest: "d".repeat(64), report: { pairs: [], summary: {}, mode: "keyed", sample: {} },
      report_sha256: "r", recorded_at: "2026-11-22T00:00:00Z",
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  const timeout = () => new Response(JSON.stringify({ detail: "The mapping check did not finish in time." }),
    { status: 504, headers: { "Content-Type": "application/json" } });

  async function mount(props: MappingWitnessInput) {
    const { createRoot } = await import("react-dom/client");
    const { act } = await import("react");
    const state: { current: MappingWitnessState | null } = { current: null };
    function Harness(p: MappingWitnessInput) {
      state.current = useMappingWitness(p);
      return null;
    }
    const root = createRoot(document.getElementById("root")!);
    await act(async () => root.render(<Harness {...props} />));
    return {
      state,
      rerender: async (next: MappingWitnessInput) => act(async () => root.render(<Harness {...next} />)),
      wait: async (ms: number) => act(async () => sleep(ms)),
      call: async (fn: () => void) => act(async () => fn()),
      unmount: async () => act(async () => root.unmount()),
    };
  }

  const base: MappingWitnessInput = {
    rows: [{ source: "id", target: "id" }, { source: "mail", target: "email" }],
    sampleRows: [{ id: 1, mail: "a@x.io" }],
    destEndpoint: { kind: "database", format: "snowflake", table: "CUSTOMERS" },
    targetKeyColumns: ["id"],
    candidateColumns: ["id", "email"],
    catalogReady: true,
    warehouse: true,
    sessionId: "map-session-0001",
    source: { kind: "file", name: "customers.csv" },
  };

  it("keeps Check mapping until a check succeeded for the current inputs, and edits never read the warehouse", async () => {
    calls.length = 0;
    respond = ok;
    const h = await mount(base);
    await h.wait(800);
    assert.deepEqual(calls.map((c) => c.trigger), ["auto"]);
    assert.equal(h.state.current!.needsCheck, true);

    respond = timeout;
    await h.call(() => h.state.current!.check());
    await h.wait(50);
    assert.match(h.state.current!.error, /did not finish in time/);
    assert.equal(h.state.current!.needsCheck, true, "a failed check does not count as checked");

    respond = ok;
    await h.call(() => h.state.current!.retry());
    await h.wait(50);
    assert.equal(h.state.current!.error, "");
    assert.equal(h.state.current!.needsCheck, false);
    assert.deepEqual(calls.map((c) => c.trigger), ["auto", "explicit", "explicit"]);

    await h.rerender({ ...base, sampleRows: [{ id: 1, mail: "a@x.io" }, { id: 2, mail: "b@x.io" }] });
    await h.wait(800);
    assert.equal(h.state.current!.needsCheck, true, "new inputs need a new check");
    assert.equal(calls.at(-1)!.trigger, "auto");
    await h.unmount();
  });

  it("shows an automatic run's failure and retries it", async () => {
    calls.length = 0;
    respond = timeout;
    const h = await mount({ ...base, warehouse: false, destEndpoint: { kind: "database", format: "postgresql" } });
    await h.wait(800);
    assert.match(h.state.current!.error, /did not finish in time/);
    respond = ok;
    await h.call(() => h.state.current!.retry());
    await h.wait(800);
    assert.equal(h.state.current!.error, "");
    assert.deepEqual(calls.map((c) => c.trigger), ["auto", "auto"]);
    await h.unmount();
  });
});
