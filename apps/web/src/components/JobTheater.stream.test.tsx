/**
 * Mounted Job Theater stream. Deferred fetches prove a slower read cannot
 * put an older frame back on screen.
 *
 * Run: npx tsx --test src/components/JobTheater.stream.test.tsx
 */
import assert from "node:assert/strict";
import { afterEach, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
  url: "http://localhost/",
});
const g = globalThis as typeof globalThis & {
  window: Window;
  document: Document;
  localStorage: Storage;
  sessionStorage: Storage;
  navigator: Navigator;
  HTMLElement: typeof HTMLElement;
  Node: typeof Node;
  Element: typeof Element;
  MutationObserver: typeof MutationObserver;
  getComputedStyle: typeof getComputedStyle;
  requestAnimationFrame: (cb: FrameRequestCallback) => number;
  cancelAnimationFrame: (id: number) => void;
  IS_REACT_ACT_ENVIRONMENT: boolean;
  EventSource: typeof EventSource;
  fetch: typeof fetch;
};
g.window = dom.window as unknown as Window;
g.document = dom.window.document;
g.localStorage = dom.window.localStorage;
g.sessionStorage = dom.window.sessionStorage;
try {
  Object.defineProperty(g, "navigator", { value: dom.window.navigator, configurable: true });
} catch {
  // Node already exposes a read-only navigator. React can use that one.
}
g.HTMLElement = dom.window.HTMLElement;
g.Node = dom.window.Node;
g.Element = dom.window.Element;
g.MutationObserver = dom.window.MutationObserver;
g.getComputedStyle = dom.window.getComputedStyle.bind(dom.window);
g.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0) as unknown as number;
g.cancelAnimationFrame = (id) => clearTimeout(id);
g.IS_REACT_ACT_ENVIRONMENT = true;
g.CustomEvent = dom.window.CustomEvent as unknown as typeof CustomEvent;
g.Event = dom.window.Event as unknown as typeof Event;

type Waiter = { url: string; resolve: (res: Response) => void };
const waits: Waiter[] = [];

class FakeEventSource {
  static latest: FakeEventSource | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;
  constructor(public url: string) {
    FakeEventSource.latest = this;
  }
  close() {
    this.closed = true;
  }
  emit(body: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(body) });
  }
  fail() {
    this.onerror?.();
  }
}

function jobBody(over: Record<string, unknown>): Record<string, unknown> {
  return {
    _id: "job-a",
    workspace_id: "ws-a",
    source_type: "postgresql",
    source_name: "generate_series",
    destination_type: "mysql",
    destination_database: "defaultdb",
    destination_collection: "landed",
    status: "running",
    records_processed: 10000,
    total_rows: 1_000_000,
    progress_pct: 99,
    phase: "reconcile",
    message: "10,000 of 1,000,000 (1%, 31s)",
    created_at: "2026-11-06T03:12:17.000Z",
    ...over,
  };
}

async function flush() {
  const { act } = await import("react");
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

describe("mounted Job Theater stream ordering", () => {
  let root: { render: (node: unknown) => void; unmount: () => void };
  let host: HTMLElement;
  let completed: string[];

  before(async () => {
    g.fetch = (async (input: RequestInfo | URL) => {
      const url = String(input);
      if (!url.includes("/connectors/jobs/") || url.includes("mapping-proof")) {
        return new Response("{}", { status: 404 });
      }
      return new Promise<Response>((resolve) => waits.push({ url, resolve }));
    }) as typeof fetch;
    g.EventSource = FakeEventSource as unknown as typeof EventSource;
    const { setActiveWorkspaceId } = await import("../lib/workspace.js");
    setActiveWorkspaceId("ws-a");
  });

  afterEach(() => {
    waits.length = 0;
    FakeEventSource.latest = null;
  });

  async function mount(jobId: string) {
    const { createElement } = await import("react");
    const { createRoot } = await import("react-dom/client");
    const { JobTheater } = await import("./JobTheater.js");
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    completed = [];
    const { act } = await import("react");
    await act(async () => {
      root.render(createElement(JobTheater, {
        jobId,
        sourceLabel: "generate_series",
        destLabel: "landed",
        onComplete: () => completed.push(jobId),
      }));
    });
    await flush();
    return FakeEventSource.latest;
  }

  function banner(): string {
    return host.textContent || "";
  }

  async function emit(es: FakeEventSource, body: Record<string, unknown>) {
    const { act } = await import("react");
    await act(async () => {
      es.emit(body);
    });
  }

  async function fail(es: FakeEventSource) {
    const { act } = await import("react");
    await act(async () => {
      es.fail();
    });
  }

  async function answer(waiter: Waiter, response: Response) {
    const { act } = await import("react");
    await act(async () => {
      waiter.resolve(response);
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  }

  it("a slower poll does not replace a newer 500,000-row frame with 10,000", async () => {
    const es = await mount("job-a");
    assert.ok(es);
    await fail(es!);
    assert.equal(waits.length, 1);
    await emit(es!, jobBody({
      records_processed: 1_000_000,
      message: "500,000 of 1,000,000",
    }));
    assert.match(banner(), /500,000 of 1,000,000/);
    await answer(waits[0], new Response(JSON.stringify(jobBody({
      records_processed: 10_000,
      message: "10,000 of 1,000,000 (1%, 31s)",
    })), { status: 200, headers: { "content-type": "application/json" } }));
    assert.match(banner(), /500,000 of 1,000,000/);
    assert.equal(banner().includes("10,000 of 1,000,000 (1%, 31s)"), false);
    const { act: unmountAct } = await import("react");
    await unmountAct(async () => {
      root.unmount();
    });
  });

  it("a late poll for the previous job does not overwrite the job now on screen", async () => {
    const { createElement, act } = await import("react");
    const { JobTheater } = await import("./JobTheater.js");
    const esA = await mount("job-a");
    await fail(esA!);
    assert.equal(waits.length, 1);
    const stale = waits[0];
    await act(async () => {
      root.render(createElement(JobTheater, {
        jobId: "job-b",
        sourceLabel: "generate_series",
        destLabel: "landed",
        onComplete: () => completed.push("job-b"),
      }));
    });
    await flush();
    const esB = FakeEventSource.latest;
    assert.notEqual(esB, esA);
    await emit(esB!, jobBody({
      _id: "job-b",
      message: "job-b 200,000",
      records_processed: 200_000,
    }));
    assert.match(banner(), /job-b 200,000/);
    await answer(stale, new Response(JSON.stringify(jobBody({
      _id: "job-a",
      message: "job-a 10000",
      records_processed: 10_000,
    })), { status: 200 }));
    assert.match(banner(), /job-b 200,000/);
    assert.equal(banner().includes("job-a 10000"), false);
    const { act: unmountAct } = await import("react");
    await unmountAct(async () => {
      root.unmount();
    });
  });

  it("a newer phase message applies while the writer-ack count stays at 1,000,000", async () => {
    const es = await mount("job-a");
    await emit(es!, jobBody({
      phase: "load",
      records_processed: 1_000_000,
      message: "Writing batch 200/200 (1,000,000 rows).",
    }));
    await emit(es!, jobBody({
      phase: "reconcile",
      records_processed: 1_000_000,
      reconcile_pct: 6,
      message: "Verifying, step 1 of 2 — re-reading the source to fingerprint it: 60,000 of 1,000,000 rows (6%, 5s).",
    }));
    assert.match(banner(), /60,000 of 1,000,000/);
    const { act: unmountAct } = await import("react");
    await unmountAct(async () => {
      root.unmount();
    });
  });

  it("a missing job and a workspace change clear the frame; a terminal frame stays terminal", async () => {
    const { setActiveWorkspaceId } = await import("../lib/workspace.js");
    const es = await mount("job-a");
    await emit(es!, jobBody({ message: "500,000 of 1,000,000" }));
    await fail(es!);
    const missing = waits.at(-1)!;
    await answer(missing, new Response("{}", { status: 404 }));
    assert.match(banner(), /Connecting to live job stream/);
    assert.equal(banner().includes("500,000"), false);

    const es2 = FakeEventSource.latest!;
    await emit(es2, jobBody({ message: "500,000 of 1,000,000", status: "running" }));
    await emit(es2, jobBody({
      workspace_id: "ws-other",
      message: "other-tenant rows",
      records_processed: 1,
    }));
    assert.equal(banner().includes("other-tenant rows"), false);
    assert.match(banner(), /Connecting to live job stream/);
    await emit(es2, jobBody({ message: "500,000 of 1,000,000", status: "running" }));
    await emit(es2, jobBody({
      status: "completed",
      phase: "completed",
      records_processed: 1_000_000,
      message: "Row fidelity verified — source and target checksums match (1000000 rows).",
    }));
    assert.match(banner(), /Transfer complete/);
    await emit(es2, jobBody({
      status: "running",
      message: "10,000 of 1,000,000 (1%, 31s)",
      records_processed: 10_000,
    }));
    assert.match(banner(), /Transfer complete/);
    assert.equal(banner().includes("10,000 of 1,000,000"), false);

    const { act } = await import("react");
    await act(async () => {
      setActiveWorkspaceId("ws-b");
    });
    assert.match(banner(), /Connecting to live job stream/);
    const { act: unmountAct } = await import("react");
    await unmountAct(async () => {
      root.unmount();
    });
    setActiveWorkspaceId("ws-a");
  });

  it("unmount drops a read that resolves later, and a socket error does not mark the run failed", async () => {
    const es = await mount("job-a");
    await emit(es!, jobBody({ message: "500,000 of 1,000,000" }));
    await fail(es!);
    assert.match(banner(), /reconnecting/);
    assert.equal(banner().includes("Transfer failed"), false);
    const pending = waits.at(-1)!;
    const { act: unmountAct } = await import("react");
    await unmountAct(async () => {
      root.unmount();
    });
    await answer(pending, new Response(JSON.stringify(jobBody({
      status: "completed",
      message: "should-not-apply",
    })), { status: 200 }));
    assert.deepEqual(completed, []);
  });
});
