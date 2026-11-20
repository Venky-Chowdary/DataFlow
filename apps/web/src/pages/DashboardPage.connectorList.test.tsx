/**
 * D410 on the Overview, the screen a signed-in user lands on. While the
 * connector list is still loading, or after every read of it failed, the
 * rows are [] but the workspace may have connections: the Overview must say
 * "loading" or "couldn't load" with Try again, never "No saved connections."
 * or the empty data plane. Only a successful empty answer shows those.
 *
 * The props come from the shell's own reducer (connectorListLoad), so this
 * drives the same states the app reaches.
 */
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  MutationObserver: dom.window.MutationObserver,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
dom.window.requestAnimationFrame = (callback) => {
  callback(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
Object.defineProperty(globalThis, "localStorage", { value: dom.window.localStorage, configurable: true });
// The Overview's own side reads (freshness, DLQ, approvals) are not under test.
globalThis.fetch = (async () => {
  throw new Error("offline in test");
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ToastProvider } = await import("../components/Toast.js");
const { DashboardPage } = await import("./DashboardPage.js");
const {
  connectorListFailed,
  connectorListLoaded,
  connectorListPageState,
  connectorListStarted,
  connectorListView,
  initialConnectorList,
} = await import("../lib/connectorListLoad.js");

type Row = { id: string; name: string; type: string; last_test_ok?: boolean | null };
const HISTORY = { jobs: [], total: 0, filteredTotal: 0, statusCounts: {} };

const pending = connectorListStarted(initialConnectorList<Row>(), { gen: 1, workspaceId: "ws" });
const failed = connectorListFailed(pending, { gen: 1, workspaceId: "ws", message: "Request timed out" });
const loadedEmpty = connectorListLoaded(pending, { gen: 1, workspaceId: "ws", rows: [] });

async function renderOverview(state: typeof pending, onRetry?: () => void) {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  const props = {
    connectors: state.rows,
    jobs: [],
    history: HISTORY,
    schedules: [],
    onOpenConnectors: () => {},
    ...connectorListPageState(connectorListView(state), onRetry),
  };
  await act(async () => root.render(createElement(ToastProvider, null, createElement(DashboardPage, props as never))));
  return {
    html: () => document.body.innerHTML,
    host,
    async unmount() {
      await act(async () => root.unmount());
      document.body.innerHTML = "";
    },
  };
}

after(() => dom.window.close());

describe("Overview connector list states (D410)", () => {
  it("shows loading, never the empty state, while the first read may still answer", async () => {
    const view = await renderOverview(pending);
    const html = view.html();
    assert.match(html, /Loading connections/);
    assert.doesNotMatch(html, /No saved connections/);
    assert.doesNotMatch(html, /No connections yet/);
    assert.doesNotMatch(html, /Connect sources and destinations to activate your data plane/);
    await view.unmount();
  });

  it("shows the failure with its reason and a working Try again after every read failed", async () => {
    let retried = 0;
    const view = await renderOverview(failed, () => {
      retried += 1;
    });
    try {
      const html = view.html();
      assert.match(html, /Couldn't load your connections/);
      assert.match(html, /The request for your connections timed out\. Reading the list doesn't change your connections/);
      assert.match(html, /role="alert"/);
      assert.doesNotMatch(html, /No saved connections/);
      assert.doesNotMatch(html, /No connections yet/);
      assert.doesNotMatch(html, /Connect sources and destinations to activate your data plane/);
      // The connector notice's own button (the parity card has its own Try again).
      const buttons = [...view.host.querySelectorAll('[aria-label="Couldn\'t load your connections"] button')].filter((b) =>
        /Try again/.test(b.textContent || ""),
      );
      assert.ok(buttons.length >= 1, "a Try again button is offered");
      await act(async () => buttons[0].dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true })));
      assert.equal(retried, 1);
    } finally {
      await view.unmount();
    }
  });

  it("says there are no saved connections only after a successful empty answer", async () => {
    const view = await renderOverview(loadedEmpty);
    const html = view.html();
    assert.match(html, /No saved connections/);
    assert.doesNotMatch(html, /Loading connections/);
    assert.doesNotMatch(html, /Couldn't load your connections/);
    await view.unmount();
  });
});
