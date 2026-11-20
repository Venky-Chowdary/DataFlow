/**
 * D410: the Connectors page never draws "No connections yet" unless the list
 * really loaded empty. While a read may still answer it shows loading; when
 * the read failed it says so with Try again, and the tab shows no "(0)".
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

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ToastProvider } = await import("../components/Toast.js");
const { ConnectorsPage } = await import("./ConnectorsPage.js");

const noop = () => {};

async function render(props: Record<string, unknown>): Promise<string> {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  await act(async () => root.render(createElement(ToastProvider, null,
    createElement(ConnectorsPage, { connectors: [], onAdd: noop, onEdit: noop, onDelete: noop, onRefresh: noop, ...props } as never))));
  const html = document.body.innerHTML;
  await act(async () => root.unmount());
  document.body.innerHTML = "";
  return html;
}

after(() => dom.window.close());

describe("ConnectorsPage list states", () => {
  it("shows loading, not the empty state, while a read may still answer", async () => {
    const markup = await render({ connectorsLoading: true });
    assert.match(markup, /Loading connections/);
    assert.doesNotMatch(markup, /No connections yet/);
    assert.doesNotMatch(markup, /Connections \(0\)/);
  });

  it("shows the failure with Try again, never the empty state", async () => {
    const markup = await render({ connectorsError: "The request for your connections timed out" });
    assert.match(markup, /Couldn't load your connections/);
    assert.match(markup, /The request for your connections timed out\. Reading the list doesn't change your connections/);
    assert.match(markup, /Try again/);
    assert.match(markup, /role="alert"/);
    assert.doesNotMatch(markup, /No connections yet/);
    assert.doesNotMatch(markup, /Connections \(0\)/);
  });

  it("shows the empty state after a successful empty answer", async () => {
    const markup = await render({});
    assert.match(markup, /No connections yet/);
    assert.match(markup, /Connections \(0\)/);
  });
});
