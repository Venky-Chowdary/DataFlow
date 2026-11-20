/**
 * D410 beyond the Overview: every page that lists connectors gets the shell's
 * list state. A failed read with nothing loaded is "couldn't load" with Try
 * again, never that page's own "no connectors" empty state; a successful
 * empty answer still shows it.
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
dom.window.matchMedia = (() => ({ matches: false, addEventListener() {}, removeEventListener() {} })) as never;
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
Object.defineProperty(globalThis, "localStorage", { value: dom.window.localStorage, configurable: true });
globalThis.fetch = (async () => {
  throw new Error("offline in test");
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ToastProvider } = await import("./Toast.js");
const { QueryPage } = await import("../pages/QueryPage.js");
const { DestinationPicker } = await import("./transfer/DestinationPicker.js");

const FAILED = { connectorsLoading: false, connectorsError: "The request for your connections timed out" };
const PENDING = { connectorsLoading: true, connectorsError: "" };
const LOADED = { connectorsLoading: false, connectorsError: "" };

async function render(element: unknown): Promise<string> {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  await act(async () => root.render(createElement(ToastProvider, null, element as never)));
  const html = document.body.innerHTML;
  await act(async () => root.unmount());
  document.body.innerHTML = "";
  return html;
}

const picker = (state: object) =>
  createElement(DestinationPicker, {
    connectors: [],
    connectorId: "",
    destType: "",
    liveDestTypes: [{ id: "postgresql", label: "PostgreSQL" }],
    onSelectConnector: () => {},
    onSelectManual: () => {},
    onSelectType: () => {},
    ...state,
  });

after(() => dom.window.close());

describe("connector list state on the pages that list connectors (D410)", () => {
  it("Query says the list could not be read, not 'Add a connector', after a failed read", async () => {
    const html = await render(createElement(QueryPage, { connectors: [], ...FAILED }));
    assert.match(html, /Couldn't load your connections/);
    assert.match(html, /timed out\. Reading the list doesn't change your connections/);
    assert.doesNotMatch(html, /Add a connector to run queries/);
  });

  it("Query keeps its empty state after a successful empty answer", async () => {
    const html = await render(createElement(QueryPage, { connectors: [], ...LOADED }));
    assert.match(html, /Add a connector to run queries/);
    assert.doesNotMatch(html, /Couldn't load your connections/);
  });

  it("the destination picker shows loading or the failure, never 'No saved connectors yet'", async () => {
    const pending = await render(picker(PENDING));
    assert.match(pending, /Loading connections/);
    assert.doesNotMatch(pending, /No saved connectors yet/);
    const failed = await render(picker(FAILED));
    assert.match(failed, /Couldn't load your connections/);
    assert.match(failed, /Try again/);
    assert.doesNotMatch(failed, /No saved connectors yet/);
  });

  it("the destination picker offers New connection when the list really is empty", async () => {
    const html = await render(picker(LOADED));
    assert.doesNotMatch(html, /Couldn't load your connections/);
    assert.doesNotMatch(html, /Loading connections/);
  });
});
