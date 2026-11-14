/**
 * A connection that was never tested must look neutral (grey), never healthy (green),
 * and the Connectors "Healthy (N)" filter must not count it.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => {
  callback(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};

const { act, createElement } = await import("react");
const { renderToStaticMarkup } = await import("react-dom/server");
const { createRoot } = await import("react-dom/client");
const { ConnectorCard } = await import("./ConnectorCard.js");
const { ConnectorDetailDrawer } = await import("../ConnectorDetailDrawer.js");

const src = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");
const neverTested = { id: "c1", name: "Orders", type: "postgresql", database: "sales", host: "127.0.0.1", port: 5432 } as never;
const passed = { ...(neverTested as object), id: "c2", last_test_ok: true, last_tested_at: "2026-11-01T00:00:00Z" } as never;
const noop = () => {};

function card(connector: never, compact: boolean) {
  return renderToStaticMarkup(createElement(ConnectorCard, { connector, compact, online: true, onTest: noop } as never));
}

async function drawerHtml(connector: never) {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  await act(async () => root.render(createElement(ConnectorDetailDrawer, {
    open: true, connector, workbench: null, connectors: [connector], connectionTab: "Status",
    setConnectionTab: noop, onClose: noop, onTest: noop, onEdit: noop, onDelete: noop, onSelectConnection: noop,
  } as never)));
  const html = document.body.innerHTML;
  await act(async () => root.unmount());
  document.body.innerHTML = "";
  return html;
}

after(() => dom.window.close());

describe("never-tested connections look neutral", () => {
  for (const compact of [true, false]) {
    it(`the ${compact ? "compact row" : "card"} health dot is grey, not green`, () => {
      const html = card(neverTested, compact);
      assert.doesNotMatch(html, /df2-health-dot ok/);
      assert.match(html, /title="Never tested"/);
      assert.match(card(passed, compact), /df2-health-dot ok/);
    });
  }

  it("the drawer badge and Last test fact are grey, not green", async () => {
    const html = await drawerHtml(neverTested);
    assert.match(html, /Never tested/);
    assert.doesNotMatch(html, /df2-badge-live|df2-health-dot ok|df2-fact-ok/);
    assert.match(html, /df2-badge-muted/);
    assert.match(html, /df2-connector-health-badge/);
    const ok = await drawerHtml(passed);
    assert.match(ok, /df2-badge-live/);
  });

  it("the health badge keeps sentence case ('Never tested', not 'Never Tested')", () => {
    const css = read("styles/connectors-page.css");
    assert.match(css, /\.df2-connector-health-badge \{\s*text-transform: none;/);
  });

  it("the Healthy filter and its count use a passed probe, not 'looks healthy'", () => {
    const page = read("pages/ConnectorsPage.tsx");
    assert.match(page, /const healthyCount = connectors\.filter\(\(c\) => connectorPassedProbe\(c\)\)\.length;/);
    assert.match(page, /: connectorPassedProbe\(c\)\);/);
    assert.doesNotMatch(page, /connectorLooksHealthy/);
  });
});
