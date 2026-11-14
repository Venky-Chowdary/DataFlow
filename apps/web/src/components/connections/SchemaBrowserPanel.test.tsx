/**
 * UX-9: schema browsing finds any table in a large schema.
 * - The workbench lists the whole schema through a server-side search, with a
 *   schema selector, an honest "N of M" and columns loaded on demand.
 * - Studio's table picker searches the catalog, so a table past the first
 *   page, or in another schema ("w4ux.simple"), is found instead of
 *   "No tables match".
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  localStorage: dom.window.localStorage,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  Event: dom.window.Event,
  MouseEvent: dom.window.MouseEvent,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });

const calls: Array<Record<string, unknown>> = [];
const page = Array.from({ length: 200 }, (_, i) => `t${String(i).padStart(3, "0")}`);
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = String(input);
  const body = JSON.parse(String(init?.body || "{}"));
  calls.push({ url: url.replace(/^.*\/api\/v1/, ""), ...body });
  const json = (data: unknown) => new Response(JSON.stringify(data), { status: 200, headers: { "Content-Type": "application/json" } });
  if (url.endsWith("/query/schemas")) {
    return json({ connected: true, schema_name: "public", listed_live: true, nodes: [{ kind: "database", name: "db", children: [{ kind: "schema", name: "public" }, { kind: "schema", name: "sales" }] }] });
  }
  if (body.object_name) {
    return json({ connected: true, objects: [{ name: body.object_name, type: "table", columns: [{ name: "id", type: "INT4", primary_key: true }, { name: "note", type: "TEXT" }] }] });
  }
  if (body.search) {
    const names = body.schema_name === "sales" ? ["orders_eu"] : ["orders_2025"];
    return json({ connected: true, objects: names.filter((n) => n.includes(String(body.search).toLowerCase())).map((name) => ({ name, type: "table", columns: [] })), objects_total: 1, objects_truncated: false });
  }
  return json({ connected: true, objects: page.map((name) => ({ name, type: "table", columns: [] })), objects_total: 9497, objects_truncated: true });
}) as typeof fetch;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { SchemaBrowserPanel } = await import("./SchemaBrowserPanel.js");
const { ObjectNameCombobox } = await import("../transfer/ObjectNameCombobox.js");
const { listingCountLabel, splitQualifiedName, searchCatalogNames } = await import("../../lib/schemaBrowse.js");

let root: ReturnType<typeof createRoot> | undefined;
async function render(node: ReturnType<typeof createElement>) {
  if (!root) {
    const host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(node));
}
const settle = (ms = 0) => act(async () => new Promise((r) => setTimeout(r, ms)));
const setValue = async (el: HTMLInputElement | HTMLSelectElement, value: string) => {
  const proto = el instanceof dom.window.HTMLSelectElement ? dom.window.HTMLSelectElement.prototype : dom.window.HTMLInputElement.prototype;
  await act(async () => {
    Object.getOwnPropertyDescriptor(proto, "value")!.set!.call(el, value);
    el.dispatchEvent(new dom.window.Event(el instanceof dom.window.HTMLSelectElement ? "change" : "input", { bubbles: true }));
  });
};

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  calls.length = 0;
});
after(() => {
  globalThis.fetch = realFetch;
  dom.window.close();
});

describe("schema browse helpers", () => {
  it("says how much of the schema is on screen", () => {
    assert.equal(listingCountLabel(200, 9497), "Showing 200 of 9,497 tables");
    assert.equal(listingCountLabel(2, 2), "2 tables");
    assert.equal(listingCountLabel(1, null, "collection"), "1 collection");
  });

  it("splits a schema-qualified name", () => {
    assert.deepEqual(splitQualifiedName("w4ux.simple"), { schema: "w4ux", name: "simple" });
    assert.deepEqual(splitQualifiedName("orders"), { schema: "", name: "orders" });
  });

  it("searches another schema and qualifies what it finds", async () => {
    const res = await searchCatalogNames("c1", "sales.orders", "public");
    assert.deepEqual(res.names, ["sales.orders_eu"]);
    assert.deepEqual(calls.at(-1), { url: "/query/schema", connector_id: "c1", schema_name: "sales", search: "orders" });
  });
});

describe("workbench schema browser", () => {
  it("shows N of M, searches the server, switches schema and loads columns on demand", async () => {
    await render(createElement(SchemaBrowserPanel, { connector: { id: "c1", name: "Orders PG", type: "postgresql" } }));
    await settle(10);
    const count = () => document.querySelector('[data-testid="schema-browser-count"]')!.textContent;
    assert.match(count()!, /Showing 200 of 9,497 tables in public\. Search to narrow the list\./);
    assert.equal(document.querySelectorAll("tbody tr").length, 200, "the whole page, not 40");
    assert.ok(calls.every((c) => !("object_name" in c)), "no columns are fetched up front");

    await setValue(document.querySelector('input[type="search"]') as HTMLInputElement, "orders");
    await settle(350);
    assert.deepEqual(calls.at(-1), { url: "/query/schema", connector_id: "c1", schema_name: "public", search: "orders" });
    assert.match(count()!, /1 table matching “orders” in public\./);

    await setValue(document.querySelector("select") as HTMLSelectElement, "sales");
    await settle(10);
    assert.equal(calls.at(-1)!.schema_name, "sales");
    assert.match(document.querySelector("tbody")!.textContent!, /orders_eu/);

    const show = [...document.querySelectorAll("tbody button")].find((b) => b.textContent?.includes("Show columns")) as HTMLButtonElement;
    await act(async () => show.click());
    await settle(10);
    assert.deepEqual(calls.at(-1), { url: "/query/schema", connector_id: "c1", schema_name: "sales", object_name: "orders_eu" });
    assert.equal(show.getAttribute("aria-expanded"), "true");
    assert.match(document.querySelector('ul[aria-label="Columns of orders_eu"]')!.textContent!, /id INT4key.*note TEXT/);
  });
});

describe("Studio table picker", () => {
  it("finds a table in another schema instead of saying No tables match", async () => {
    let value = "";
    const search = (q: string) => searchCatalogNames("c1", q, "public").then((r) => r.names);
    const view = () =>
      createElement(ObjectNameCombobox, {
        id: "src",
        label: "Table(s)",
        value,
        onChange: (next: string) => {
          value = next;
        },
        options: page,
        allowCreate: false,
        searchCatalog: search,
      });
    await render(view());
    const input = document.getElementById("src") as HTMLInputElement;
    await act(async () => {
      input.focus();
      input.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
    });
    value = "sales.orders";
    await render(view());
    const menuText = () => document.querySelector('[role="listbox"]')?.textContent ?? "";
    assert.match(menuText(), /Searching every table for “sales\.orders”/);
    await settle(300);
    assert.doesNotMatch(menuText(), /No tables match/);
    assert.match(menuText(), /sales\.orders_eu/);
  });

  it("Studio's footer hint follows the Continue button", () => {
    const studio = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "pages", "TransferPage.tsx"), "utf8");
    assert.match(studio, /searchCatalog=\{sourceCatalogSearch\}/);
    assert.match(studio, /disabled\s*\n?\s*\? "Select connector and table\/collection to continue"\s*\n?\s*: "Source selected — continue to pick a destination\."/);
  });
});
