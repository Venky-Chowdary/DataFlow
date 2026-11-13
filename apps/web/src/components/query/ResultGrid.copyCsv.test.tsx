/**
 * Query results "Copy CSV" follows the Query console's "Protect against
 * spreadsheet formulas" toggle (G7-F2 review L): it used to prefix always.
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
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
const copied: string[] = [];
Object.defineProperty(dom.window.navigator, "clipboard", {
  value: { writeText: async (text: string) => { copied.push(text); } },
  configurable: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { ResultGrid } = await import("./ResultGrid");

let root: ReturnType<typeof createRoot> | undefined;

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
  copied.length = 0;
});

after(() => dom.window.close());

async function copyCsv(props: Record<string, unknown>): Promise<{ text: string; title: string }> {
  const host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(createElement(ResultGrid, {
    columns: ["a", "n"], rows: [{ a: "=1+1", n: "-2" }], ...props,
  } as never)));
  const button = [...document.querySelectorAll("button")].find((b) => b.textContent === "Copy CSV")!;
  await act(async () => button.click());
  return { text: copied[0], title: button.title };
}

describe("ResultGrid Copy CSV", () => {
  it("defuses formula cells when protection is on (the default)", async () => {
    const { text, title } = await copyCsv({});
    assert.equal(text, "a,n\n'=1+1,-2");
    assert.match(title, /Protect against spreadsheet formulas is on/);
  });

  it("copies exact values when the console toggle is off", async () => {
    const { text, title } = await copyCsv({ formulaProtection: false });
    assert.equal(text, "a,n\n=1+1,-2");
    assert.match(title, /exact values/);
  });
});
