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

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { PlanConflictNote } = await import("./PlanConflictNote.js");

let root: ReturnType<typeof createRoot> | undefined;
async function render(node: ReturnType<typeof createElement>) {
  const host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(node));
}
afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
});
after(() => dom.window.close());

describe("Validate panel during a plan conflict", () => {
  it("says why there is no verdict and re-opens the comparison", async () => {
    let reopened = 0;
    await render(createElement(PlanConflictNote, {
      note: "Validate did not run: this plan changed since you opened it.",
      onResolve: () => { reopened += 1; },
    }));
    const alert = document.querySelector('[role="alert"]');
    assert.match(alert?.textContent ?? "", /Validate did not run/);
    await act(async () => (document.querySelector("button") as HTMLButtonElement).click());
    assert.equal(reopened, 1);
  });

  it("renders nothing without a conflict", async () => {
    await render(createElement(PlanConflictNote, { note: "" }));
    assert.equal(document.querySelector('[role="alert"]'), null);
  });
});
