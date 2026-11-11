/**
 * Drawer: Escape closes only the topmost modal, a parent re-render never
 * pulls focus back to the panel, and a drawer can mount in a given container.
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
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => {
  callback(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};

const { act, createElement, Fragment } = await import("react");
const { createRoot } = await import("react-dom/client");
const { Drawer } = await import("./Drawer.js");

let root: ReturnType<typeof createRoot> | undefined;

async function render(node: ReturnType<typeof createElement>) {
  if (!root) {
    const host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(node));
}

async function escape() {
  await act(async () => {
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
  });
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
});

after(() => dom.window.close());

describe("Drawer", () => {
  it("closes only the topmost drawer on Escape", async () => {
    const closed: string[] = [];
    await render(createElement(Fragment, null,
      createElement(Drawer, { open: true, onClose: () => closed.push("detail"), ariaLabel: "Detail" }, "detail"),
      createElement(Drawer, { open: true, onClose: () => closed.push("evidence"), ariaLabel: "Evidence" }, "evidence"),
    ));
    await escape();
    assert.deepEqual(closed, ["evidence"], "the drawer underneath stays open");
  });

  it("keeps focus where the operator put it when the parent re-renders", async () => {
    const view = (tick: number) => createElement(Drawer, { open: true, onClose: () => tick, ariaLabel: "Detail" },
      createElement("button", { type: "button", id: "inside" }, `tick ${tick}`));
    await render(view(1));
    const inside = document.getElementById("inside") as HTMLButtonElement;
    inside.focus();
    await render(view(2));
    assert.equal(document.activeElement, inside, "a new onClose identity must not refocus the panel");
  });

  it("mounts in the given container", async () => {
    const shell = document.createElement("div");
    shell.className = "df2-app";
    document.body.appendChild(shell);
    await render(createElement(Drawer, { open: true, onClose: () => {}, ariaLabel: "Detail", container: shell }, "x"));
    assert.ok(shell.querySelector('[role="dialog"][aria-label="Detail"]'));
  });
});
