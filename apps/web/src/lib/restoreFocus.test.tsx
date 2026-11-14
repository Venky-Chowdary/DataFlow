/**
 * UX-15 (WCAG 2.4.3): closing a dialog or drawer gives focus back to the
 * control that opened it, instead of dropping it on <body>.
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
  KeyboardEvent: dom.window.KeyboardEvent,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (cb) => {
  cb(0);
  return 0;
};
dom.window.cancelAnimationFrame = () => {};

const { act, createElement, useRef, useState } = await import("react");
const { createRoot } = await import("react-dom/client");
const { Drawer } = await import("../components/ui/Drawer.js");
const { Dialog } = await import("../components/ui/Dialog.js");
const { useOverlayFocus, captureFocusForRestore } = await import("./restoreFocus.js");

let root: ReturnType<typeof createRoot> | undefined;
async function render(node: ReturnType<typeof createElement>) {
  if (!root) {
    const host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(node));
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
});
after(() => dom.window.close());

function Harness({ kind }: { kind: "drawer" | "dialog" | "hand-rolled" }) {
  const [open, setOpen] = useState(false);
  const panel = useRef<HTMLDivElement>(null);
  useOverlayFocus(kind === "hand-rolled" && open, panel, () => setOpen(false));
  const close = () => setOpen(false);
  return createElement(
    "div",
    null,
    createElement("button", { id: "trigger", type: "button", onClick: () => setOpen(true) }, "Open"),
    kind === "drawer" && createElement(Drawer, { open, onClose: close, title: "Details", ariaLabel: "Details" }, createElement("p", null, "body")),
    kind === "dialog" && createElement(Dialog, { open, onClose: close, title: "Confirm", size: "md" }, createElement("p", null, "body")),
    kind === "hand-rolled" && open && createElement(
      "div",
      { ref: panel, role: "dialog", tabIndex: -1, "aria-label": "Editor" },
      createElement("input", { id: "inner-search", autoFocus: true, "aria-label": "Search" }),
      createElement("button", { id: "inner-close", type: "button", onClick: close }, "Close"),
    ),
  );
}

async function openThenEscape(kind: "drawer" | "dialog" | "hand-rolled") {
  await render(createElement(Harness, { kind }));
  const trigger = document.getElementById("trigger") as HTMLButtonElement;
  trigger.focus();
  await act(async () => trigger.click());
  const dialog = document.querySelector('[role="dialog"]');
  assert.ok(dialog, `${kind} opened`);
  assert.ok(dialog!.contains(document.activeElement), `${kind} takes focus when it opens`);
  await act(async () => {
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
  });
  assert.equal(document.querySelector('[role="dialog"]'), null, `${kind} closed on Escape`);
  return trigger;
}

describe("focus returns to the trigger", () => {
  it("when a drawer closes", async () => {
    const trigger = await openThenEscape("drawer");
    assert.equal(document.activeElement, trigger);
  });

  it("when a dialog closes", async () => {
    const trigger = await openThenEscape("dialog");
    assert.equal(document.activeElement, trigger);
  });

  it("when a hand-rolled overlay closes even though its search box autofocused first", async () => {
    const trigger = await openThenEscape("hand-rolled");
    assert.equal(document.activeElement, trigger);
  });

  it("falls back to the main landmark when the trigger left the page", () => {
    const main = document.createElement("main");
    main.tabIndex = -1;
    const trigger = document.createElement("button");
    document.body.append(main, trigger);
    trigger.focus();
    const restore = captureFocusForRestore();
    trigger.remove();
    restore();
    assert.equal(document.activeElement, main);
  });

  it("does not steal focus the user already moved elsewhere", () => {
    const trigger = document.createElement("button");
    const other = document.createElement("input");
    document.body.append(trigger, other);
    trigger.focus();
    const restore = captureFocusForRestore();
    other.focus();
    restore();
    assert.equal(document.activeElement, other);
  });
});
