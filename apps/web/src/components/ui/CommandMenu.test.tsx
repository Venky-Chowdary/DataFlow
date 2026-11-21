/**
 * Command menu (cmdk) and the "?" sheet in jsdom (UI pass 2, item 7).
 * Run: npx tsx --test apps/web/src/components/ui/CommandMenu.test.tsx
 */
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/", pretendToBeVisual: true });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  NodeFilter: dom.window.NodeFilter,
  MouseEvent: dom.window.MouseEvent,
  KeyboardEvent: dom.window.KeyboardEvent,
  CustomEvent: dom.window.CustomEvent,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  MutationObserver: dom.window.MutationObserver,
  requestAnimationFrame: (cb: FrameRequestCallback) => setTimeout(() => cb(0), 0),
  cancelAnimationFrame: (id: number) => clearTimeout(id),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
class RO { observe() {} unobserve() {} disconnect() {} }
Object.assign(globalThis, { ResizeObserver: RO });
(dom.window as unknown as { ResizeObserver: unknown }).ResizeObserver = RO;
(dom.window.HTMLElement.prototype as unknown as { scrollIntoView: () => void }).scrollIntoView = () => {};

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { CommandMenu } = await import("./CommandMenu.js");
const { setSelectionActions, SHORTCUTS, isTypingTarget } = await import("../../lib/commandRegistry.js");

after(() => dom.window.close());

describe("CommandMenu", () => {
  it("lists the selection's actions first, then pages and records, and runs the chosen one", async () => {
    const ran: string[] = [];
    const clear = setSelectionActions({
      title: "Selected job · orders nightly",
      actions: [
        { id: "trust", label: "Open Trust Report", run: () => ran.push("trust") },
        { id: "cancel", label: "Cancel job", tone: "danger", run: () => ran.push("cancel") },
      ],
    });
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    let open = true;
    const navigated: unknown[] = [];
    const render = async () => act(async () => root.render(createElement(CommandMenu, {
      open,
      onOpenChange: (next: boolean) => { open = next; },
      navItems: [{ id: "jobs", label: "Jobs", icon: "jobs" }],
      jobs: [{ _id: "job-7f3a9c21", name: "orders nightly", status: "completed" } as never],
      connectors: [],
      schedules: [],
      onNavigate: (t: unknown) => navigated.push(t),
      onShowShortcuts: () => ran.push("shortcuts"),
    })));
    await render();
    const headings = [...document.querySelectorAll("[cmdk-group-heading]")].map((h) => h.textContent);
    assert.deepEqual(headings, ["Selected job · orders nightly", "Go to", "Jobs", "Help"]);
    const items = [...document.querySelectorAll("[cmdk-item]")].map((i) => i.textContent);
    assert.deepEqual(items.slice(0, 2), ["Open Trust Report", "Cancel job"]);
    assert.equal(document.querySelector('[cmdk-item][data-tone="danger"]')?.textContent, "Cancel job");
    assert.ok(document.querySelector('[role="dialog"]'), "a modal dialog");
    const item = document.querySelector<HTMLElement>("[cmdk-item]")!;
    await act(async () => { item.click(); await new Promise((r) => setTimeout(r, 5)); });
    assert.deepEqual(ran, ["trust"]);
    assert.equal(open, false, "running an action closes the menu");
    clear();
    open = true;
    await render();
    assert.deepEqual([...document.querySelectorAll("[cmdk-group-heading]")].map((h) => h.textContent)[0], "Go to", "no selection: no selection section");
    await act(async () => root.unmount());
  });
});

describe("shortcuts", () => {
  it("documents the menu, the sheet and the list keys, and ignores typing", () => {
    assert.ok(SHORTCUTS.some((s) => s.keys.join("+") === "⌘+K"));
    assert.ok(SHORTCUTS.some((s) => s.keys[0] === "?"));
    assert.ok(SHORTCUTS.some((s) => s.where === "Validate" && s.keys.includes("J")));
    const input = document.createElement("input");
    assert.equal(isTypingTarget(input), true);
    assert.equal(isTypingTarget(document.createElement("button")), false);
  });

  it("a stale clear never removes a newer page's actions", () => {
    const first = setSelectionActions({ title: "a", actions: [{ id: "x", label: "X", run: () => {} }] });
    setSelectionActions({ title: "b", actions: [{ id: "y", label: "Y", run: () => {} }] });
    first();
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return import("../../lib/commandRegistry.js").then(({ getSelectionActions }) => assert.equal(getSelectionActions()?.title, "b"));
  });
});

describe("shell keys (review UI2 #6, #11)", () => {
  const key = (k: string, extra: Partial<{ metaKey: boolean; ctrlKey: boolean; altKey: boolean; defaultPrevented: boolean; target: EventTarget | null }> = {}) =>
    ({ key: k, metaKey: false, ctrlKey: false, altKey: false, defaultPrevented: false, target: document.body, ...extra });

  it("Ctrl+K and Cmd+K open the menu; ? opens the sheet, even from a drawer", async () => {
    const { shellShortcut } = await import("../../lib/commandRegistry.js");
    assert.equal(shellShortcut(key("k", { ctrlKey: true })), "menu");
    assert.equal(shellShortcut(key("K", { metaKey: true })), "menu");
    const drawer = document.createElement("div");
    drawer.setAttribute("role", "dialog");
    const button = document.createElement("button");
    drawer.appendChild(button);
    document.body.appendChild(drawer);
    assert.equal(shellShortcut(key("?", { target: button })), "shortcuts");
    drawer.remove();
  });

  it("leaves typing, already-handled keys and the canvas palette's Ctrl+K alone", async () => {
    const { shellShortcut } = await import("../../lib/commandRegistry.js");
    const input = document.createElement("input");
    assert.equal(shellShortcut(key("?", { target: input })), null);
    assert.equal(shellShortcut(key("k", { ctrlKey: true, defaultPrevented: true })), null);
    const canvas = document.createElement("div");
    canvas.setAttribute("data-owns-command-key", "");
    document.body.appendChild(canvas);
    assert.equal(shellShortcut(key("k", { ctrlKey: true })), null, "the visible canvas owns Ctrl+K");
    const kept = document.createElement("div");
    kept.hidden = true;
    kept.appendChild(canvas);
    document.body.appendChild(kept);
    assert.equal(shellShortcut(key("k", { ctrlKey: true })), "menu", "a canvas kept mounted behind another screen does not");
    kept.remove();
  });
});

describe("Escape closes only the top overlay (review UI2 #6)", () => {
  it("a Dialog underneath ignores an Escape the menu handled", async () => {
    const { Dialog } = await import("./Dialog.js");
    const { pushOverlay } = await import("../../lib/overlayStack.js");
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    let closed = 0;
    await act(async () => root.render(createElement(Dialog, { open: true, onClose: () => { closed += 1; }, title: "Add a computed column" }, createElement("input"))));
    const menu = document.createElement("div");
    document.body.appendChild(menu);
    const pop = pushOverlay(menu);
    await act(async () => { document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true })); });
    assert.equal(closed, 0, "the menu is on top");
    pop();
    const handled = new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true });
    handled.preventDefault();
    await act(async () => { document.dispatchEvent(handled); });
    assert.equal(closed, 0, "an Escape another layer took is not reused");
    await act(async () => { document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true })); });
    assert.equal(closed, 1);
    await act(async () => root.unmount());
  });
});

describe("selection gating (review UI2 #3)", () => {
  it("offers job actions only for an explicit pick on the visible Jobs screen with its detail open", async () => {
    const { selectionCommandsEnabled } = await import("../../lib/jobSelection.js");
    assert.equal(selectionCommandsEnabled({ active: true, explicitPick: true, panelOpen: true }), true);
    assert.equal(selectionCommandsEnabled({ active: true, explicitPick: false, panelOpen: true }), false, "auto-selected first row");
    assert.equal(selectionCommandsEnabled({ active: false, explicitPick: true, panelOpen: true }), false, "Jobs kept mounted behind another screen");
    assert.equal(selectionCommandsEnabled({ active: true, explicitPick: true, panelOpen: false }), false, "drawer closed");
  });
});

describe("the menu joins the overlay stack while open (review UI2 #6)", () => {
  it("is the top overlay, so a Dialog underneath keeps its typed input on Escape", async () => {
    const { isTopOverlay } = await import("../../lib/overlayStack.js");
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    await act(async () => root.render(createElement(CommandMenu, {
      open: true, onOpenChange: () => {}, navItems: [], jobs: [], connectors: [], schedules: [], onNavigate: () => {}, onShowShortcuts: () => {},
    })));
    await act(async () => { await new Promise((r) => setTimeout(r, 10)); });
    assert.equal(isTopOverlay(document.querySelector<HTMLElement>(".df2-cmdk-content")), true);
    await act(async () => root.unmount());
  });
});
