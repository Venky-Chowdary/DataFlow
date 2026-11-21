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
