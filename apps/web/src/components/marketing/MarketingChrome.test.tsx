/**
 * Marketing header and footer: one wording for the two actions, menus grouped
 * by what each product part does, and keyboard behaviour of the menus
 * (Escape returns focus to the trigger; focus leaving a menu closes it).
 */
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { createRequire } from "node:module";
import type { Root } from "react-dom/client";

const { JSDOM } = createRequire(import.meta.url)("jsdom") as {
  JSDOM: new (html: string, options: { url: string; pretendToBeVisual: boolean }) => {
    window: Window & typeof globalThis;
  };
};
const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/", pretendToBeVisual: true });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Node: dom.window.Node,
  NodeFilter: dom.window.NodeFilter,
  Event: dom.window.Event,
  CustomEvent: dom.window.CustomEvent,
  KeyboardEvent: dom.window.KeyboardEvent,
  FocusEvent: dom.window.FocusEvent,
  MutationObserver: dom.window.MutationObserver,
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback: FrameRequestCallback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle: number) => dom.window.clearTimeout(handle);
dom.window.scrollTo = () => undefined;
Object.assign(globalThis, {
  requestAnimationFrame: dom.window.requestAnimationFrame,
  cancelAnimationFrame: dom.window.cancelAnimationFrame,
});

let root: Root | null = null;

async function mount() {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { MarketingChrome } = await import("./MarketingChrome");
  const host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  const noop = () => undefined;
  await act(async () =>
    root!.render(
      createElement(
        MarketingChrome,
        { route: "pricing", onNavigate: noop, onLogin: noop, onGetStarted: noop },
        createElement("p", null, "page"),
      ),
    ),
  );
  return host;
}

async function settle() {
  const { act } = await import("react");
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

function trigger(host: HTMLElement, label: string): HTMLButtonElement {
  const found = [...host.querySelectorAll<HTMLButtonElement>(".mk-nav button.mk-nav-link")].find((b) =>
    (b.textContent || "").includes(label),
  );
  assert.ok(found, `trigger ${label}`);
  return found;
}

async function openWithKeyboard(button: HTMLButtonElement) {
  const { act } = await import("react");
  button.focus();
  await act(async () => {
    button.click();
  });
  await settle();
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  root = null;
  document.body.replaceChildren();
});

describe("marketing chrome", () => {
  it("words the sales and primary actions the same in the header, the mobile sheet and the footer", async () => {
    const { PRIMARY_CTA_LABEL, SALES_CTA_LABEL } = await import("./MarketingChrome");
    const host = await mount();
    const texts = (sel: string) => [...host.querySelectorAll(sel)].map((el) => (el.textContent || "").trim());
    for (const area of [".mk-header-actions", ".mk-mobile-actions", ".mk-footer-base-links"]) {
      const labels = texts(`${area} a, ${area} button`);
      assert.ok(labels.includes(SALES_CTA_LABEL), `${area} has ${SALES_CTA_LABEL}: ${labels.join(", ")}`);
      assert.ok(labels.includes(PRIMARY_CTA_LABEL), `${area} has ${PRIMARY_CTA_LABEL}: ${labels.join(", ")}`);
    }
    const all = (host.textContent || "").toLowerCase();
    for (const stale of ["contact sales", "talk to an engineer", "create a workspace"]) {
      assert.ok(!all.includes(stale), `no "${stale}" in the chrome`);
    }
  });

  it("groups the Product menu by what each part does", async () => {
    const { PRODUCT_GROUPS, PRODUCT_LINKS } = await import("./MarketingChrome");
    assert.deepEqual(
      PRODUCT_GROUPS.map((g) => g.title),
      ["Move", "Prove", "Ask"],
    );
    const ask = PRODUCT_GROUPS.find((g) => g.title === "Ask")!.routes;
    const prove = PRODUCT_GROUPS.find((g) => g.title === "Prove")!.routes;
    assert.ok(ask.includes("product-query") && ask.includes("product-pilot"), "Query and Pilot answer questions");
    assert.ok(!prove.includes("product-query") && !prove.includes("product-pilot"), "Query and Pilot are not proof");
    const grouped = PRODUCT_GROUPS.flatMap((g) => g.routes).sort();
    assert.deepEqual(grouped, PRODUCT_LINKS.map((l) => l.route).sort(), "every product page is in exactly one group");
  });

  it("returns focus to the trigger when Escape closes a menu", async () => {
    const { act } = await import("react");
    const host = await mount();
    const product = trigger(host, "Product");
    await openWithKeyboard(product);
    assert.equal(product.getAttribute("aria-expanded"), "true");
    const content = host.querySelector<HTMLElement>(".mk-nav-menu--product");
    assert.ok(content, "menu content rendered");
    assert.equal(product.getAttribute("aria-controls"), content.id);
    const firstLink = content.querySelector<HTMLAnchorElement>("a");
    assert.ok(firstLink);
    firstLink.focus();
    await act(async () => {
      document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    });
    await settle();
    assert.equal(host.querySelector(".mk-nav-menu--product"), null, "menu closed");
    assert.equal(document.activeElement, product, "focus back on the trigger");
  });

  it("closes a menu when focus moves to the next item in the bar", async () => {
    const { act } = await import("react");
    const host = await mount();
    const solutions = trigger(host, "Solutions");
    await openWithKeyboard(solutions);
    assert.ok(host.querySelector(".mk-nav-menu--solutions"), "solutions open");
    const next = [...host.querySelectorAll<HTMLAnchorElement>(".mk-nav a.mk-nav-link")].find(
      (a) => a.textContent === "Connectors",
    );
    assert.ok(next);
    await act(async () => {
      next.focus();
    });
    await settle();
    assert.equal(host.querySelector(".mk-nav-menu--solutions"), null, "menu closed once focus left it");
  });

  it("closes the mobile sheet on Escape and returns focus to the toggle", async () => {
    const { act } = await import("react");
    const host = await mount();
    const toggle = host.querySelector<HTMLButtonElement>(".mk-menu-toggle")!;
    const sheet = host.querySelector<HTMLElement>("#mk-mobile-menu")!;
    assert.equal(toggle.getAttribute("aria-controls"), "mk-mobile-menu");
    await act(async () => toggle.click());
    assert.equal(sheet.hidden, false);
    assert.equal(toggle.getAttribute("aria-expanded"), "true");
    sheet.querySelector<HTMLAnchorElement>("a")!.focus();
    await act(async () => {
      document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    });
    assert.equal(sheet.hidden, true);
    assert.equal(document.activeElement, toggle);
  });
});
