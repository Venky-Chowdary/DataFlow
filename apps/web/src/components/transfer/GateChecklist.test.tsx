/**
 * GateChecklist in jsdom: ranked groups, passed collapsed, j/k and Enter, the
 * Re-run action on a check that could not run, and the Explain popover.
 * Run: npx tsx --test apps/web/src/components/transfer/GateChecklist.test.tsx
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
  MouseEvent: dom.window.MouseEvent,
  KeyboardEvent: dom.window.KeyboardEvent,
  CustomEvent: dom.window.CustomEvent,
  NodeFilter: dom.window.NodeFilter, // Radix focus scope walks the popover with a TreeWalker
  getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
(dom.window as unknown as { ResizeObserver: unknown }).ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
(globalThis as unknown as { ResizeObserver: unknown }).ResizeObserver = (dom.window as unknown as { ResizeObserver: unknown }).ResizeObserver;

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { GateChecklist } = await import("./GateChecklist.js");

after(() => dom.window.close());

const gates = [
  { id: "g1_source", status: "pass", message: "ok" },
  { id: "g2_destination", status: "pass", message: "ok" },
  { id: "g3_schema_contract", status: "block", message: "3 columns narrow", details: { errors: [{ column: "a", message: "Too long" }, { column: "b", message: "Too long" }] } },
  { id: "g13_source_coverage", status: "skip", message: "Source coverage check could not run: the service that runs it did not answer.", details: { unmeasured: true } },
  { id: "g22_dest_referential_integrity", status: "skip", message: "No foreign keys were declared on this route.", details: { declared: false } },
];

async function mount(onRerun?: () => void) {
  const host = document.createElement("div");
  document.body.appendChild(host);
  const root = createRoot(host);
  await act(async () => root.render(createElement(GateChecklist, {
    gates,
    onRerun,
    note: "confidence floor 85%",
  })));
  return host;
}

const press = async (el: Element, key: string) => {
  await act(async () => {
    el.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key, bubbles: true, cancelable: true }));
  });
};

describe("GateChecklist", () => {
  it("ranks the work first, keeps passed and not-applicable collapsed, and counts honestly", async () => {
    const host = await mount();
    const titles = [...host.querySelectorAll(".df2-check-group-title button")].map((b) => [b.textContent, b.getAttribute("aria-expanded")]);
    assert.deepEqual(titles, [["Blocking1", "true"], ["Could not run1", "true"], ["Passed2", "false"], ["Not applicable1", "false"]]);
    assert.match(host.querySelector(".df2-checklist-summary")?.textContent || "", /^1 blocking · 1 could not run · 2 passed · 1 not applicable · confidence floor 85%$/);
    const failed = host.querySelector('[data-gate="g3_schema_contract"]')!;
    const skipped = host.querySelector('[data-gate="g13_source_coverage"]')!;
    assert.equal(failed.querySelector(".df2-check-word")?.textContent, "Failed");
    assert.equal(skipped.querySelector(".df2-check-word")?.textContent, "Could not run");
    assert.equal(failed.querySelector(".df2-check-findings")?.textContent, "Too long: a, b");
    assert.equal(host.querySelector('[data-gate="g1_source"]'), null, "passed rows start collapsed");
    const passedToggle = [...host.querySelectorAll(".df2-check-group-title button")][2] as HTMLButtonElement;
    await act(async () => passedToggle.click());
    assert.ok(host.querySelector('[data-gate="g1_source"]'));
  });

  it("j / k move between checks; Enter re-runs a check that could not run", async () => {
    let reruns = 0;
    const host = await mount(() => { reruns += 1; });
    const rows = [...host.querySelectorAll<HTMLElement>("[data-check-row]")];
    assert.deepEqual(rows.map((r) => r.tabIndex), [0, -1], "roving tab stop");
    rows[0].focus();
    await press(rows[0], "j");
    assert.equal(document.activeElement, rows[1]);
    await press(rows[1], "Enter");
    assert.equal(reruns, 1);
    await press(rows[1], "k");
    assert.equal(document.activeElement, rows[0]);
  });

  it("Explain opens the catalog's words for that check", async () => {
    const host = await mount();
    const trigger = host.querySelector<HTMLButtonElement>('[data-gate="g3_schema_contract"] .df2-explain-trigger')!;
    assert.equal(trigger.getAttribute("aria-label"), "Explain Schema contract");
    await act(async () => {
      trigger.click();
    });
    const pop = document.querySelector(".df2-explain");
    assert.ok(pop, "popover opened");
    assert.match(pop!.textContent || "", /This check failed, so Run stays locked until it passes/);
    assert.match(pop!.textContent || "", /not by AI/);
  });
});

describe("GateChecklist accessibility (review UI2 #11)", () => {
  it("Show all is a real, focusable button and the tab stop survives a collapse", async () => {
    const many = [{ id: "g3_schema_contract", status: "block", message: "narrow", details: { errors: ["a", "b", "c", "d", "e"].map((column) => ({ column, message: "X" })) } },
      { id: "g13_source_coverage", status: "skip", details: { unmeasured: true } }];
    const host = document.createElement("div");
    document.body.appendChild(host);
    const root = createRoot(host);
    await act(async () => root.render(createElement(GateChecklist, { gates: many })));
    const more = host.querySelector<HTMLButtonElement>(".df2-check-more")!;
    assert.notEqual(more.tabIndex, -1);
    assert.equal(more.getAttribute("aria-expanded"), "false");
    await act(async () => more.click());
    assert.equal(host.querySelector('[data-gate="g3_schema_contract"] .df2-check-findings')?.textContent, "X: a, b, c, d, e");
    const rows = () => [...host.querySelectorAll<HTMLElement>("[data-check-row]")];
    rows()[1].focus();
    const blockingToggle = host.querySelector<HTMLButtonElement>(".df2-check-group-title button")!;
    await act(async () => blockingToggle.click());
    assert.deepEqual(rows().map((r) => r.tabIndex), [0], "one tab stop remains");
  });
});
