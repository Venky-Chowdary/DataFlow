import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { EditableMapping } from "../../lib/mapping";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  MutationObserver: dom.window.MutationObserver,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { TransferMapStep } = await import("./TransferMapStep.js");
let root: ReturnType<typeof createRoot> | null = null;
let host: HTMLDivElement | null = null;
let continueCount = 0;

const safeUnapproved: EditableMapping = {
  source: "order_id",
  target: "order_id",
  confidence: 0.99,
  inferredType: "INTEGER",
  destType: "INTEGER",
  existsInDestination: true,
  transform: "none",
  fidelity: "preserve",
  approved: false,
};

async function render(mapping: EditableMapping) {
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(createElement(TransferMapStep, {
    columnMappings: [mapping],
    analysis: null,
    destColumns: ["order_id"],
    destSchemaLoading: false,
    destTableExists: true,
    destConnected: true,
    targetCollection: "orders",
    targetDatabase: "warehouse",
    destKindMode: "database",
    destType: "postgresql",
    sourceLabel: "source",
    sourceSubtitle: "",
    sourceType: "postgresql",
    destRouteLabel: "warehouse.orders",
    destRouteSubtitle: "",
    mappingReviewCount: 0,
    confidenceThreshold: 0.85,
    onChangeMappings: () => {},
    onBack: () => {},
    onContinue: () => { continueCount += 1; },
  })));
}

function continueButton() {
  const button = Array.from(host!.querySelectorAll<HTMLButtonElement>("button"))
    .find((item) => item.textContent?.includes("Continue to Validate"));
  assert.ok(button);
  return button;
}

afterEach(async () => {
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
  continueCount = 0;
});

describe("Map to Validate approval gate", () => {
  it("keeps an unapproved safe mapping from advancing until explicitly approved", async () => {
    await render(safeUnapproved);
    const blocked = continueButton();
    assert.equal(blocked.disabled, true);
    assert.match(blocked.title, /1 column\(s\) need Approve before Validate/);
    await act(async () => blocked.click());
    assert.equal(continueCount, 0);

    await render({ ...safeUnapproved, approved: true });
    const ready = continueButton();
    assert.equal(ready.disabled, false);
    await act(async () => ready.click());
    assert.equal(continueCount, 1);
  });
});