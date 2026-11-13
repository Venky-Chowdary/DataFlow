/**
 * Plan-conflict resolution, rendered and driven like a user would: the
 * dialog compares the latest revision with the unsaved mappings, and each
 * choice does exactly what its label says.
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

const { act, createElement, useRef } = await import("react");
const { createRoot } = await import("react-dom/client");
const { usePlanConflict } = await import("./usePlanConflict.js");
const { PlanConflictDialog } = await import("../components/transfer/PlanConflictDialog.js");
const { PlanVersionConflictError } = await import("./planConcurrency.js");
type Api = import("./usePlanConflict.js").PlanConflictApi;
type Row = import("./planConcurrency.js").PlanMappingRow;

const MINE: Row[] = [
  { source: "email", target: "email" },
  { source: "name", target: "full_name" },
];
const LATEST: Row[] = [
  { source: "email", target: "email" },
  { source: "username", target: "full_name" },
  { source: "id", target: "id" },
];
const plan = (head: number, mappings: Row[]) => ({
  plan: {
    id: "p1",
    active_version: head,
    revisions: Array.from({ length: head }, (_, i) => ({ version: i + 1, mappings: i + 1 === head ? mappings : [] })),
  },
});

type Harness = {
  open: (mine: Row[]) => Promise<"shown" | "resolved">;
  head: { current: number | null };
  applied: Array<{ mappings: Row[]; version: number }>;
  notes: string[];
};

let root: ReturnType<typeof createRoot> | undefined;

async function mount(api: Api, initialHead = 2): Promise<Harness> {
  const harness = { applied: [], notes: [] } as unknown as Harness;
  function App() {
    const headRef = useRef<number | null>(initialHead);
    harness.head = headRef;
    const resolver = usePlanConflict({
      api,
      headRef,
      onUseLatest: (mappings, version) => harness.applied.push({ mappings, version }),
      notify: (title) => harness.notes.push(title),
    });
    harness.open = (mine) => resolver.open("p1", mine, "validate");
    return createElement("div", null,
      createElement("button", { type: "button", id: "validate" }, "Validate"),
      createElement(PlanConflictDialog, {
        conflict: resolver.conflict,
        onUseLatest: resolver.useLatest,
        onKeepMine: () => void resolver.keepMine(),
        onCancel: resolver.cancel,
      }),
    );
  }
  const host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(createElement(App)));
  (document.getElementById("validate") as HTMLButtonElement).focus();
  return harness;
}

const dialog = () => document.querySelector('[role="dialog"]') as HTMLElement | null;
const button = (label: RegExp) =>
  [...document.querySelectorAll("button")].find((b) => label.test(b.textContent ?? "")) as HTMLButtonElement;
const click = async (label: RegExp) => act(async () => button(label).click());
const escape = async () =>
  act(async () => {
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
  });

function api(overrides: Partial<Api> = {}): Api & { saved: Array<{ mappings: Row[]; base: number }> } {
  const saved: Array<{ mappings: Row[]; base: number }> = [];
  return {
    saved,
    getPlan: async () => plan(3, LATEST),
    saveMappings: async (_id, mappings, base) => {
      saved.push({ mappings, base });
      return plan(base + 1, mappings);
    },
    ...overrides,
  };
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
});
after(() => dom.window.close());

describe("plan conflict dialog", () => {
  it("compares each destination column and highlights the differences", async () => {
    const h = await mount(api());
    await act(async () => void (await h.open(MINE)));
    const d = dialog();
    assert.ok(d, "the dialog is open");
    assert.equal(d.getAttribute("aria-modal"), "true");
    assert.match(d.getAttribute("aria-labelledby") ?? "", /.+/);
    assert.ok(d.contains(document.activeElement), "focus moved into the dialog");
    const rows = [...d.querySelectorAll("tbody tr")].map((tr) => ({
      target: tr.querySelector("th")?.textContent,
      differs: tr.getAttribute("data-differs"),
    }));
    assert.deepEqual(rows, [
      { target: "email", differs: "false" },
      { target: "full_name", differs: "true" },
      { target: "id", differs: "true" },
    ]);
    assert.match(d.textContent ?? "", /Revision 3 was saved after this page loaded the plan/);
    assert.match(d.textContent ?? "", /2 of 3 destination columns differ/);
  });

  it("Use latest puts the latest revision in the editor and adopts its head", async () => {
    const h = await mount(api());
    await act(async () => void (await h.open(MINE)));
    await click(/^Use latest/);
    assert.equal(dialog(), null);
    assert.deepEqual(h.applied, [{ mappings: LATEST, version: 3 }]);
    assert.equal(h.head.current, 3);
    assert.equal(document.activeElement?.id, "validate", "focus returns to the control that opened it");
  });

  it("Keep mine saves the user's mappings on top of the latest head", async () => {
    const a = api();
    const h = await mount(a);
    await act(async () => void (await h.open(MINE)));
    await click(/^Keep mine/);
    assert.deepEqual(a.saved, [{ mappings: MINE, base: 3 }], "an explicit overwrite based on the head the user saw");
    assert.equal(h.head.current, 4);
    assert.equal(dialog(), null);
    assert.deepEqual(h.applied, [], "the editor keeps the user's mappings");
    assert.deepEqual(h.notes, ["Your mappings were saved"]);
  });

  it("Cancel and Escape change nothing, so the next save still conflicts", async () => {
    const h = await mount(api());
    await act(async () => void (await h.open(MINE)));
    await click(/^Cancel$/);
    assert.equal(dialog(), null);
    assert.equal(h.head.current, 2);
    await act(async () => void (await h.open(MINE)));
    await escape();
    assert.equal(dialog(), null);
    assert.equal(h.head.current, 2);
    assert.deepEqual(h.applied, []);
  });

  it("shows nothing when the latest revision already holds the user's mappings", async () => {
    const h = await mount(api({ getPlan: async () => plan(5, MINE) }));
    let outcome = "";
    await act(async () => void (outcome = await h.open(MINE)));
    assert.equal(outcome, "resolved");
    assert.equal(dialog(), null);
    assert.equal(h.head.current, 5, "the page's own write landed: adopt its head");
  });

  it("Keep mine that meets yet another change compares again instead of overwriting", async () => {
    let head = 3;
    const a = api({
      getPlan: async () => plan(head, LATEST),
      saveMappings: async () => {
        head = 4;
        throw new PlanVersionConflictError(3, 4);
      },
    });
    const h = await mount(a);
    await act(async () => void (await h.open(MINE)));
    await click(/^Keep mine/);
    const d = dialog();
    assert.ok(d, "still open");
    assert.match(d.textContent ?? "", /Revision 4 was saved/);
    assert.match(d.textContent ?? "", /changed again/);
    assert.equal(h.head.current, 2);
  });
});
