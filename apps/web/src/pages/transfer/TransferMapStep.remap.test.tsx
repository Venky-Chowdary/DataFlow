import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { EditableMapping } from "../../lib/mapping";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Node: dom.window.Node,
  KeyboardEvent: dom.window.KeyboardEvent,
  MutationObserver: dom.window.MutationObserver,
  localStorage: dom.window.localStorage,
  sessionStorage: dom.window.sessionStorage,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);
// jsdom has no layout: give every element a box so the portaled menu places.
dom.window.HTMLElement.prototype.getBoundingClientRect = function () {
  return { top: 100, left: 10, right: 410, bottom: 130, width: 400, height: 30, x: 10, y: 100, toJSON() {} } as DOMRect;
};
dom.window.HTMLElement.prototype.scrollIntoView = function () {};

const { act, createElement, useState } = await import("react");
const { createRoot } = await import("react-dom/client");
const { TransferMapStep } = await import("./TransferMapStep.js");

type Col = Record<string, unknown>;
const col = (name: string, type: string, extra: Col = {}): Col => ({
  name, type, nullable: true, default: null, has_default: false, primary_key: false,
  identity: false, generated: false, required: false, ...extra,
});

let catalogColumns: Col[] = [];
let canAdd = true;
let autoMapSuggestions: Col[] = [];
let calls: Array<{ url: string; body: unknown }> = [];

function stubFetch() {
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const body = init?.body ? JSON.parse(String(init.body)) : null;
    calls.push({ url, body });
    const ok = (data: unknown) =>
      new Response(JSON.stringify(data), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url.endsWith("/transfer/mapping/destination-columns")) {
      return ok({
        version: "dest_column_catalog.v1", connected: true, table_exists: true, dest_type: "postgresql",
        nullability_measured: true, can_add_columns: canAdd,
        add_columns_reason: canAdd ? "The writer adds the column." : "Schema policy 'type_locked' forbids ADD COLUMN.",
        message: "", columns: catalogColumns.map((c, i) => ({ ordinal: i, ...c })),
      });
    }
    if (url.endsWith("/transfer/mapping/type-compatibility")) {
      return ok({
        results: (body.pairs as Array<{ source_type: string; target_type: string }>).map((p) => ({
          source_type: p.source_type, target_type: p.target_type,
          verdict: p.source_type === p.target_type ? "safe" : "lossy",
          conversion_class: "x",
          reasons: p.source_type === p.target_type ? [{ kind: "safe", message: "round-trips" }]
            : [{ kind: "narrowing", message: `${p.source_type} → ${p.target_type} is narrower.` }],
          requires_risk_contract: false,
        })),
      });
    }
    if (url.endsWith("/transfer/mapping/auto-map")) return ok({ suggestions: autoMapSuggestions });
    if (url.endsWith("/transfer/mapping/memory")) return ok({ recorded: 1, total: 1 });
    return new Response("{}", { status: 404 });
  }) as typeof fetch;
}

const m = (source: string, target: string, type: string, extra: Partial<EditableMapping> = {}): EditableMapping => ({
  source, target, confidence: 0.99, inferredType: type, destType: type, existsInDestination: true,
  transform: "none", fidelity: "preserve", approved: true, ...extra,
});

let root: ReturnType<typeof createRoot> | null = null;
let host: HTMLDivElement | null = null;
let latest: EditableMapping[] = [];
let continued = 0;

function Harness({ initial }: { initial: EditableMapping[] }) {
  const [rows, setRows] = useState(initial);
  latest = rows;
  return createElement(TransferMapStep, {
    columnMappings: rows,
    analysis: null,
    destColumns: catalogColumns.map((c) => String(c.name)),
    destSchemaLoading: false,
    destTableExists: true,
    destConnected: true,
    targetCollection: "orders_dest",
    targetDatabase: "warehouse",
    destKindMode: "database",
    destType: "postgresql",
    sourceLabel: "mysql.orders",
    sourceSubtitle: "",
    sourceType: "mysql",
    destRouteLabel: "warehouse.orders_dest",
    destRouteSubtitle: "",
    mappingReviewCount: 0,
    confidenceThreshold: 0.85,
    onChangeMappings: (next: EditableMapping[]) => setRows(next),
    onBack: () => {},
    onContinue: () => { continued += 1; },
    destEndpoint: { kind: "database", format: "postgresql", connector_id: "c1", table: "orders_dest" },
    destProbeKey: "c1|orders_dest",
    schemaPolicy: "manual_review",
    syncMode: "full_refresh_append",
    sourceObjectName: "orders",
  });
}

async function settle(ms = 0) {
  await act(async () => {
    await new Promise((r) => setTimeout(r, ms));
  });
}

async function render(initial: EditableMapping[]) {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(createElement(Harness, { initial })));
  await settle(10);
  await settle(10);
}

function q<T extends Element = HTMLElement>(sel: string, scope: ParentNode = document): T {
  const el = scope.querySelector<T>(sel);
  assert.ok(el, `missing ${sel}`);
  return el;
}

function combo(source: string): HTMLInputElement {
  return q<HTMLInputElement>(`input[aria-label="Destination column for ${source}"]`);
}

async function key(el: Element, k: string, mods: Partial<KeyboardEventInit> = {}) {
  await act(async () => {
    el.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...mods }));
  });
}

function continueButton(): HTMLButtonElement {
  const b = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((x) => x.textContent?.includes("Continue to Validate"));
  assert.ok(b);
  return b;
}

const ORDERS = () => [
  m("order_id", "order_id", "BIGINT", { existsInDestination: false, createNew: true }),
  m("name", "name", "VARCHAR(80)", { existsInDestination: false, createNew: true }),
  m("first_name", "first_name", "VARCHAR(40)", { existsInDestination: false, createNew: true }),
  m("last_name", "last_name", "VARCHAR(60)", { existsInDestination: false, createNew: true }),
];

beforeEach(() => {
  calls = [];
  continued = 0;
  canAdd = true;
  autoMapSuggestions = [];
  catalogColumns = [
    col("legacy_ref", "BIGINT", { nullable: false, primary_key: true, required: true }),
    col("given_name", "VARCHAR(40)", { nullable: false, required: true }),
    col("surname", "VARCHAR(60)"),
    col("full_name", "VARCHAR(120)"),
    col("amount", "DECIMAL(12,2)", { nullable: false, default: "0", has_default: true }),
  ];
  stubFetch();
});

afterEach(async () => {
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
  document.body.innerHTML = "";
});

describe("Map step: remap onto existing destination columns", () => {
  it("each row's picker lists the destination's columns with type, nullability, key and default", async () => {
    await render(ORDERS());
    assert.ok(calls.some((c) => c.url.endsWith("/destination-columns")), "catalog read from the API");
    const input = combo("order_id");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const list = document.getElementById(input.getAttribute("aria-controls")!)!;
    assert.ok(list);
    assert.equal(list.getAttribute("role"), "listbox");
    assert.equal(input.getAttribute("aria-expanded"), "true");
    const text = list.textContent ?? "";
    assert.match(text, /legacy_ref/);
    assert.match(text, /BIGINT · PK · NOT NULL/);
    assert.match(text, /DECIMAL\(12,2\) · NOT NULL · default 0/);
    assert.match(text, /required/);
  });

  it("picking legacy_ref remaps order_id onto the existing column and its type", async () => {
    await render(ORDERS());
    const input = combo("order_id");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const option = Array.from(document.querySelectorAll<HTMLElement>('[role="option"]')).find((o) => o.textContent?.startsWith("legacy_ref"));
    assert.ok(option);
    await act(async () => option.click());
    const row = latest.find((r) => r.source === "order_id")!;
    assert.equal(row.target, "legacy_ref");
    assert.equal(row.destType, "BIGINT");
    assert.equal(row.existsInDestination, true);
    assert.equal(row.createNew, false);
    assert.equal(row.approved, false, "a remap needs a fresh Approve");
  });

  it("a typed name the table lacks is offered as 'will create column' only when the destination can add one", async () => {
    await render(ORDERS());
    const input = combo("name");
    await act(async () => input.focus());
    await act(async () => {
      const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, "value")!.set!;
      setter.call(input, "display_name");
      input.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
    });
    assert.match(document.body.textContent ?? "", /will create column/);
    await key(input, "Enter");
    const row = latest.find((r) => r.source === "name")!;
    assert.equal(row.target, "display_name");
    assert.equal(row.createNew, true);
  });

  it("when ADD COLUMN is forbidden the typed name is refused, not created", async () => {
    canAdd = false;
    await render(ORDERS());
    const input = combo("name");
    await act(async () => input.focus());
    await act(async () => {
      const setter = Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, "value")!.set!;
      setter.call(input, "display_name");
      input.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
    });
    assert.doesNotMatch(document.body.textContent ?? "", /will create column/);
    assert.match(document.body.textContent ?? "", /is not a destination column/);
    await key(input, "Enter");
    assert.equal(latest.find((r) => r.source === "name")!.target, "name");
  });

  it("the destination panel flags required unmapped columns and Validate is held", async () => {
    await render([
      m("order_id", "legacy_ref", "BIGINT"),
      m("last_name", "surname", "VARCHAR(60)"),
    ]);
    const panel = q('aside[aria-label^="Destination columns of"]');
    const given = q('[data-dest-column="given_name"]', panel);
    assert.match(given.textContent ?? "", /Required/);
    assert.match(given.textContent ?? "", /NOT NULL, no default: blocks Validate until mapped/);
    const amount = q('[data-dest-column="amount"]', panel);
    assert.match(amount.textContent ?? "", /default \(0\)/);
    const full = q('[data-dest-column="full_name"]', panel);
    assert.match(full.textContent ?? "", /new rows get NULL/);
    assert.match(q('[data-testid="map-target-issues"]').textContent ?? "", /given_name is NOT NULL with no default/);
    assert.equal(continueButton().disabled, true);
    assert.match(continueButton().title, /given_name/);
  });

  it("picking a column another source writes moves it (one source per target) and undo restores both", async () => {
    await render([
      m("order_id", "legacy_ref", "BIGINT"),
      m("first_name", "given_name", "VARCHAR(40)"),
      m("name", "full_name", "VARCHAR(80)"),
    ]);
    const input = combo("name");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const claimed = Array.from(document.querySelectorAll<HTMLElement>('[role="option"]')).find((o) => o.textContent?.startsWith("given_name"));
    assert.match(claimed!.textContent ?? "", /mapped from first_name/);
    await act(async () => claimed!.click());
    assert.equal(latest.find((r) => r.source === "name")!.target, "given_name");
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "");
    await settle(50);
    assert.match(q('[data-testid="map-live-region"]').textContent ?? "", /name now writes given_name\. first_name unmapped from it/);
    assert.equal(continueButton().disabled, true, "first_name has no destination now");

    await act(async () => (document.activeElement as HTMLElement | null)?.blur());
    await key(document.body, "z", { ctrlKey: true });
    assert.equal(latest.find((r) => r.source === "name")!.target, "full_name");
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "given_name");
    await key(document.body, "z", { ctrlKey: true, shiftKey: true });
    assert.equal(latest.find((r) => r.source === "name")!.target, "given_name");
  });

  it("keyboard drag and drop: Space picks up, arrows move, Enter drops, all announced", async () => {
    await render([
      m("order_id", "legacy_ref", "BIGINT"),
      m("first_name", "", "VARCHAR(40)"),
    ]);
    const grip = q<HTMLButtonElement>('button[aria-label="Move first_name to a destination column"]');
    assert.equal(grip.getAttribute("aria-roledescription"), "draggable source column");
    await act(async () => grip.focus());
    await key(grip, " ");
    await settle(50);
    const live = q('[data-testid="map-live-region"]');
    assert.match(live.textContent ?? "", /Picked up first_name/);
    assert.equal(grip.getAttribute("aria-pressed"), "true");
    await key(grip, "ArrowDown");
    await settle(50);
    assert.match(live.textContent ?? "", /Over given_name/);
    assert.ok(document.querySelector('[data-dest-column="given_name"].is-drop-over'));
    await key(grip, "Enter");
    await settle(50);
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "given_name");
    assert.match(live.textContent ?? "", /first_name now writes given_name/);
  });

  it("Escape cancels a keyboard drag and leaves the row unchanged", async () => {
    await render([m("first_name", "surname", "VARCHAR(40)")]);
    const grip = q<HTMLButtonElement>('button[aria-label="Move first_name to a destination column"]');
    await act(async () => grip.focus());
    await key(grip, "Enter");
    await key(grip, "ArrowDown");
    await key(grip, "Escape");
    await settle(50);
    assert.equal(latest[0].target, "surname");
    assert.match(q('[data-testid="map-live-region"]').textContent ?? "", /Cancelled\. first_name is unchanged/);
  });

  it("type compatibility per row comes from the API grader", async () => {
    await render([m("first_name", "given_name", "VARCHAR(80)", { destType: "VARCHAR(40)" })]);
    await settle(350);
    await settle(10);
    assert.ok(calls.some((c) => c.url.endsWith("/type-compatibility")));
    const chip = q('[data-verdict]');
    assert.equal(chip.getAttribute("data-verdict"), "lossy");
    const tip = document.getElementById(chip.getAttribute("aria-describedby")!)!;
    assert.ok(tip);
    assert.match(tip.textContent ?? "", /VARCHAR\(80\) → VARCHAR\(40\) is narrower/);
  });

  it("auto-map previews suggestions with confidence and applies the selected ones in one undo step", async () => {
    autoMapSuggestions = [
      { source: "first_name", target: "given_name", confidence: 0.86, kind: "synonym", reason: "Synonyms", selected: true, alternatives: [], compat: { verdict: "safe", reasons: [] } },
      { source: "last_name", target: "surname", confidence: 0.86, kind: "synonym", reason: "Synonyms", selected: true, alternatives: [] },
      { source: "name", target: "full_name", confidence: 0.6, kind: "token", reason: "Shares name", selected: false, alternatives: [] },
    ];
    await render(ORDERS());
    const btn = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((b) => b.textContent === "Auto-map…")!;
    await act(async () => btn.click());
    await settle(20);
    const dialog = q(".df2-ms-automap[role=\"dialog\"]");
    assert.match(dialog.textContent ?? "", /Auto-map columns/);
    assert.match(dialog.textContent ?? "", /86%/);
    assert.match(dialog.textContent ?? "", /Synonym/);
    const apply = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((b) => /^Apply 2 mappings$/.test(b.textContent ?? ""));
    assert.ok(apply, "weak partial match starts unselected");
    await act(async () => apply!.click());
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "given_name");
    assert.equal(latest.find((r) => r.source === "last_name")!.target, "surname");
    assert.equal(latest.find((r) => r.source === "name")!.target, "name");
    await key(document.body, "z", { metaKey: true });
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "first_name");
    assert.equal(latest.find((r) => r.source === "last_name")!.target, "last_name");
  });

  it("clear all empties every destination and Validate is held until each row is mapped or omitted", async () => {
    await render([m("order_id", "legacy_ref", "BIGINT"), m("first_name", "given_name", "VARCHAR(40)")]);
    const clear = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((b) => b.textContent === "Clear all")!;
    await act(async () => clear.click());
    assert.deepEqual(latest.map((r) => r.target), ["", ""]);
    assert.equal(continueButton().disabled, true);
    assert.match(q('[data-testid="map-target-issues"]').textContent ?? "", /order_id has no destination column/);
  });

  it("a generated column is listed but cannot be picked or dropped onto", async () => {
    catalogColumns = [
      ...catalogColumns,
      col("total_with_tax", "DECIMAL(12,2)", { generated: true, writable: false, fill: "generated" }),
    ];
    await render([m("amount", "", "DECIMAL(12,2)")]);
    const input = combo("amount");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const option = Array.from(document.querySelectorAll<HTMLElement>('[role="option"]')).find((o) => o.textContent?.startsWith("total_with_tax"));
    assert.ok(option);
    assert.equal(option.getAttribute("aria-disabled"), "true");
    assert.match(option.textContent ?? "", /engine-filled/);
    await act(async () => option.click());
    assert.equal(latest[0].target, "", "a disabled option does not commit");
    await key(input, "Escape");
    const grip = q<HTMLButtonElement>('button[aria-label="Move amount to a destination column"]');
    await act(async () => grip.focus());
    await key(grip, " ");
    await key(grip, "End");
    await key(grip, "Enter");
    await settle(50);
    assert.equal(latest[0].target, "");
    assert.match(q('[data-testid="map-live-region"]').textContent ?? "", /total_with_tax is generated by the destination/);
  });

  it("keyboard drag moves over the panel's filtered list, not every column", async () => {
    await render([m("order_id", "legacy_ref", "BIGINT"), m("first_name", "", "VARCHAR(40)")]);
    const unmapped = Array.from(document.querySelectorAll<HTMLButtonElement>(".df2-ms-chip")).find((b) => b.textContent?.startsWith("Unmapped"))!;
    await act(async () => unmapped.click());
    const grip = q<HTMLButtonElement>('button[aria-label="Move first_name to a destination column"]');
    await act(async () => grip.focus());
    await key(grip, " ");
    await settle(50);
    assert.match(q('[data-testid="map-live-region"]').textContent ?? "", /Over given_name/);
    await key(grip, "ArrowDown");
    await key(grip, "Enter");
    await settle(50);
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "surname");
  });

  it("Ctrl+Z inside an open dialog does not undo the mapping behind it", async () => {
    await render([m("first_name", "surname", "VARCHAR(40)")]);
    const input = combo("first_name");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const option = Array.from(document.querySelectorAll<HTMLElement>('[role="option"]')).find((o) => o.textContent?.startsWith("given_name"))!;
    await act(async () => option.click());
    assert.equal(latest[0].target, "given_name");
    const btn = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((b) => b.textContent === "Auto-map…")!;
    await act(async () => btn.click());
    await settle(20);
    const inDialog = document.querySelector<HTMLButtonElement>('[role="dialog"] button')!;
    await act(async () => inDialog.focus());
    await key(inDialog, "z", { ctrlKey: true });
    assert.equal(latest[0].target, "given_name");
  });

  it("the combobox never promises a column the destination cannot add", async () => {
    canAdd = false;
    await render([m("note", "brand_new", "TEXT", { existsInDestination: false, createNew: true })]);
    const hint = document.getElementById(combo("note").getAttribute("aria-describedby")!)!;
    assert.doesNotMatch(hint.textContent ?? "", /will be created/);
    assert.match(hint.textContent ?? "", /cannot be created/);
    assert.match(host!.textContent ?? "", /not a destination column/);
  });

  it("auto-map never pre-selects an incompatible pair, and says why in text", async () => {
    autoMapSuggestions = [
      { source: "first_name", target: "given_name", confidence: 1, kind: "exact", reason: "Same name.", selected: true, alternatives: [],
        compat: { verdict: "incompatible", reasons: [{ kind: "incompatible", message: "BLOB → INTEGER is an unsupported domain jump." }] } },
    ];
    await render(ORDERS());
    const btn = Array.from(host!.querySelectorAll<HTMLButtonElement>("button")).find((b) => b.textContent === "Auto-map…")!;
    await act(async () => btn.click());
    await settle(20);
    const dialog = q(".df2-ms-automap[role=\"dialog\"]");
    const box = dialog.querySelector<HTMLInputElement>('input[type="checkbox"]')!;
    assert.equal(box.checked, false);
    assert.match(dialog.textContent ?? "", /Same name\./);
    assert.match(dialog.textContent ?? "", /unsupported domain jump/);
  });

  it("touch: a tap or a jitter picks nothing up; a hold does; a cancelled drag never drops on lift", async () => {
    await render([m("first_name", "", "VARCHAR(40)"), m("last_name", "", "VARCHAR(60)")]);
    const fire = (el: Element, type: string, x = 10, y = 10) => {
      const Ctor = (dom.window as unknown as { PointerEvent?: typeof MouseEvent }).PointerEvent ?? dom.window.MouseEvent;
      const ev = new Ctor(type, { bubbles: true, cancelable: true, clientX: x, clientY: y });
      Object.defineProperty(ev, "pointerType", { value: "touch" });
      Object.defineProperty(ev, "pointerId", { value: 9 });
      el.dispatchEvent(ev);
    };
    let under: Element | null = null;
    document.elementFromPoint = () => under;
    const grip = q<HTMLButtonElement>('button[aria-label="Move first_name to a destination column"]');
    const given = () => q('[data-dest-column="given_name"]').querySelector(".df2-ms-dest-name");

    // A flick: down and up over a column at once, with a 3px jitter.
    under = given();
    await act(async () => fire(grip, "pointerdown", 10, 10));
    assert.equal(grip.getAttribute("aria-pressed"), "false", "no pick-up on touch-down");
    await act(async () => fire(grip, "pointermove", 12, 12));
    await act(async () => fire(grip, "pointerup", 12, 12));
    await settle(200);
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "", "a flick drops nothing");

    // A hold picks up without moving.
    under = null;
    await act(async () => fire(grip, "pointerdown", 10, 10));
    await settle(200);
    assert.equal(grip.getAttribute("aria-pressed"), "true", "a 150 ms hold picks up");
    // Escape cancels; lifting the finger over a column afterwards drops nothing.
    await key(grip, "Escape");
    under = given();
    await act(async () => fire(grip, "pointermove", 20, 20));
    await act(async () => fire(grip, "pointerup", 20, 20));
    await settle(50);
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "", "a cancelled drag never drops");
  });

  it("touch: a finger drags a source onto a destination column; lifting off a column cancels", async () => {
    await render([m("first_name", "", "VARCHAR(40)"), m("last_name", "", "VARCHAR(60)")]);
    const fire = (el: Element, type: string, pointerType: string, x = 10, y = 10) => {
      const Ctor = (dom.window as unknown as { PointerEvent?: typeof MouseEvent }).PointerEvent ?? dom.window.MouseEvent;
      const ev = new Ctor(type, { bubbles: true, cancelable: true, clientX: x, clientY: y });
      Object.defineProperty(ev, "pointerType", { value: pointerType });
      Object.defineProperty(ev, "pointerId", { value: 7 });
      el.dispatchEvent(ev);
    };
    let under: Element | null = null;
    document.elementFromPoint = () => under;
    const grip = q<HTMLButtonElement>('button[aria-label="Move first_name to a destination column"]');
    await act(async () => fire(grip, "pointerdown", "touch"));
    under = q('[data-dest-column="given_name"]').querySelector(".df2-ms-dest-name");
    await act(async () => fire(grip, "pointermove", "touch", 20, 20));
    assert.ok(document.querySelector('[data-dest-column="given_name"].is-drop-over'));
    assert.match(host!.textContent ?? "", /Lift your finger on a destination column/);
    await act(async () => fire(grip, "pointerup", "touch", 20, 20));
    await settle(50);
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "given_name");

    const grip2 = q<HTMLButtonElement>('button[aria-label="Move last_name to a destination column"]');
    await act(async () => fire(grip2, "pointerdown", "pen"));
    under = null;
    await act(async () => fire(grip2, "pointermove", "pen", 900, 900));
    await act(async () => fire(grip2, "pointerup", "pen", 900, 900));
    await settle(50);
    assert.equal(latest.find((r) => r.source === "last_name")!.target, "");
    assert.match(q('[data-testid="map-live-region"]').textContent ?? "", /Cancelled\. last_name is unchanged/);

    // A mouse keeps native drag and drop: pointer events from it start nothing.
    await act(async () => fire(grip2, "pointerdown", "mouse"));
    assert.equal(grip2.getAttribute("aria-pressed"), "false");
  });

  it("a 1,500-column destination renders a window of options, not all of them", async () => {
    catalogColumns = Array.from({ length: 1500 }, (_, i) => col(`col_${i}`, "INT"));
    await render([m("first_name", "col_3", "INT")]);
    const input = combo("first_name");
    await act(async () => input.focus());
    await key(input, "ArrowDown");
    const options = document.querySelectorAll('[role="option"]');
    assert.ok(options.length > 0 && options.length < 60, `rendered ${options.length}`);
    assert.equal(options[0].getAttribute("aria-setsize"), "1501");
    const panelRows = document.querySelectorAll("[data-dest-column]");
    assert.ok(panelRows.length < 60, `panel rendered ${panelRows.length}`);
  });

  it("Continue records only pairs the operator chose, and each one once per session", async () => {
    await render([
      m("order_id", "legacy_ref", "BIGINT", { operatorTarget: true }),
      m("first_name", "given_name", "VARCHAR(40)", { operatorTarget: true }),
      // An engine suggestion left as it was is not a confirmation.
      m("last_name", "surname", "VARCHAR(60)"),
    ]);
    assert.equal(continueButton().disabled, false);
    await act(async () => continueButton().click());
    await settle(10);
    assert.equal(continued, 1);
    const posts = () => calls.filter((c) => c.url.endsWith("/transfer/mapping/memory"));
    const mem = posts()[0];
    assert.deepEqual((mem?.body as { destination: unknown }).destination,
      { kind: "database", format: "postgresql", connector_id: "c1", table: "orders_dest" });
    assert.deepEqual((mem?.body as { pairs: unknown }).pairs, [
      { source: "order_id", target: "legacy_ref" },
      { source: "first_name", target: "given_name" },
    ]);
    // Back from Validate and Continue again: nothing new was confirmed.
    await act(async () => continueButton().click());
    await settle(10);
    assert.equal(continued, 2);
    assert.equal(posts().length, 1, "the same confirmations are not posted twice");
  });
});
