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
let importResponse: Record<string, unknown> = {};
let templateApply: Record<string, unknown> = {};
let addedSteps: unknown[] = [];

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
    if (url.endsWith("/transfer/mapping/export")) {
      return new Response("apiVersion: datawrap.io/mapping/v1\nkind: ColumnMapping\n", {
        status: 200,
        headers: { "Content-Type": "application/yaml", "Content-Disposition": 'attachment; filename="orders-orders_dest.mapping.yaml"' },
      });
    }
    if (url.endsWith("/transfer/mapping/import")) return ok(importResponse);
    if (url.endsWith("/transfer/mapping/templates") && init?.method === "POST") return ok({ id: "mtpl_1", name: body.name });
    if (url.endsWith("/transfer/mapping/templates")) return ok({ templates: [{ id: "mtpl_1", name: "Orders onto customers", description: "", created_at: "", created_by: "a@b.c", mapping_count: 2, computed_count: 1 }] });
    if (url.endsWith("/transfer/mapping/templates/mtpl_1/apply")) return ok(templateApply);
    if (url.endsWith("/transfer/mapping/templates/mtpl_1") && init?.method === "DELETE") return ok({ deleted: "mtpl_1" });
    if (url.endsWith("/shape/expression")) {
      const bad = /surname/.test(body.expression);
      return ok(bad ? { valid: false, error: "unknown column 'surname'" } : { valid: true, columns: ["first_name", "last_name"] });
    }
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
    transformSteps: [{ op: "derive_column", options: { to: "full_name", expression: "concat(first_name, ' ', last_name)" } }],
    onAddComputedSteps: (steps: unknown[]) => { addedSteps.push(...steps); },
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

const ROWS = () => [
  m("order_id", "order_id", "BIGINT", { existsInDestination: false, createNew: true }),
  m("first_name", "first_name", "VARCHAR(40)", { existsInDestination: false, createNew: true }),
  m("last_name", "last_name", "VARCHAR(60)", { existsInDestination: false, createNew: true }),
];

function button(text: string | RegExp): HTMLButtonElement {
  const b = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((x) =>
    typeof text === "string" ? x.textContent === text : text.test(x.textContent ?? ""));
  assert.ok(b, `no button ${text}`);
  return b;
}

async function type(el: HTMLInputElement | HTMLTextAreaElement, value: string) {
  await act(async () => {
    const proto = el instanceof dom.window.HTMLTextAreaElement ? dom.window.HTMLTextAreaElement.prototype : dom.window.HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, "value")!.set!.call(el, value);
    el.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
  });
}

beforeEach(() => {
  calls = [];
  addedSteps = [];
  catalogColumns = [
    col("legacy_ref", "BIGINT", { nullable: false, primary_key: true, required: true }),
    col("given_name", "VARCHAR(40)"),
    col("surname", "VARCHAR(60)"),
    col("full_name", "VARCHAR(120)"),
    col("loaded_at", "TIMESTAMP", { nullable: false, default: "now()", has_default: true }),
    col("batch_ts", "TIMESTAMP"),
  ];
  canAdd = true;
  stubFetch();
});

afterEach(async () => {
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
  document.body.innerHTML = "";
});

describe("Map step P2: computed columns, templates, mapping-as-code", () => {
  it("export sends the rows and the Transform's computed steps as a document", async () => {
    await render(ROWS());
    await act(async () => button("Export YAML").click());
    await settle(20);
    const call = calls.find((c) => c.url.endsWith("/transfer/mapping/export"));
    assert.ok(call);
    const sent = call.body as { format: string; mappings: Array<{ source: string }>; computed: Array<{ op: string }>; destination: { table: string } };
    assert.equal(sent.format, "yaml");
    assert.deepEqual(sent.mappings.map((x) => x.source), ["order_id", "first_name", "last_name"]);
    assert.deepEqual(sent.computed.map((x) => x.op), ["derive_column"]);
    assert.equal(sent.destination.table, "orders_dest");
    assert.match(document.querySelector('[data-testid="map-code-status"]')?.textContent ?? "", /orders-orders_dest\.mapping\.yaml/);
  });

  it("import applies the document in one undo step and offers its computed columns", async () => {
    importResponse = {
      mappings: [{ source: "order_id", target: "legacy_ref" }, { source: "first_name", target: "given_name" }, { source: "ghost", target: "x" }],
      computed: [{ op: "constant_column", options: { to: "source_system", value: "mysql" } }],
      checksum: "sha256:abc", checksum_verified: true, warnings: [], metadata: {},
    };
    await render(ROWS());
    const input = document.querySelector<HTMLInputElement>('input[type="file"]')!;
    const file = new dom.window.File(["apiVersion: x"], "orders.mapping.yaml", { type: "application/yaml" });
    Object.defineProperty(file, "text", { value: async () => "apiVersion: x" });
    await act(async () => {
      Object.defineProperty(input, "files", { value: [file], configurable: true });
      input.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    await settle(20);
    assert.equal(latest.find((r) => r.source === "order_id")!.target, "legacy_ref");
    assert.equal(latest.find((r) => r.source === "first_name")!.target, "given_name");
    const status = document.querySelector('[data-testid="map-code-status"]')?.textContent ?? "";
    assert.match(status, /Imported 2 mappings from orders\.mapping\.yaml · unchanged since export · not in this source: ghost/);
    await act(async () => button(/Add 1 computed column to Transform/).click());
    assert.deepEqual(addedSteps, [{ op: "constant_column", options: { to: "source_system", value: "mysql" } }]);
    await act(async () => (document.activeElement as HTMLElement | null)?.blur());
    await key(document.body, "z", { ctrlKey: true });
    assert.equal(latest.find((r) => r.source === "order_id")!.target, "order_id");
  });

  it("a computed column is checked by the engine's compiler and added to Transform", async () => {
    await render(ROWS());
    await act(async () => button("Computed column…").click());
    const target = document.querySelector<HTMLInputElement>('.df2-ms-computed input[placeholder="full_name"]')!;
    await type(target, "full_name");
    const expr = document.querySelector<HTMLTextAreaElement>(".df2-ms-computed textarea")!;
    await type(expr, "CONCAT(first_name, ' ', surname)");
    await settle(400);
    assert.match(document.querySelector(".df2-ms-computed")!.textContent ?? "", /unknown column 'surname'/);
    assert.equal(button("Add to Transform").disabled, true);
    await type(expr, "CONCAT(first_name, ' ', last_name)");
    await settle(400);
    assert.match(document.querySelector(".df2-ms-computed")!.textContent ?? "", /Valid\. Reads first_name, last_name/);
    await act(async () => button("Add to Transform").click());
    assert.deepEqual(addedSteps, [
      { op: "derive_column", options: { to: "full_name", expression: "CONCAT(first_name, ' ', last_name)" } },
    ]);
  });

  it("run start time adds one run-scoped value to Transform", async () => {
    await render(ROWS());
    await act(async () => button("Computed column…").click());
    await act(async () => document.querySelector<HTMLInputElement>('input[aria-label="Run start time (fixed once per run)"]')!.click());
    const target = document.querySelector<HTMLInputElement>('.df2-ms-computed input[placeholder="full_name"]')!;
    await type(target, "loaded_at");
    assert.match(document.querySelector(".df2-ms-computed")!.textContent ?? "", /or leave loaded_at unmapped: the destination stamps now\(\)/i);
    await type(target, "batch_ts");
    assert.match(document.querySelector(".df2-ms-computed")!.textContent ?? "", /fixed once when the run starts/);
    await act(async () => button("Add to Transform").click());
    assert.deepEqual(addedSteps, [{ op: "run_timestamp", options: { to: "batch_ts" } }]);
  });

  it("templates: save the mapping, apply one with re-bound pairs through the preview", async () => {
    catalogColumns.push(col("row_hash", "TEXT", { generated: true, writable: false, fill: "generated" }));
    templateApply = {
      suggestions: [{ source: "order_id", target: "legacy_ref", confidence: 0.98, kind: "template", reason: "Template pair OrderID → legacy_ref.", selected: true, alternatives: [] }],
      unbound: [{ source: "tax_code", target: "tax_ref", missing: "source" }],
      computed: [],
    };
    await render(ROWS());
    await act(async () => button("Templates…").click());
    await settle(20);
    const name = document.querySelector<HTMLInputElement>('.df2-ms-templates input[placeholder="Orders onto customers"]')!;
    await type(name, "Orders v2");
    await act(async () => button("Save as template").click());
    await settle(20);
    const saved = calls.find((c) => c.url.endsWith("/transfer/mapping/templates") && (c.body as { name?: string })?.name === "Orders v2");
    assert.ok(saved, "template POSTed");
    await act(async () => button("Apply…").click());
    await settle(20);
    const dialog = document.querySelector(".df2-ms-automap")!;
    assert.match(dialog.textContent ?? "", /Apply template “Orders onto customers”/);
    assert.match(dialog.textContent ?? "", /tax_code → tax_ref: no matching source column here/);
    assert.match(dialog.textContent ?? "", /Template/);
    await act(async () => button(/^Apply 1 mapping$/).click());
    assert.equal(latest.find((r) => r.source === "order_id")!.target, "legacy_ref");
    const apply = calls.find((c) => c.url.endsWith("/apply"))!.body as { unwritable_columns: string[] };
    assert.deepEqual(apply.unwritable_columns, ["row_hash"], "engine-filled columns are named to the server");
  });

  it("deleting a template asks in the page first and keeps focus in the dialog", async () => {
    await render(ROWS());
    await act(async () => button("Templates…").click());
    await settle(20);
    const del = document.querySelector<HTMLButtonElement>('button[aria-label="Delete Orders onto customers"]')!;
    await act(async () => del.click());
    assert.equal(calls.some((c) => c.url.endsWith("/templates/mtpl_1")), false, "nothing deleted before confirming");
    const confirm = document.querySelector<HTMLButtonElement>('button[aria-label="Confirm delete Orders onto customers"]')!;
    assert.ok(confirm, "an in-page confirmation, not a browser dialog");
    assert.equal(document.activeElement, confirm);
    await act(async () => button("Keep").click());
    assert.equal(document.activeElement, document.querySelector('button[aria-label="Delete Orders onto customers"]'));
    await act(async () => document.querySelector<HTMLButtonElement>('button[aria-label="Delete Orders onto customers"]')!.click());
    await act(async () => document.querySelector<HTMLButtonElement>('button[aria-label="Confirm delete Orders onto customers"]')!.click());
    await settle(20);
    assert.ok(calls.some((c) => c.url.endsWith("/templates/mtpl_1")), "deleted after confirming");
    assert.equal(document.activeElement?.textContent, "Workspace templates", "focus lands on the list, not the body");
  });

  it("the Import control shows keyboard focus on its visible label", async () => {
    await render(ROWS());
    const input = document.querySelector<HTMLInputElement>('input[type="file"]')!;
    const label = input.nextElementSibling;
    assert.ok(label?.matches("label.df2-ms-file-label"), "the label follows the input so :focus-visible + label can style it");
    assert.equal(label?.getAttribute("for"), input.id);
  });
});
