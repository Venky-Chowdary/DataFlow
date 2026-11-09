import assert from "node:assert/strict";
import { test } from "node:test";
import { JSDOM } from "jsdom";
import {
  DATE_LOCALES, NUMBER_LOCALES, SCHEMA_POLICIES, SYNC_MODES, VALIDATION_MODES, syncModeHonestyLine,
} from "../../lib/transferConstants";

test("append identity is selected metadata, not an implicit destination key or mode change", async () => {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://localhost/", pretendToBeVisual: true,
  });
  Object.assign(globalThis, {
    window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { DestinationAdvancedDrawer } = await import("./DestinationAdvancedDrawer");
  const root = createRoot(document.getElementById("root")!);
  const selected: string[] = [];
  const modes: string[] = [];
  const noop = () => {};
  try {
    await act(async () => root.render(createElement(DestinationAdvancedDrawer, {
      open: true, onClose: noop, syncModes: SYNC_MODES, schemaPolicies: SCHEMA_POLICIES,
      validationModes: VALIDATION_MODES, dateLocales: DATE_LOCALES, numberLocales: NUMBER_LOCALES,
      syncMode: "full_refresh_append", schemaPolicy: "manual_review", validationMode: "strict",
      dateLocale: "", numberLocale: "", backfillNewFields: false,
      streamNames: ["sample-orders"], streamFields: {}, defaultCursor: "", defaultPrimaryKey: "order_id",
      sourceColumns: ["order_id", "status"], sourceSchema: { order_id: "INTEGER", status: "TEXT" },
      syncModeLabel: "Full append", schemaPolicyLabel: "Manual approval", requiresCursor: false,
      requiresPrimaryKey: false, streamNeedsReview: false,
      onSyncModeChange: (mode) => modes.push(mode), onSchemaPolicyChange: noop,
      onValidationModeChange: noop, onDateLocaleChange: noop, onNumberLocaleChange: noop,
      onBackfillChange: noop, onStreamCursorChange: noop, onStreamCursorSemanticsChange: noop,
      onStreamPrimaryKeyChange: (_stream, value) => selected.push(value),
      syncHonestyLine: syncModeHonestyLine("full_refresh_append", false, "mysql"),
    })));
    const control = document.querySelector<HTMLSelectElement>('[aria-label="Identity key for sample-orders"]');
    assert.ok(control);
    assert.equal(control.value, "order_id");
    assert.match(control.closest("td")!.textContent || "", /Identity metadata/);
    const cursor = document.querySelector('[aria-label="Cursor field for sample-orders"]')!;
    assert.doesNotMatch(cursor.closest("td")!.textContent || "", /Identity metadata/);
    assert.match(document.body.textContent || "", /plain append creates no PRIMARY KEY/);
    assert.doesNotMatch(document.body.textContent || "", /duplicate id values will not block/);
    control.value = "status";
    await act(async () => control.dispatchEvent(new dom.window.Event("change", { bubbles: true })));
    assert.deepEqual(selected, ["status"]);
    assert.deepEqual(modes, []);
  } finally {
    await act(async () => root.unmount());
    dom.window.close();
  }
});

test("a stale recovered cursor is blank and can be replaced from the loaded schema", async () => {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://localhost/", pretendToBeVisual: true,
  });
  Object.assign(globalThis, {
    window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { DestinationAdvancedDrawer } = await import("./DestinationAdvancedDrawer");
  const root = createRoot(document.getElementById("root")!);
  try {
    await act(async () => root.render(createElement(DestinationAdvancedDrawer, {
      open: true, onClose: () => {}, syncModes: SYNC_MODES, schemaPolicies: SCHEMA_POLICIES,
      validationModes: VALIDATION_MODES, dateLocales: DATE_LOCALES, numberLocales: NUMBER_LOCALES,
      syncMode: "incremental_append", schemaPolicy: "manual_review", validationMode: "strict",
      dateLocale: "", numberLocale: "", backfillNewFields: false,
      streamNames: ["Records"],
      streamFields: { Records: { cursorField: "Records.order_id", primaryKeyField: "" } },
      defaultCursor: "Records.order_id", defaultPrimaryKey: "",
      sourceColumns: ["id", "event_date"], sourceSchema: { id: "INTEGER", event_date: "DATE" },
      syncModeLabel: "Incremental append", schemaPolicyLabel: "Manual review",
      requiresCursor: true, requiresPrimaryKey: false, streamNeedsReview: true,
      onSyncModeChange: () => {}, onSchemaPolicyChange: () => {}, onValidationModeChange: () => {},
      onDateLocaleChange: () => {}, onNumberLocaleChange: () => {}, onBackfillChange: () => {},
      onStreamCursorChange: () => {}, onStreamCursorSemanticsChange: () => {},
      onStreamPrimaryKeyChange: () => {},
      syncHonestyLine: syncModeHonestyLine("incremental_append", false, "mysql"),
    })));
    const cursor = document.querySelector<HTMLSelectElement>('[aria-label="Cursor field for Records"]');
    assert.ok(cursor);
    assert.equal(cursor.value, "");
    assert.deepEqual(Array.from(cursor.options).map((option) => option.value), ["", "id", "event_date"]);
    assert.match(cursor.closest("td")?.textContent || "", /Select cursor/);
  } finally {
    await act(async () => root.unmount());
    dom.window.close();
  }
});

test("append shows a labelled Duplicate rows choice that defaults to insert", async () => {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://localhost/", pretendToBeVisual: true,
  });
  Object.assign(globalThis, {
    window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { DestinationAdvancedDrawer } = await import("./DestinationAdvancedDrawer");
  const root = createRoot(document.getElementById("root")!);
  const chosen: string[] = [];
  const noop = () => {};
  const props = (syncMode: string, duplicateRowsMode: "insert" | "dedupe_skip" | "upsert") => ({
    open: true, onClose: noop, syncModes: SYNC_MODES, schemaPolicies: SCHEMA_POLICIES,
    validationModes: VALIDATION_MODES, dateLocales: DATE_LOCALES, numberLocales: NUMBER_LOCALES,
    syncMode, schemaPolicy: "manual_review", validationMode: "strict",
    dateLocale: "", numberLocale: "", backfillNewFields: false,
    streamNames: [], streamFields: {}, defaultCursor: "", defaultPrimaryKey: "",
    sourceColumns: ["id"], sourceSchema: { id: "INTEGER" },
    syncModeLabel: syncMode, schemaPolicyLabel: "Manual approval", requiresCursor: false,
    requiresPrimaryKey: false, streamNeedsReview: false,
    onSyncModeChange: noop, onSchemaPolicyChange: noop,
    onValidationModeChange: noop, onDateLocaleChange: noop, onNumberLocaleChange: noop,
    onBackfillChange: noop, onStreamCursorChange: noop, onStreamCursorSemanticsChange: noop,
    onStreamPrimaryKeyChange: noop,
    syncHonestyLine: syncModeHonestyLine(syncMode, false, "postgresql"),
    duplicateRowsMode,
    onDuplicateRowsModeChange: (m: string) => chosen.push(m),
  });
  try {
    await act(async () => root.render(createElement(DestinationAdvancedDrawer, props("full_refresh_append", "insert"))));
    const select = document.getElementById("df2-adv-duplicate-rows") as HTMLSelectElement | null;
    assert.ok(select, "append exposes the choice");
    assert.equal(select.value, "insert");
    assert.equal(select.options.length, 3);
    const label = document.querySelector('label[for="df2-adv-duplicate-rows"]');
    assert.equal(label?.textContent, "Duplicate rows");
    const hint = document.getElementById(select.getAttribute("aria-describedby") || "");
    assert.match(hint?.textContent || "", /another copy on every rerun/);
    select.value = "dedupe_skip";
    await act(async () => select.dispatchEvent(new dom.window.Event("change", { bubbles: true })));
    assert.deepEqual(chosen, ["dedupe_skip"]);
    await act(async () => root.render(createElement(DestinationAdvancedDrawer, props("full_refresh_overwrite", "insert"))));
    assert.equal(document.getElementById("df2-adv-duplicate-rows"), null, "overwrite has no duplicate choice");
  } finally {
    await act(async () => root.unmount());
    dom.window.close();
  }
});
