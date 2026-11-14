/**
 * UX-3: "Schedule" on the run-complete screen created an active daily Full
 * append schedule in one click, re-appending the whole table every day. It now
 * opens a prefilled form that starts paused and warns about a cursorless append.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
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

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { StudioScheduleDialog } = await import("./StudioScheduleDialog.js");
const { appendsWithoutCursor, defaultStudioScheduleDraft } = await import("../../lib/studioScheduleDraft.js");

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

const initial = defaultStudioScheduleDraft("Orders PG", "customers");

function dialogFor(syncMode: string, cursorColumn: string, onCreate: (d: unknown) => void = () => {}) {
  return createElement(StudioScheduleDialog, {
    open: true,
    initial,
    route: { source: "Orders PG · public.customers", destination: "Warehouse · customers" },
    syncMode,
    cursorColumn,
    onCancel: () => {},
    onCreate,
  });
}

describe("studio schedule draft", () => {
  it("starts paused, daily, named after the route", () => {
    assert.deepEqual(initial, { name: "Orders PG → customers", interval: "daily", enabled: false });
  });

  it("flags appends that re-read every row", () => {
    assert.equal(appendsWithoutCursor("full_refresh_append", ""), true);
    assert.equal(appendsWithoutCursor("full_refresh_append", "updated_at"), true);
    assert.equal(appendsWithoutCursor("incremental_append", ""), true);
    assert.equal(appendsWithoutCursor("incremental_append", "updated_at"), false);
    assert.equal(appendsWithoutCursor("full_refresh_overwrite", ""), false);
  });
});

describe("Studio schedule form", () => {
  it("opens prefilled and paused, and warns about a cursorless Full append", async () => {
    await render(dialogFor("full_refresh_append", ""));
    const dialog = document.querySelector('[role="dialog"]')!;
    assert.match(dialog.textContent ?? "", /Orders PG · public\.customers → Warehouse · customers/);
    const warning = dialog.querySelector('[data-testid="studio-schedule-append-warning"]');
    assert.ok(warning, "a cursorless append is called out");
    assert.match(warning!.textContent ?? "", /appends every source row again: the destination grows by the whole table every day/);
    const paused = dialog.querySelector('input[type="checkbox"]') as HTMLInputElement;
    assert.equal(paused.checked, true, "paused by default");
    assert.equal(dialog.querySelector('[data-testid="studio-schedule-create"]')!.textContent, "Create paused schedule");
  });

  it("creates exactly what the form shows", async () => {
    const created: unknown[] = [];
    await render(dialogFor("incremental_append", "updated_at", (d) => created.push(d)));
    assert.equal(document.querySelector('[data-testid="studio-schedule-append-warning"]'), null);
    const select = document.querySelector("select") as HTMLSelectElement;
    await act(async () => {
      select.value = "hourly";
      select.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    await act(async () => {
      (document.querySelector('[data-testid="studio-schedule-create"]') as HTMLButtonElement).click();
    });
    assert.deepEqual(created, [{ name: "Orders PG → customers", interval: "hourly", enabled: false }]);
  });

  it("names the button for what it does when the operator chooses to start it", async () => {
    await render(dialogFor("full_refresh_overwrite", ""));
    const paused = document.querySelector('input[type="checkbox"]') as HTMLInputElement;
    await act(async () => {
      paused.click();
    });
    assert.equal(document.querySelector('[data-testid="studio-schedule-create"]')!.textContent, "Create and start schedule");
  });
});

describe("Transfer Studio wiring", () => {
  const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "pages", "TransferPage.tsx"), "utf8");
  it("no longer creates an enabled daily schedule from the footer", () => {
    assert.doesNotMatch(page, /interval: "daily", enabled: true/);
    const start = page.indexOf("const handleScheduleRoute");
    const chunk = page.slice(start, start + 9000);
    assert.match(chunk, /setScheduleDraft\(\{/);
    assert.doesNotMatch(chunk, /await createSchedule\(/, "the footer only opens the form");
  });
});
