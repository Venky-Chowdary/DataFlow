import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import type { SchemaDriftReport } from "../lib/api";

const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  url: "http://localhost/", pretendToBeVisual: true,
});
Object.assign(globalThis, {
  window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);

const additive: SchemaDriftReport = {
  severity: "additive", additive: [{ kind: "column_added", column: "customer_note", to_type: "TEXT" }],
  breaking: [], compatibility: "full",
};
let root: Root | null = null;
let host: HTMLElement | null = null;

async function mount(overrides: Partial<SchemaDriftReport> = {}) {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { SchemaDriftDialog } = await import("./SchemaDriftDialog");
  const calls = { approve: 0, close: 0, reject: 0, remap: 0 };
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  const props = {
    open: true, report: { ...additive, ...overrides },
    onApproveAdditive: () => { calls.approve += 1; },
    onClose: () => { calls.close += 1; },
    onReject: () => { calls.reject += 1; },
    onRemap: () => { calls.remap += 1; },
  };
  await act(async () => root!.render(createElement(SchemaDriftDialog, props)));
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 0)); });
  return { calls, props, SchemaDriftDialog };
}

function buttons() {
  return Array.from(document.querySelectorAll<HTMLButtonElement>('[role="dialog"] button'));
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
  document.body.replaceChildren();
});

describe("schema drift review safety and keyboard workflow", () => {
  it("preserves and displays rename destination names", async () => {
    const { schemaDriftReportFromDetails } = await import("./SchemaDriftDialog");
    const report = schemaDriftReportFromDetails({ classification: {
      breaking: [{ kind: "rename", column: "legacy_name", to: "customer_name" }],
    } }, "Column renamed");
    assert.equal(report.breaking[0].to, "customer_name");
    await mount(report);
    const text = document.querySelector('[role="dialog"]')?.textContent || "";
    assert.match(text, /legacy_name/);
    assert.match(text, /customer_name/);
  });

  it("restores the confirmation opener despite child autofocus", async () => {
    const { act, createElement } = await import("react");
    const { createRoot } = await import("react-dom/client");
    const { ConfirmProvider, useConfirm } = await import("./ui/ConfirmDialog");
    function Opener() {
      const { confirm } = useConfirm();
      return createElement("button", { onClick: () => { void confirm({ title: "Confirm change" }); } }, "Open confirmation");
    }
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
    await act(async () => root!.render(createElement(ConfirmProvider, null, createElement(Opener))));
    const opener = host.querySelector("button")!;
    opener.focus();
    await act(async () => opener.click());
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 0)); });
    const cancel = buttons().find((button) => button.textContent?.trim() === "Cancel")!;
    await act(async () => cancel.click());
    assert.equal(document.activeElement === opener, true);
  });
  it("normalizes top-level hard evidence and old/new types without hiding them", async () => {
    const { schemaDriftReportFromDetails } = await import("./SchemaDriftDialog");
    const report = schemaDriftReportFromDetails({
      hard_breaking: [{ kind: "narrow_type", column: "amount" }],
      compatibility: "none",
      schema_evolution: { hard_breaking: [], compatibility: "full" },
      additive: [{ kind: "widen_type", column: "name", old_type: "VARCHAR(32)", new_type: "VARCHAR(64)" }],
    }, "Schema changed");
    assert.equal(report.hard_breaking?.length, 1);
    assert.equal(report.compatibility, "none");
    assert.equal((report.additive[0] as Record<string, unknown>).from_type, "VARCHAR(32)");
    await mount(report);
    assert.equal(buttons().some((button) => /Approve additive/.test(button.textContent || "")), false);
  });
  for (const [name, overrides] of [
    ["nested hard changes despite an empty top-level list", {
      hard_breaking: [], schema_evolution: { hard_breaking: [{ kind: "type_narrowed", column: "amount" }] },
    }],
    ["nested incompatibility", { compatibility: undefined, schema_evolution: { compatibility: "none" } }],
    ["explicit pause", { schema_evolution: { should_pause: true } }],
    ["unclassified hard breaking kind", { breaking: [{ kind: "narrow_type", column: "amount" }] }],
    ["unknown breaking kind", { breaking: [{ kind: "future_unsafe_change", column: "amount" }] }],
  ] as [string, Partial<SchemaDriftReport>][]) {
    it(`does not offer additive approval for ${name}`, async () => {
      await mount(overrides);
      assert.equal(buttons().some((button) => /Approve additive/i.test(button.textContent || "")), false);
      assert.ok(buttons().some((button) => /remap/i.test(button.textContent || "")));
    });
  }

  it("approves safe additive changes through the existing callback", async () => {
    const { calls } = await mount();
    const approve = buttons().find((button) => /Approve additive/i.test(button.textContent || ""));
    assert.ok(approve);
    approve.click();
    assert.equal(calls.approve, 1);
    assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /customer_note/);
  });

  it("keeps current and proposed types visible on hard-only findings", async () => {
    const { schemaDriftReportFromDetails } = await import("./SchemaDriftDialog");
    const report = schemaDriftReportFromDetails({ schema_evolution: {
      hard_breaking: [{ kind: "narrow_type", column: "amount", old_type: "DECIMAL(18,2)", new_type: "TEXT" }],
    } }, "Type changed");
    await mount(report);
    const text = document.querySelector('[role="dialog"]')?.textContent || "";
    assert.match(text, /DECIMAL\(18,2\)/);
    assert.match(text, /TEXT/);
  });

  it("dismisses with Escape", async () => {
    const { calls } = await mount();
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    assert.equal(calls.close, 1);
  });

  it("keeps keyboard focus inside the review", async () => {
    await mount();
    const controls = buttons();
    controls.at(-1)!.focus();
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Tab", bubbles: true }));
    assert.equal(document.activeElement === controls[0], true);
    controls[0].focus();
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Tab", shiftKey: true, bubbles: true }));
    assert.equal(document.activeElement === controls.at(-1), true);
  });

  it("uses the visible summary rather than controls inside collapsed details", async () => {
    await mount();
    const panel = document.querySelector('[role="dialog"]')!;
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.tabIndex = 0;
    summary.textContent = "Fields";
    const hidden = document.createElement("button");
    hidden.textContent = "More fields";
    details.append(summary, hidden);
    panel.appendChild(details);
    const first = buttons()[0];
    first.focus();
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Tab", shiftKey: true, bubbles: true }));
    assert.equal(document.activeElement === summary, true);
  });

  for (const [label, callback] of [["Reject", "reject"], ["Open remapping", "remap"], ["Cancel", "close"]] as const) {
    it(`dispatches only the ${callback} decision for ${label}`, async () => {
      const { calls } = await mount();
      const button = buttons().find((candidate) => candidate.textContent?.trim() === label)!;
      button.click();
      assert.equal(calls[callback], 1);
      assert.equal(Object.values(calls).reduce((total, value) => total + value, 0), 1);
    });
  }

  it("returns focus to the control that opened the review", async () => {
    const opener = document.createElement("button");
    document.body.appendChild(opener);
    opener.focus();
    const { props, SchemaDriftDialog } = await mount();
    assert.equal(document.querySelector('[role="dialog"]')?.contains(document.activeElement), true);
    const { act, createElement } = await import("react");
    await act(async () => root!.render(createElement(SchemaDriftDialog, { ...props, open: false })));
    assert.equal(document.activeElement === opener, true);
  });

  it("does not offer remapping when no remapping action exists", async () => {
    const { props, SchemaDriftDialog } = await mount();
    const { act, createElement } = await import("react");
    await act(async () => root!.render(createElement(SchemaDriftDialog, { ...props, onRemap: undefined })));
    assert.equal(buttons().some((button) => /Open remapping/.test(button.textContent || "")), false);
  });

  it("locks background scrolling and restores it after the review closes", async () => {
    const previous = document.body.style.overflow;
    const { props, SchemaDriftDialog } = await mount();
    assert.equal(document.body.style.overflow, "hidden");
    const { act, createElement } = await import("react");
    await act(async () => root!.render(createElement(SchemaDriftDialog, { ...props, open: false })));
    assert.equal(document.body.style.overflow, previous);
  });
});