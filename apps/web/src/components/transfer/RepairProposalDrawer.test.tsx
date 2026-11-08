import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import type { Root } from "react-dom/client";
import type { RepairProposal } from "../../lib/api";

const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  url: "http://localhost/", pretendToBeVisual: true,
});
Object.assign(globalThis, {
  window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
  localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle) => dom.window.clearTimeout(handle);
const requests: Array<{ url: string; resolve: (response: Response) => void }> = [];
globalThis.fetch = ((input: RequestInfo | URL) => new Promise<Response>((resolve) => {
  requests.push({ url: String(input), resolve });
})) as typeof fetch;
let root: Root | null = null;
let host: HTMLElement | null = null;
const proposal = (id: string): RepairProposal => ({
  id, status: "proposed", source: "preflight", summary: "Review mapping transform",
  actions: [{ kind: "add_transform", column: "amount", transform: "strip_controls" }],
});

async function render(value: RepairProposal, applied: string[], closed: string[], open = true, identity?: () => void, attached = true, decided?: string[]) {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { RepairProposalDrawer } = await import("./RepairProposalDrawer");
  if (!root) {
    host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(createElement(RepairProposalDrawer, {
    open, proposal: value, mappings: attached ? [{ source: "amount", destination: "amount" }] : [],
    onClose: () => closed.push(value.id), onApplied: (_maps, decided) => applied.push(decided.id),
    onOpenIdentitySettings: identity,
    onDecided: (value) => decided?.push(value.id),
  })));
}

function applyButton() {
  return Array.from(document.querySelectorAll<HTMLButtonElement>("button"))
    .find((button) => /Approve & apply/.test(button.textContent || ""))!;
}

async function answer(request: typeof requests[number], result: object, status = 200) {
  const { act } = await import("react");
  await act(async () => {
    request.resolve(new Response(JSON.stringify(result), { status }));
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  root = null;
  host?.remove();
  host = null;
  for (const request of requests) request.resolve(new Response("{}", { status: 500 }));
  requests.length = 0;
});

describe("human-gated repair decision isolation", () => {
  it("applies verified matching results once and closes", async () => {
    const applied: string[] = [], closed: string[] = [];
    await render(proposal("a"), applied, closed);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    await answer(requests[0], { ...proposal("a"), status: "applied", apply_result: {
      applied: true, mappings: [{ source: "amount", destination: "amount", transform: "strip_controls" }],
    } });
    assert.deepEqual(applied, ["a"]);
    assert.deepEqual(closed, ["a"]);
  });

  for (const result of [
    { ...proposal("a"), status: "applied", apply_result: { applied: true, mappings: [] } },
    { ...proposal("other"), status: "applied", apply_result: { applied: true, mappings: [{ source: "amount" }] } },
  ]) {
    it(`does not accept ${result.id === "a" ? "empty applied mappings" : "a mismatched response identity"}`, async () => {
      const applied: string[] = [], closed: string[] = [];
      await render(proposal("a"), applied, closed);
      const { act } = await import("react");
      await act(async () => applyButton().click());
      await answer(requests[0], result);
      assert.deepEqual(applied, []);
      assert.deepEqual(closed, []);
      assert.ok(document.querySelector('[role="alert"]'));
    });
  }

  it("continues without an audit request when mappings are not attached", async () => {
    const closed: string[] = [], decided: string[] = [];
    await render(proposal("a"), [], closed, true, undefined, false, decided);
    const button = Array.from(document.querySelectorAll<HTMLButtonElement>("button"))
      .find((candidate) => candidate.textContent?.trim() === "Continue in Validate")!;
    const { act } = await import("react");
    await act(async () => button.click());
    assert.equal(requests.length, 0);
    assert.deepEqual(decided, ["a"]);
    assert.deepEqual(closed, ["a"]);
  });
  it("keeps eligible mapping changes available beside duplicate-key guidance", async () => {
    await render({ ...proposal("mixed"), actions: [
      { kind: "fix_source_keys", column: "id", mapping_applyable: false },
      { kind: "change_target_type", column: "amount", to_type: "DECIMAL(20,2)", apply_proven: true },
    ] }, [], [], true, () => {});
    assert.ok(applyButton());
    assert.equal(applyButton().disabled, false);
  });

  for (const flags of [{ requires_ddl: true }, { apply_proven: false }, { mapping_applyable: false }]) {
    it(`does not apply a type change excluded by ${Object.keys(flags)[0]}`, async () => {
      await render({ ...proposal("guidance"), actions: [{ kind: "change_target_type", column: "amount", to_type: "TEXT", ...flags }] }, [], []);
      assert.equal(applyButton(), undefined);
    });
  }

  it("does not dismiss a pending decision through Escape", async () => {
    const closed: string[] = [];
    await render(proposal("a"), [], closed);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    await act(async () => document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true })));
    assert.deepEqual(closed, []);
  });
  it("labels proposed changes and manual steps without claiming unknown proof", async () => {
    await render({ ...proposal("presentation"), actions: [
      { kind: "change_target_type", column: "amount", to_type: "BIGINT", apply_proven: "true" },
      { kind: "add_transform", source: "notes", transform: "strip_controls", apply_proven: true },
      { kind: "fix_source_keys", column: "id", apply_proven: false },
      { kind: "future_action", label: "Review destination contract" },
    ] }, [], []);
    const changes = Array.from(document.querySelectorAll(".df2-repair-actions li"));
    assert.match(changes[0].textContent || "", /Change target type.*Columnamount.*Proposed changeBIGINT.*EvidenceUnverified/);
    assert.match(changes[1].textContent || "", /Add transform.*Columnnotes.*Proposed transformstrip_controls.*EvidenceProven against scanned values/);
    assert.match(changes[2].textContent || "", /Fix source keys.*Manual step\. Not applied by this proposal\..*EvidenceNot proven/);
    assert.match(changes[3].textContent || "", /Review proposed step.*EvidenceUnverified/);
    assert.doesNotMatch(document.body.textContent || "", /change_target_type|add_transform|fix_source_keys|future_action|Each type was proven/);
    assert.equal(document.querySelector(".df2-repair-status")?.textContent, "Statusproposed");
    assert.equal(requests.length, 0);
  });

  it("issues one apply request for rapid activation", async () => {
    await render(proposal("a"), [], []);
    const button = applyButton();
    const { act } = await import("react");
    await act(async () => { button.click(); button.click(); });
    assert.equal(requests.length, 1);
  });

  it("does not apply an old response after selecting another proposal", async () => {
    const applied: string[] = [];
    const closed: string[] = [];
    await render(proposal("a"), applied, closed);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    const old = requests[0];
    await render(proposal("b"), applied, closed);
    await answer(old, { ...proposal("a"), status: "applied", apply_result: {
      applied: true, mappings: [{ source: "amount", destination: "amount", transform: "strip_controls" }],
    } });
    assert.deepEqual(applied, []);
    assert.deepEqual(closed, []);
  });

  it("does not revive an old response after closing and reopening the same proposal", async () => {
    const applied: string[] = [];
    const closed: string[] = [];
    await render(proposal("a"), applied, closed);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    await render(proposal("a"), applied, closed, false);
    await render(proposal("a"), applied, closed, true);
    await answer(requests[0], { ...proposal("a"), status: "applied", apply_result: {
      applied: true, mappings: [{ source: "amount", destination: "amount", transform: "strip_controls" }],
    } });
    assert.deepEqual(applied, []);
    assert.deepEqual(closed, []);
  });

  it("does not carry an error into a different proposal", async () => {
    await render(proposal("a"), [], []);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    await answer(requests[0], { detail: "Old proposal failed" }, 500);
    assert.match(document.body.textContent || "", /Old proposal failed/);
    assert.equal(document.activeElement === document.querySelector('[role="alert"]'), true);
    assert.equal(document.querySelector('[role="dialog"]')?.getAttribute("aria-label"), "Repair proposal");
    await render(proposal("b"), [], []);
    assert.doesNotMatch(document.body.textContent || "", /Old proposal failed/);
  });

  it("does not treat a truthy non-boolean apply result as applied", async () => {
    const applied: string[] = [];
    await render(proposal("a"), applied, []);
    const { act } = await import("react");
    await act(async () => applyButton().click());
    await answer(requests[0], { ...proposal("a"), status: "approved", apply_result: {
      applied: "true", mappings: [{ source: "amount", destination: "amount" }],
    } });
    assert.deepEqual(applied, []);
  });
});