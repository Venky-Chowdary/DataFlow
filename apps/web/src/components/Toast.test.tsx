import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { afterEach, it } from "node:test";
import { createRequire } from "node:module";
import type { Root } from "react-dom/client";

const { JSDOM } = createRequire(import.meta.url)("jsdom") as {
  JSDOM: new (html: string, options: { url: string; pretendToBeVisual: boolean }) => {
    window: Window & typeof globalThis;
  };
};
const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/", pretendToBeVisual: true });
Object.assign(globalThis, {
  window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement,
  MouseEvent: dom.window.MouseEvent, Node: dom.window.Node,
  MutationObserver: dom.window.MutationObserver, IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback: FrameRequestCallback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle: number) => dom.window.clearTimeout(handle);
Object.assign(globalThis, { requestAnimationFrame: dom.window.requestAnimationFrame, cancelAnimationFrame: dom.window.cancelAnimationFrame });
let root: Root | null = null;

type Notify = (title: string, message?: string, tone?: "info" | "success" | "warning" | "error") => Promise<void>;

async function settle(ms = 0) {
  const { act } = await import("react");
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, ms)); });
}

async function mount(page?: () => unknown) {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { ToastProvider, useToast } = await import("./Toast");
  let api!: ReturnType<typeof useToast>;
  function Emitter() {
    api = useToast();
    return (page?.() as never) ?? null;
  }
  const host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(createElement(ToastProvider, null, createElement(Emitter))));
  const notify: Notify = async (title, message = "message", tone = "warning") => {
    await act(async () => api.toast({ title, message, tone }));
    await settle(5);
  };
  return { notify, api: () => api, host };
}

/** Toasts on screen (a dismissed one animates out with data-removed="true"). */
const toasts = () => Array.from(document.querySelectorAll<HTMLElement>('[data-sonner-toast]:not([data-removed="true"]) .dt-toast'));

afterEach(async () => {
  const { act } = await import("react");
  const { toast } = await import("sonner");
  await act(async () => { toast.dismiss(); });
  await settle(300);
  if (root) await act(async () => root!.unmount());
  root = null;
  document.body.replaceChildren();
});

it("renders in one notification region outside the page, with the full title and message (P1-4)", async () => {
  const { notify, host } = await mount();
  await notify("Source profiled", "5 rows and 5 columns detected.", "success");
  const [card] = toasts();
  assert.ok(card);
  assert.equal(host.contains(card), false, "never inside the page tree it was raised from");
  const region = card.closest("section");
  assert.match(region?.getAttribute("aria-label") || "", /^Notifications/);
  assert.equal(region?.getAttribute("aria-live"), "polite");
  assert.ok(card.classList.contains("dt-toast--success"));
  assert.equal(card.querySelector(".dt-toast-title")?.textContent, "Source profiled");
  assert.equal(card.querySelector(".dt-toast-message")?.textContent, "5 rows and 5 columns detected.");
  assert.equal(document.querySelector("[data-df2-toast-dock]"), null);
});

it("uses the standard status glyph for every tone (P1-5)", async () => {
  const { notify } = await mount();
  const glyph: Record<string, string> = { info: "lucide-info", success: "lucide-circle-check", warning: "lucide-triangle-alert", error: "lucide-circle-x" };
  for (const tone of ["info", "success", "warning", "error"] as const) {
    await notify(`Tone ${tone}`, "m", tone);
    const card = toasts().find((t) => t.textContent?.includes(`Tone ${tone}`))!;
    assert.ok(card.querySelector(`.dt-toast-icon svg.${glyph[tone]}`), `${tone} uses ${glyph[tone]}`);
    assert.match(card.querySelector(".dt-toast-icon")?.textContent || "", /\w+: /, "the tone is spoken, not only coloured");
  }
});

it("collapses an identical notification into the one already showing", async () => {
  const { notify } = await mount();
  await notify("Repeated warning", "Same finding");
  await notify("Repeated warning", "Same finding");
  await notify("Repeated warning", "Same finding");
  assert.equal(toasts().filter((t) => t.textContent?.includes("Repeated warning")).length, 1);
});

it("dismisses by its close button and by title", async () => {
  const { notify, api } = await mount();
  await notify("First", "one");
  await notify("Second", "two");
  const { act } = await import("react");
  await act(async () => toasts().find((t) => t.textContent?.includes("First"))!.querySelector<HTMLButtonElement>('[aria-label="Dismiss notification"]')!.click());
  await settle(300);
  assert.deepEqual(toasts().map((t) => t.querySelector(".dt-toast-title")?.textContent), ["Second"]);
  await act(async () => api().dismissMatching("Second"));
  await settle(300);
  assert.equal(toasts().length, 0);
});

it("shows at most three; the rest wait behind them", async () => {
  const { notify } = await mount();
  for (const n of [1, 2, 3, 4, 5]) await notify(`Notice ${n}`, "m", "info");
  const visible = Array.from(document.querySelectorAll('[data-sonner-toast][data-visible="true"]'));
  assert.equal(visible.length, 3);
});

it("long text clamps and opens in full under Details; the toast waits and focus returns", async () => {
  const { notify } = await mount();
  const message = "Full diagnostic\n" + "recovery-step ".repeat(30);
  await notify("Expiring warning", message);
  const card = toasts()[0];
  assert.ok(card.querySelector(".dt-toast-message.is-clamped"));
  const details = Array.from(card.querySelectorAll("button")).find((b) => b.textContent === "Details")!;
  assert.ok(details, "a text action, not an icon");
  details.focus();
  const { act } = await import("react");
  await act(async () => details.click());
  await settle(20);
  const dialog = document.querySelector('[role="dialog"]');
  assert.ok(dialog);
  assert.match(dialog.querySelector(".dt-toast-review-item p")?.textContent || "", /^Full diagnostic/);
  assert.equal(toasts().length, 1, "the toast stays while its details are open");
  await act(async () => document.querySelector<HTMLButtonElement>('[role="dialog"] [aria-label="Close"]')!.click());
  await settle(20);
  assert.equal(document.querySelector('[role="dialog"]'), null);
  assert.equal(document.activeElement?.textContent, "Details");
});

it("keeps the reference chip outside the clamped text", async () => {
  const { notify } = await mount();
  await notify("Couldn't load job history", "The server did not answer. Reference: ref_8f2a91", "error");
  const card = toasts()[0];
  assert.doesNotMatch(card.querySelector(".dt-toast-message")?.textContent || "", /ref_8f2a91/);
  assert.match(card.querySelector(".dt-toast-actions")?.textContent || "", /ref_8f2a91/);
});

it("has no body-wide MutationObserver and no dock lookup (P1-5)", () => {
  const source = readFileSync(new URL("./Toast.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(source, /MutationObserver|data-df2-toast-dock|getComputedStyle/);
});
