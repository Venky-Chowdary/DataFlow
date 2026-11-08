import assert from "node:assert/strict";
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
  MutationObserver: dom.window.MutationObserver, IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });
dom.window.requestAnimationFrame = (callback: FrameRequestCallback) => dom.window.setTimeout(() => callback(0), 0);
dom.window.cancelAnimationFrame = (handle: number) => dom.window.clearTimeout(handle);
let root: Root | null = null;
let host: HTMLElement | null = null;

async function mount() {
  const { act, createElement } = await import("react");
  const { createRoot } = await import("react-dom/client");
  const { ToastProvider, useToast } = await import("./Toast");
  let notify: ReturnType<typeof useToast>["toast"];
  function Emitter() {
    notify = useToast().toast;
    return null;
  }
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  await act(async () => root!.render(createElement(ToastProvider, null, createElement(Emitter))));
  return async (title: string, message = "message") => {
    await act(async () => notify({ title, message, tone: "warning" }));
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 0)); });
  };
}

afterEach(async () => {
  const { act } = await import("react");
  if (root) await act(async () => root!.unmount());
  root = null;
  host = null;
  document.body.replaceChildren();
});

it("keeps a reviewed notification identity when re-emitted after the dedupe window", async () => {
  const notify = await mount();
  await notify("Repeated warning", "Same finding");
  const trigger = document.querySelector<HTMLButtonElement>('[aria-label="View notification details"]')!;
  trigger.focus();
  const { act } = await import("react");
  await act(async () => trigger.click());
  const realNow = Date.now;
  const later = realNow() + 5_000;
  try {
    Date.now = () => later;
    await notify("Repeated warning", "Same finding");
    assert.equal(trigger.isConnected, true);
    await act(async () => document.querySelector<HTMLButtonElement>('[role="dialog"] [aria-label="Close"]')!.click());
    assert.equal(document.activeElement === trigger, true);
  } finally {
    Date.now = realNow;
  }
});

it("never portals notifications into a hidden retained screen", async () => {
  const screen = document.createElement("section");
  screen.hidden = true;
  screen.innerHTML = '<div data-df2-toast-dock=""></div>';
  document.body.appendChild(screen);
  const notify = await mount();
  await notify("Visible warning");
  assert.equal(document.querySelector(".dt-toast-host")?.closest("[hidden]") === null, true);
});

it("moves the toast host when a dock becomes hidden without unmounting", async () => {
  const screen = document.createElement("section");
  screen.innerHTML = '<div data-df2-toast-dock=""></div>';
  document.body.appendChild(screen);
  const notify = await mount();
  await notify("Route warning");
  const { act } = await import("react");
  await act(async () => {
    screen.style.display = "none";
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
  assert.equal(screen.querySelector(".dt-toast-host"), null);
  assert.match(document.querySelector(".dt-toast-host")?.textContent || "", /Route warning/);
});

it("offers full notification details without expanding the preview", async () => {
  const notify = await mount();
  const message = "Migration diagnostics ".repeat(60);
  await notify("Long warning", message);
  const details = document.querySelector<HTMLButtonElement>('[aria-label="View notification details"]');
  assert.ok(details);
  const { act } = await import("react");
  await act(async () => details.click());
  assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /Migration diagnostics/);
  assert.equal(document.querySelector('[role="dialog"]')?.textContent?.includes(message.trim()), true);
});

it("makes dock overflow accessible instead of hiding notification history", async () => {
  const dock = document.createElement("div");
  dock.setAttribute("data-df2-toast-dock", "");
  document.body.appendChild(dock);
  const notify = await mount();
  await notify("First warning");
  await notify("Second warning");
  await notify("Third warning");
  const more = document.querySelector<HTMLButtonElement>("button.dt-toast-more");
  assert.ok(more);
  const { act } = await import("react");
  await act(async () => more.click());
  assert.match(document.querySelector('[role="dialog"]')?.textContent || "", /First warning/);
});

for (const hiddenState of ["hidden", "inert", "aria-hidden", "visibility", "class"]) {
  it(`switches to another visible dock when the retained route uses ${hiddenState}`, async () => {
    const style = document.createElement("style");
    style.textContent = ".toast-test-hidden { display: none; }";
    document.body.appendChild(style);
    const screen = document.createElement("section");
    screen.innerHTML = '<div data-df2-toast-dock=""></div>';
    document.body.appendChild(screen);
    const notify = await mount();
    await notify("Retained route warning");
    assert.ok(screen.querySelector(".dt-toast-host"));
    const otherDock = document.createElement("div");
    otherDock.setAttribute("data-df2-toast-dock", "");
    const { act } = await import("react");
    await act(async () => {
      document.body.appendChild(otherDock);
      if (hiddenState === "visibility") screen.style.visibility = "hidden";
      else if (hiddenState === "class") screen.classList.add("toast-test-hidden");
      else screen.setAttribute(hiddenState, hiddenState === "aria-hidden" ? "true" : "");
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    assert.equal(screen.querySelector(".dt-toast-host"), null);
    assert.match(otherDock.textContent || "", /Retained route warning/);
  });
}

it("bounds the floating stack and reviews every active message", async () => {
  const notify = await mount();
  await notify("First warning", "First recovery step");
  await notify("Second warning", "Second recovery step");
  await notify("Third warning", "Third recovery step");
  assert.equal(document.querySelectorAll(".dt-toast").length, 2);
  const more = document.querySelector<HTMLButtonElement>("button.dt-toast-more")!;
  assert.equal(more.getAttribute("aria-label"), "Review 1 more notifications");
  assert.equal(more.getAttribute("aria-haspopup"), "dialog");
  const { act } = await import("react");
  await act(async () => more.click());
  assert.deepEqual(Array.from(document.querySelectorAll(".dt-toast-review-item p"), node => node.textContent),
    ["Third recovery step", "Second recovery step", "First recovery step"]);
});

it("returns keyboard focus to the details trigger after Escape", async () => {
  const notify = await mount();
  await notify("Keyboard warning");
  const trigger = document.querySelector<HTMLButtonElement>('[aria-label="View notification details"]')!;
  trigger.focus();
  const { act } = await import("react");
  await act(async () => trigger.click());
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
  const dialog = document.querySelector<HTMLElement>('[role="dialog"]')!;
  assert.equal(document.activeElement, dialog);
  await act(async () => {
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true }));
  });
  assert.equal(document.activeElement, dialog.querySelector('[aria-label="Close"]'));
  await act(async () => {
    document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
  });
  assert.equal(document.querySelector('[role="dialog"]'), null);
  assert.equal(document.activeElement, trigger);
});

function controlDismissTimers() {
  const original = window.setTimeout;
  const originalClear = window.clearTimeout;
  const pending = new Map<number, () => void>();
  let nextHandle = 987654;
  window.setTimeout = ((callback: TimerHandler, delay?: number, ...args: unknown[]) => {
    if (delay === 10000) {
      const handle = nextHandle++;
      pending.set(handle, callback as () => void);
      return handle;
    }
    return Reflect.apply(original, window, [callback, delay, ...args]) as number;
  }) as typeof window.setTimeout;
  window.clearTimeout = (handle?: number) => {
    if (handle != null && pending.delete(handle)) return;
    originalClear.call(window, handle);
  };
  return {
    pending,
    expire: () => {
      for (const [handle, callback] of [...pending]) {
        pending.delete(handle);
        callback();
      }
    },
    restore: () => {
      window.setTimeout = original;
      window.clearTimeout = originalClear;
    },
  };
}

for (const close of ["Escape", "Close"]) {
  it(`pauses detail expiry and restores opener focus after ${close}, then resumes expiry`, async () => {
    const timers = controlDismissTimers();
    try {
      const notify = await mount();
      const message = "Full diagnostic\n" + "recovery-step ".repeat(100);
      await notify("Expiring warning", message);
      await notify("Unrelated warning");
      assert.equal(timers.pending.size, 2);
      const trigger = document.querySelector<HTMLButtonElement>('[aria-label="View notification details"]')!;
      trigger.focus();
      const { act } = await import("react");
      await act(async () => trigger.click());
      await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
      assert.equal(document.activeElement, document.querySelector('[role="dialog"]'));
      await act(async () => timers.expire());
      assert.equal(trigger.isConnected, true);
      assert.equal(document.querySelectorAll(".dt-toast").length, 1);
      assert.equal(document.querySelector(".dt-toast-review-item p")?.textContent, message.trim());
      await act(async () => {
        if (close === "Escape") document.dispatchEvent(new dom.window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
        else document.querySelector<HTMLButtonElement>('[role="dialog"] [aria-label="Close"]')!.click();
      });
      assert.equal(document.querySelector('[role="dialog"]'), null);
      assert.equal(document.activeElement, trigger);
      assert.equal(timers.pending.size, 1);
      await act(async () => timers.expire());
      assert.equal(document.querySelectorAll(".dt-toast").length, 0);
    } finally {
      timers.restore();
    }
  });
}

it("keeps the overflow review opener through expiry and restores focus on close", async () => {
  const timers = controlDismissTimers();
  try {
    const notify = await mount();
    await notify("First warning", "First recovery step");
    await notify("Second warning", "Second recovery step");
    await notify("Third warning", "Third recovery step");
    const trigger = document.querySelector<HTMLButtonElement>("button.dt-toast-more")!;
    trigger.focus();
    const { act } = await import("react");
    await act(async () => trigger.click());
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
    assert.equal(document.querySelector<HTMLElement>(".dt-toast-host")?.style.display, "none");
    await act(async () => timers.expire());
    assert.equal(trigger.isConnected, true);
    assert.equal(document.querySelectorAll(".dt-toast").length, 2);
    assert.deepEqual(Array.from(document.querySelectorAll(".dt-toast-review-item p"), node => node.textContent),
      ["Third recovery step", "Second recovery step", "First recovery step"]);
    await act(async () => document.querySelector<HTMLButtonElement>('[role="dialog"] [aria-label="Close"]')!.click());
    assert.equal(document.querySelector('[role="dialog"]'), null);
    assert.notEqual(document.querySelector<HTMLElement>(".dt-toast-host")?.style.display, "none");
    assert.equal(document.activeElement, trigger);
    assert.equal(timers.pending.size, 3);
    await act(async () => timers.expire());
    assert.equal(document.querySelector("button.dt-toast-more"), null);
    assert.equal(document.querySelectorAll(".dt-toast").length, 0);
  } finally {
    timers.restore();
  }
});