import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { mergeToastStack, toastFingerprint } from "./toastDedupe.ts";

describe("toastFingerprint", () => {
  it("collapses whitespace-identical toasts", () => {
    const a = toastFingerprint({ tone: "info", title: " Cancellation requested ", message: " stop " });
    const b = toastFingerprint({ tone: "info", title: "Cancellation requested", message: "stop" });
    assert.equal(a, b);
  });

  it("keeps different tones distinct", () => {
    const a = toastFingerprint({ tone: "info", title: "Saved" });
    const b = toastFingerprint({ tone: "success", title: "Saved" });
    assert.notEqual(a, b);
  });
});

describe("mergeToastStack", () => {
  it("replaces an identical toast inside the dedupe window and keeps the id", () => {
    const first = { id: "1", key: "k", createdAt: 1000 };
    const second = { id: "2", key: "k", createdAt: 1200 };
    const { items, shownId, replaced } = mergeToastStack([first], second, 2000, 2500, 2);
    assert.equal(replaced, true);
    assert.equal(shownId, "1");
    assert.equal(items.length, 1);
    assert.equal(items[0].id, "1");
    assert.equal(items[0].createdAt, 2000);
  });

  it("caps the visible stack at two", () => {
    const a = { id: "a", key: "a", createdAt: 1 };
    const b = { id: "b", key: "b", createdAt: 2 };
    const c = { id: "c", key: "c", createdAt: 3 };
    const { items } = mergeToastStack([a, b], c, 10_000, 2500, 2);
    assert.deepEqual(items.map((t) => t.key), ["b", "c"]);
  });
});
