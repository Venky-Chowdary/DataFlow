import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { emptyHistory, historyKey, recordEdit, redoStep, undoStep } from "./mappingHistory";

describe("mapping undo / redo", () => {
  it("undo restores the previous mapping list and redo re-applies it", () => {
    let h = emptyHistory<string[]>();
    let current = ["a"];
    h = recordEdit(h, current);
    current = ["a", "b"];
    h = recordEdit(h, current);
    current = ["a", "b", "c"];

    const u1 = undoStep(h, current)!;
    assert.deepEqual(u1.value, ["a", "b"]);
    const u2 = undoStep(u1.history, u1.value)!;
    assert.deepEqual(u2.value, ["a"]);
    assert.equal(undoStep(u2.history, u2.value), null);

    const r1 = redoStep(u2.history, u2.value)!;
    assert.deepEqual(r1.value, ["a", "b"]);
    const r2 = redoStep(r1.history, r1.value)!;
    assert.deepEqual(r2.value, ["a", "b", "c"]);
    assert.equal(redoStep(r2.history, r2.value), null);
  });

  it("a new edit after undo drops the redo branch", () => {
    let h = recordEdit(emptyHistory<number>(), 1);
    const u = undoStep(h, 2)!;
    h = recordEdit(u.history, u.value);
    assert.equal(h.future.length, 0);
  });

  it("keeps at most the limit", () => {
    let h = emptyHistory<number>();
    for (let i = 0; i < 10; i += 1) h = recordEdit(h, i, 3);
    assert.deepEqual(h.past, [7, 8, 9]);
  });

  it("Ctrl/Cmd+Z undoes, Shift+Ctrl/Cmd+Z and Ctrl+Y redo", () => {
    const k = (key: string, mods: Partial<{ ctrlKey: boolean; metaKey: boolean; shiftKey: boolean; altKey: boolean }>) =>
      historyKey({ key, ctrlKey: false, metaKey: false, shiftKey: false, altKey: false, ...mods });
    assert.equal(k("z", { ctrlKey: true }), "undo");
    assert.equal(k("z", { metaKey: true }), "undo");
    assert.equal(k("Z", { ctrlKey: true, shiftKey: true }), "redo");
    assert.equal(k("z", { metaKey: true, shiftKey: true }), "redo");
    assert.equal(k("y", { ctrlKey: true }), "redo");
    assert.equal(k("z", {}), null);
    assert.equal(k("z", { ctrlKey: true, altKey: true }), null);
  });
});
