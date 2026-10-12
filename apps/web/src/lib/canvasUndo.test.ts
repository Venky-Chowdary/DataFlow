import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createUndoStack, redo, undo, undoPush } from "./canvasUndo.js";

describe("canvas undo", () => {
  it("restores the previous snapshot and the one after it", () => {
    let stack = createUndoStack([{ op: "trim" }]);
    stack = undoPush(stack, [{ op: "trim" }, { op: "filter_rows" }]);
    const undone = undo(stack);
    assert.deepEqual(undone.present, [{ op: "trim" }]);
    const redone = redo(undone);
    assert.equal(redone.present.length, 2);
    assert.equal(redone.present[1].op, "filter_rows");
  });

  it("ignores a commit that did not change the recipe and caps history", () => {
    let stack = createUndoStack(["a"], 2);
    stack = undoPush(stack, ["a"]);
    assert.equal(stack.past.length, 0);
    stack = undoPush(stack, ["b"]);
    stack = undoPush(stack, ["c"]);
    stack = undoPush(stack, ["d"]);
    assert.equal(stack.past.length, 2);
    assert.deepEqual(undo(stack).present, ["c"]);
  });

  it("does not undo an empty stack", () => {
    const stack = createUndoStack([1]);
    assert.equal(undo(stack), stack);
    assert.equal(redo(stack), stack);
  });
});
