import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { insertTextAtCaret } from "../components/ui/SqlEditor.js";

describe("sql caret insert", () => {
  it("inserts at the caret and replaces a selection", () => {
    assert.deepEqual(insertTextAtCaret("select  from t", 7, 7, "email"), {
      value: "select email from t",
      caret: 12,
    });
    assert.deepEqual(insertTextAtCaret("select name", 7, 11, "email"), {
      value: "select email",
      caret: 12,
    });
  });

  it("appends when the caret is past the end", () => {
    const next = insertTextAtCaret("select ", 100, 100, "id");
    assert.equal(next.value, "select id");
    assert.equal(next.caret, 9);
  });
});
