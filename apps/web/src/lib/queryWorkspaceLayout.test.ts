/**
 * W10 Query redesign: the editor / results split, the tab "edited since last
 * run" marker, and the error copy the results pane shows for a failed run.
 * Run: npx --yes tsx --test apps/web/src/lib/queryWorkspaceLayout.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { QueryRunError } from "./api";
import {
  clampEditorSplit,
  createTab,
  editedSinceRun,
  MIN_EDITOR_PANE,
  MIN_RESULTS_PANE,
  queryFailureCopy,
} from "./queryWorkspace";

describe("clampEditorSplit", () => {
  it("keeps the wanted height when both panes fit", () => {
    assert.equal(clampEditorSplit(260, 700), 260);
  });

  it("never lets the editor squeeze the results under their minimum", () => {
    assert.equal(clampEditorSplit(900, 600), 600 - MIN_RESULTS_PANE);
  });

  it("never shrinks the editor under its minimum", () => {
    assert.equal(clampEditorSplit(20, 600), MIN_EDITOR_PANE);
    assert.equal(clampEditorSplit(500, 100), MIN_EDITOR_PANE);
  });

  it("passes through before the workspace has been measured", () => {
    assert.equal(clampEditorSplit(300, 0), 300);
  });
});

describe("editedSinceRun", () => {
  it("marks a typed tab that has never run, not an empty one", () => {
    assert.equal(editedSinceRun(createTab()), false);
    assert.equal(editedSinceRun(createTab({ query: "SELECT 1" })), true);
  });

  it("clears once the buffer ran, and returns after an edit", () => {
    assert.equal(editedSinceRun({ query: "SELECT 1", lastRunQuery: "SELECT 1" }), false);
    assert.equal(editedSinceRun({ query: "SELECT 1 ", lastRunQuery: "SELECT 1" }), false);
    assert.equal(editedSinceRun({ query: "SELECT 2", lastRunQuery: "SELECT 1" }), true);
  });

  it("persists through createTab (the store's sanitiser)", () => {
    assert.equal(createTab({ query: "q", lastRunQuery: "q" }).lastRunQuery, "q");
    assert.equal("lastRunQuery" in createTab({ query: "q" }), false);
  });
});

describe("queryFailureCopy", () => {
  it("uses the taxonomy envelope: what, why, next action, reference", () => {
    const err = new QueryRunError(
      'Table "x" does not exist. Check the table name in the query. Reference: 176183fa-cd16',
      400,
      "object.table_not_found",
      {
        code: "object.table_not_found",
        title: "Table not found",
        message: 'Table "x" does not exist.',
        action: { label: "Check the table name in the query.", ui: "edit_query" },
        reference: "176183fa-cd16-464b-aa3f-9139bb9ea740",
      },
    );
    assert.deepEqual(queryFailureCopy(err), {
      title: "Table not found",
      message: 'Table "x" does not exist.',
      action: "Check the table name in the query.",
      reference: "176183fa-cd16-464b-aa3f-9139bb9ea740",
      code: "object.table_not_found",
    });
  });

  it("without an envelope, splits the trailing reference off the text", () => {
    const copy = queryFailureCopy(new Error("Query execution failed. Reference: abcdef123456"));
    assert.equal(copy.title, "Query failed");
    assert.equal(copy.message, "Query execution failed.");
    assert.equal(copy.reference, "abcdef123456");
    assert.equal(copy.action, "");
  });

  it("titles a coded failure from its family", () => {
    const err = new QueryRunError("Can't sign in.", 400, "auth.bad_password", null);
    assert.equal(queryFailureCopy(err).title, "Sign-in rejected");
  });

  it("never comes back empty", () => {
    const copy = queryFailureCopy(undefined);
    assert.equal(copy.title, "Query failed");
    assert.equal(copy.message, "Query failed");
  });
});
