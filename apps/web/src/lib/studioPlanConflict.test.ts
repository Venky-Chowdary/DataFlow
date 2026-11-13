import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { EditableMapping } from "./mapping.js";
import {
  approveDecision,
  conflictMineFromEditor,
  editorFromLatestRevision,
  planConflictNoteReducer,
} from "./studioPlanConflict.js";

const editorRow = (over: Partial<EditableMapping>): EditableMapping =>
  ({ source: "x", target: "x", confidence: 0.9, approved: true, ...over }) as EditableMapping;

describe("conflictMineFromEditor (what Keep mine saves)", () => {
  it("keeps omits, transforms and overrides exactly as Validate would save them", () => {
    const mine = conflictMineFromEditor([
      editorRow({ source: "ssn", target: "ssn", transform: "omit" }),
      editorRow({ source: "phone", target: "phone", transform: "phone" }),
    ]);
    const ssn = mine.find((m) => m.source === "ssn")!;
    assert.equal(ssn.target, "", "an omitted column is not written");
    assert.equal(ssn.intentional_omit, true);
    assert.equal(ssn.transform, "omit");
    const phone = mine.find((m) => m.source === "phone")!;
    assert.equal(phone.target, "phone");
    assert.ok(phone.transform, "the transform travels");
    assert.equal(phone.user_override, true, "an approved row carries the operator's decision");
  });
});

describe("editorFromLatestRevision (Use latest)", () => {
  const ctx = { threshold: 0.75 };

  it("keeps operator decisions so the next regeneration does not re-propose them", () => {
    const rows = editorFromLatestRevision(
      [
        { source: "cust_email", target: "contact_email", confidence: 0.6, user_override: true },
        { source: "ssn", target: "", transform: "omit", intentional_omit: true, omit_reason: "pii" },
        { source: "id", target: "id", confidence: 0.99 },
      ],
      ctx,
    );
    const email = rows.find((r) => r.source === "cust_email")!;
    assert.equal(email.target, "contact_email");
    assert.equal(email.operatorTarget, true);
    assert.equal(email.approved, true);
    const ssn = rows.find((r) => r.source === "ssn")!;
    assert.equal(ssn.transform, "omit");
    assert.equal(ssn.approved, true);
    assert.equal(ssn.omitReason, "pii");
    const id = rows.find((r) => r.source === "id")!;
    assert.notEqual(id.operatorTarget, true, "an engine proposal stays an engine proposal");
  });

  it("round-trips: what Keep mine would save from these rows is the revision's decision", () => {
    const rows = editorFromLatestRevision([{ source: "a", target: "b", confidence: 0.8, user_override: true }], ctx);
    const again = conflictMineFromEditor(rows);
    assert.equal(again[0].target, "b");
    assert.equal(again[0].user_override, true);
  });
});

describe("approveDecision", () => {
  it("approves only the revision this page saved and Validate checked", () => {
    assert.deepEqual(approveDecision(4, 4), { approve: true, expectedVersion: 4 });
    // Page saved v2, another user saved v3 before Validate read the plan: v3 was checked.
    assert.deepEqual(approveDecision(3, 2), { approve: false, expectedVersion: null });
    assert.deepEqual(approveDecision(undefined, 2), { approve: false, expectedVersion: null });
    assert.deepEqual(approveDecision(2, null), { approve: false, expectedVersion: null });
  });
});

describe("planConflictNoteReducer", () => {
  it("shows the note on a conflict and clears it when it no longer applies", () => {
    let note = planConflictNoteReducer("", { kind: "conflict", note: "Validate did not run" });
    assert.equal(note, "Validate did not run");
    for (const kind of ["validate-started", "validate-succeeded", "plan-changed", "resolved"] as const) {
      assert.equal(planConflictNoteReducer(note, { kind }), "", kind);
    }
    note = planConflictNoteReducer(note, { kind: "conflict", note: "again" });
    assert.equal(note, "again");
  });
});
