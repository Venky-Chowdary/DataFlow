/**
 * Last-admin predicate — must match ``team_store._assert_not_last_admin``.
 * Run: npx --yes tsx --test apps/web/src/lib/workspaceAdmin.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { isLastWorkspaceAdmin, workspaceAdmins } from "./workspaceAdmin";

const members = [
  { email: "a@example.com", role: "admin" },
  { email: "e@example.com", role: "editor" },
  { email: "v@example.com", role: "viewer" },
];

describe("isLastWorkspaceAdmin", () => {
  it("protects the only admin, including case-folded email", () => {
    assert.equal(isLastWorkspaceAdmin(members, "a@example.com"), true);
    assert.equal(isLastWorkspaceAdmin(members, "A@Example.com"), true);
    assert.equal(isLastWorkspaceAdmin(members, "e@example.com"), false);
  });

  it("releases once a second admin exists", () => {
    const two = [...members, { email: "b@example.com", role: "admin" }];
    assert.equal(isLastWorkspaceAdmin(two, "a@example.com"), false);
    assert.equal(workspaceAdmins(two).length, 2);
  });

  it("does not invent an admin from an empty roster", () => {
    assert.equal(isLastWorkspaceAdmin([], "a@example.com"), false);
  });
});
