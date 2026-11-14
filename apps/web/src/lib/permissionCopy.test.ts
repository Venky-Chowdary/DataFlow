import assert from "node:assert/strict";
import { test } from "node:test";

import { permissionFromRefusal, refusalPermissionAttrs, refusalSentence } from "./permissionCopy";

test("a refusal names the action, the role, and who can do it — not the permission id", () => {
  const sentence = refusalSentence("workspace.manage", "viewer");
  assert.match(sentence, /change workspace settings/);
  assert.match(sentence, /you are a viewer in this workspace/);
  assert.match(sentence, /Only workspace admins can do this/);
  // UX-22: the raw id is developer detail (a data attribute), never user copy.
  assert.doesNotMatch(sentence, /workspace\.manage|\(needs /);
  assert.deepEqual(refusalPermissionAttrs("workspace.manage"), { "data-required-permission": "workspace.manage" });
  assert.ok(!sentence.startsWith("Permission denied"));
});

test("a surface can name its own action; the permission id stays out of the sentence", () => {
  const sentence = refusalSentence("connector.write", "viewer", "sign or change contracts");
  assert.match(sentence, /sign or change contracts/);
  assert.doesNotMatch(sentence, /add or change connections/);
  assert.match(sentence, /Only workspace editors and admins can do this/);
  assert.doesNotMatch(sentence, /connector\.write/);
});

test("an unknown permission still reads as a sentence", () => {
  const sentence = refusalSentence("some.future.permission", "");
  assert.match(sentence, /You don't have permission to do this/);
  assert.ok(!sentence.includes("undefined"));
});

test("every gated action a control can take has words of its own", () => {
  for (const permission of [
    "connector.write",
    "job.run",
    "job.plan",
    "schedule.manage",
    "schedule.authorize",
    "workspace.manage",
  ]) {
    assert.ok(
      !refusalSentence(permission, "viewer").includes("to do this"),
      `${permission} falls back to generic copy`,
    );
  }
});

test("the permission is recovered from the gate's own wording", () => {
  assert.equal(permissionFromRefusal("Permission denied: schedule.manage", ""), "schedule.manage");
  // A named permission in the body always wins over the parsed text.
  assert.equal(permissionFromRefusal("Permission denied: schedule.manage", "job.run"), "job.run");
  assert.equal(permissionFromRefusal("Internal server error", ""), "");
});


test("member.invite refusals name the admin role, not editor", () => {
  const sentence = refusalSentence("member.invite", "editor");
  assert.match(sentence, /admin role/);
  assert.doesNotMatch(sentence, /editor role/);
});
