import assert from "node:assert/strict";
import { test } from "node:test";

import { permissionFromRefusal, refusalSentence } from "./permissionCopy";

test("a refusal names the action, the role, and the permission", () => {
  const sentence = refusalSentence("workspace.manage", "viewer");
  assert.match(sentence, /change workspace settings/);
  assert.match(sentence, /you are a viewer in this workspace/);
  // The code stays for support, but never leads.
  assert.match(sentence, /\(needs workspace\.manage\)/);
  assert.ok(!sentence.startsWith("Permission denied"));
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
