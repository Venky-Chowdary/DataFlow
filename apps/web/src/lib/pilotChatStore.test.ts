/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotChatStore.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { redactSecrets } from "./pilotChatStore.js";

describe("redactSecrets", () => {
  it("masks password in connection URLs", () => {
    const out = redactSecrets("postgres://alice:s3cret@db.example:5432/app");
    assert.match(out, /postgres:\/\/alice:\*\*\*@db\.example/);
    assert.ok(!out.includes("s3cret"));
  });

  it("masks password key-value pairs", () => {
    const out = redactSecrets("host: db\npassword: hunter2\nuser: alice");
    assert.ok(out.includes("password: ***") || out.includes("password:***"));
    assert.ok(!out.includes("hunter2"));
  });

  it("masks bearer tokens", () => {
    const out = redactSecrets("Authorization: Bearer abc.def.ghi");
    assert.match(out, /Bearer \*\*\*/i);
  });
});
