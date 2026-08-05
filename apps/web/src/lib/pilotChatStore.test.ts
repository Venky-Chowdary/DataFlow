/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotChatStore.test.ts
 */
import assert from "node:assert/strict";
import { describe, it, beforeEach } from "node:test";
import {
  extractPilotResultId,
  promoteRailChatToPilotSession,
  redactSecrets,
  saveRailChat,
} from "./pilotChatStore.js";

const memory = new Map<string, string>();

beforeEach(() => {
  memory.clear();
  // Minimal localStorage shim for Node test runner.
  (globalThis as { localStorage?: Storage }).localStorage = {
    getItem: (k: string) => memory.get(k) ?? null,
    setItem: (k: string, v: string) => {
      memory.set(k, v);
    },
    removeItem: (k: string) => {
      memory.delete(k);
    },
    clear: () => memory.clear(),
    key: () => null,
    length: 0,
  } as Storage;
});

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

describe("extractPilotResultId", () => {
  it("pulls the newest pr_ id from sample/query tools", () => {
    const id = extractPilotResultId([
      { name: "list_jobs", success: true, summary: "3 jobs" },
      { name: "sample_connector_object", success: true, summary: "10 rows from orders · pr_abc123" },
      { name: "analyze_result", success: true, summary: "profiled · pr_deadbeef" },
    ]);
    assert.equal(id, "pr_deadbeef");
  });

  it("ignores failed live tools", () => {
    const id = extractPilotResultId([
      { name: "sample_connector_object", success: false, summary: "pr_should_ignore" },
    ]);
    assert.equal(id, undefined);
  });
});

describe("promoteRailChatToPilotSession", () => {
  it("promotes rail history into the Pilot workspace with the same session id", () => {
    const sessionId = "rail-session-wave35";
    saveRailChat({
      sessionId,
      lastResultId: "pr_rail01",
      history: [
        { role: "user", content: "how many orders" },
        { role: "assistant", content: "5" },
      ],
      messages: [
        { role: "user", text: "how many orders" },
        { role: "assistant", text: "There are **5** orders." },
      ],
    });
    const got = promoteRailChatToPilotSession();
    assert.ok(got);
    assert.equal(got!.activeId, sessionId);
    const active = got!.sessions.find((s) => s.id === sessionId);
    assert.ok(active);
    assert.equal(active!.lastResultId, "pr_rail01");
    assert.ok(active!.messages.some((m) => m.text.includes("how many orders")));
  });
});
