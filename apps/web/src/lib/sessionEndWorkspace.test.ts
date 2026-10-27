/**
 * D154: the active workspace id must not outlive the session that chose it.
 *
 * Run: npx --yes tsx --test apps/web/src/lib/sessionEndWorkspace.test.ts
 */
import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";

function memoryStorage() {
  const store = new Map<string, string>();
  return {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
  };
}

Object.defineProperty(globalThis, "localStorage", { configurable: true, value: memoryStorage() });
Object.defineProperty(globalThis, "sessionStorage", { configurable: true, value: memoryStorage() });
Object.defineProperty(globalThis, "window", {
  configurable: true,
  value: {
    setTimeout: globalThis.setTimeout.bind(globalThis),
    clearTimeout: globalThis.clearTimeout.bind(globalThis),
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => true,
  },
});
(globalThis as { CustomEvent?: unknown }).CustomEvent = class CustomEvent {
  type: string;
  detail: unknown;
  constructor(type: string, init?: { detail?: unknown }) {
    this.type = type;
    this.detail = init?.detail;
  }
};

const { fetchWorkspaces } = await import("./api");
const { readSession, writeSession } = await import("./session");
const { getActiveWorkspaceId, setActiveWorkspaceId } = await import("./workspace");

function signIn(email: string) {
  writeSession(
    { email, name: email, role: "member", token: `token-${email}`, expires_at: 0, signed_in_at: Date.now() },
    true,
  );
}

describe("active workspace on session end", () => {
  beforeEach(() => {
    localStorage.removeItem("df2.session");
    localStorage.removeItem("df2.workspace");
  });

  it("a 401 clears df2.workspace along with the session", async () => {
    signIn("alice@example.com");
    setActiveWorkspaceId("ws-alice");
    const sent: string[] = [];
    globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
      sent.push(new Headers(init?.headers).get("X-Workspace-Id") ?? "");
      return new Response(JSON.stringify({ detail: "Session expired" }), { status: 401 });
    }) as typeof fetch;

    await assert.rejects(fetchWorkspaces());
    assert.deepEqual(sent, ["ws-alice"]);
    assert.equal(readSession(), null);
    assert.equal(getActiveWorkspaceId(), "");
    assert.equal(localStorage.getItem("df2.workspace"), null);
  });

  it("a different account signing in does not inherit the workspace", () => {
    signIn("alice@example.com");
    setActiveWorkspaceId("ws-alice");
    signIn("bob@example.com");
    assert.equal(getActiveWorkspaceId(), "");
  });

  it("the same account signing in again keeps its workspace", () => {
    signIn("alice@example.com");
    setActiveWorkspaceId("ws-alice");
    signIn("Alice@Example.com");
    assert.equal(getActiveWorkspaceId(), "ws-alice");
  });
});
