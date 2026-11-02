/**
 * Run: npx --yes tsx --test apps/web/src/lib/sessionActor.test.ts
 *
 * A decision the API records must carry a name. The client cannot mint identity —
 * it can only pass on the operator it signed in as, which is what an
 * enforcement-off single-operator deployment has to name.
 */
import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";

class MemoryStorage {
  private data = new Map<string, string>();
  getItem(key: string): string | null {
    return this.data.has(key) ? (this.data.get(key) as string) : null;
  }
  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
  removeItem(key: string): void {
    this.data.delete(key);
  }
}

const g = globalThis as unknown as { localStorage: MemoryStorage; sessionStorage: MemoryStorage };
g.localStorage = new MemoryStorage();
g.sessionStorage = new MemoryStorage();

const { getSessionActor, writeSession, clearSession } = await import("./session.js");

const SESSION = {
  email: "dana.architect@example.com",
  name: "Dana Architect",
  role: "admin",
  token: "tok",
  expires_at: 0,
  signed_in_at: 0,
};

describe("getSessionActor", () => {
  beforeEach(() => clearSession());

  it("names the signed-in operator by email", () => {
    writeSession(SESSION, true);
    assert.equal(getSessionActor(), "dana.architect@example.com");
  });

  it("is null with no session, so the API decides how to refuse", () => {
    assert.equal(getSessionActor(), null);
  });

  it("treats a session with no email as no session", () => {
    writeSession({ ...SESSION, email: "" }, true);
    assert.equal(getSessionActor(), null);
  });

  it("refuses a name too short to identify anyone", () => {
    writeSession({ ...SESSION, email: "a" }, true);
    assert.equal(getSessionActor(), null);
  });
});
