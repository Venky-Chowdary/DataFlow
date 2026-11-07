/**
 * Run: npx --yes tsx --test apps/web/src/lib/session.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  UI_PREVIEW_TOKEN,
  apiBearerToken,
  apiUnauthorizedEndsSession,
  isUiPreviewSession,
  type WorkspaceSession,
} from "./session.js";

function session(token: string): WorkspaceSession {
  return {
    email: "operator@example.com",
    name: "Operator",
    role: "admin",
    token,
    expires_at: 0,
    signed_in_at: 0,
  };
}

describe("UI preview session", () => {
  it("does not present the preview token as an API bearer", () => {
    const preview = session(UI_PREVIEW_TOKEN);
    assert.equal(isUiPreviewSession(preview), true);
    assert.equal(apiBearerToken(preview), null);
    assert.equal(apiUnauthorizedEndsSession(preview), false);
  });

  it("sends a real token and retires that session on 401", () => {
    const real = session("workspace-jwt");
    assert.equal(isUiPreviewSession(real), false);
    assert.equal(apiBearerToken(real), "workspace-jwt");
    assert.equal(apiUnauthorizedEndsSession(real), true);
  });

  it("does not invent a bearer when nobody is signed in", () => {
    assert.equal(apiBearerToken(null), null);
    assert.equal(apiUnauthorizedEndsSession(null), true);
  });
});
