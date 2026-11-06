/**
 * QA browser 2 / security 13: the hash_pii refusal sends a workspace admin to
 * Settings → Security; that card must exist, call the vault-backed routes, and
 * never put key material on screen.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  PII_HASH_KEY_MIN_LENGTH,
  piiHashKeyBadge,
  piiHashKeyInputProblem,
  setPiiHashKey,
} from "./piiHashKeyApi.js";
import { PiiHashKeyCard } from "../pages/settings/PiiHashKeyCard.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const realFetch = globalThis.fetch;
const realWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
function stubWindow() {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, dispatchEvent: () => true },
  });
}
afterEach(() => {
  globalThis.fetch = realFetch;
  if (realWindow) Object.defineProperty(globalThis, "window", realWindow);
  else Reflect.deleteProperty(globalThis, "window");
});

const STATUS = {
  workspace_id: "ws1", configured: true, managed_by_deployment: false, set_by_admin: true,
  active_version: 2, versions: [{ version: 1, retired_at: "2026-11-06" }, { version: 2 }],
};

describe("Settings → Security: hash_pii key", () => {
  it("is on the Security tab", () => {
    const settings = readFileSync(path.join(here, "..", "pages", "SettingsPage.tsx"), "utf8");
    const security = settings.slice(settings.indexOf('{tab === "security" && ('), settings.indexOf('{tab === "auth" && ('));
    assert.ok(security.length > 100);
    assert.match(security, /<PiiHashKeyCard \/>/);
  });

  it("renders the version, the actions and never a key value", () => {
    const html = renderToStaticMarkup(createElement(PiiHashKeyCard));
    assert.match(html, /PII hashing key/);
    assert.match(html, /never shown/);
    assert.match(html, /Rotate key/);
    assert.match(html, /Set my own key/);
    assert.doesNotMatch(html, /DATAFLOW_|DATAWRAP_|PII_HASH_KEY|Railway/);
  });

  it("states the key's version and who manages it", () => {
    assert.equal(piiHashKeyBadge(STATUS), "Active v2 · set by admin");
    assert.equal(piiHashKeyBadge({ ...STATUS, managed_by_deployment: true }), "Managed by deployment");
    assert.equal(piiHashKeyBadge({ ...STATUS, configured: false, set_by_admin: false }), "Created on first use");
  });

  it("checks a typed key the way the API does", () => {
    assert.equal(PII_HASH_KEY_MIN_LENGTH, 32);
    assert.match(piiHashKeyInputProblem("short"), /32/);
    assert.match(piiHashKeyInputProblem(` ${"k".repeat(40)}`), /spaces/);
    assert.equal(piiHashKeyInputProblem("0123456789abcdef0123456789abcdef"), "");
  });

  it("sets the key with PUT and reads back status only", async () => {
    stubWindow();
    const calls: Array<{ url: string; method: string; body: string }> = [];
    globalThis.fetch = (async (url: string, init: RequestInit = {}) => {
      calls.push({ url: String(url), method: String(init.method), body: String(init.body) });
      return new Response(JSON.stringify(STATUS), { status: 200, headers: { "Content-Type": "application/json" } });
    }) as typeof fetch;
    const key = "0123456789abcdef0123456789abcdef-admin";
    const out = await setPiiHashKey(key);
    assert.equal(out.active_version, 2);
    assert.equal(calls.length, 1);
    assert.match(calls[0].url, /\/workspace\/pii-hash-key$/);
    assert.equal(calls[0].method, "PUT");
    assert.deepEqual(JSON.parse(calls[0].body), { key });
  });

  it("surfaces the API's refusal", async () => {
    stubWindow();
    globalThis.fetch = (async () => new Response(
      JSON.stringify({ detail: "A hashing key must be 32–512 characters" }),
      { status: 422, headers: { "Content-Type": "application/json" } },
    )) as typeof fetch;
    await assert.rejects(setPiiHashKey("x".repeat(40)), /32–512/);
  });
});
