/**
 * QA browser 20: the Audit screen showed full API-key UUIDs. The API now masks
 * them for non-admin readers (key_ids_masked); the screen shows every key id
 * as "…last4" for every reader, says who gets them whole, and never rebuilds
 * a full id from a masked one.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, it } from "node:test";
import { KEY_IDS_MASKED_HINT, auditKeyIdHint, maskKeyIdsForDisplay, maskedKeyId } from "./auditKeyIds.js";
import { exportAuditLog, fetchAuditEventPage } from "./api.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const KEY = "7c1e2b9a-4d3f-4e8a-9b1c-2d3e4f5a6b7c";
const WS = "0f0e0d0c-0b0a-4909-8807-060504030201";
const realFetch = globalThis.fetch;
const realWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
function stub(response: () => Response) {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, dispatchEvent: () => true },
  });
  globalThis.fetch = (async () => response()) as typeof fetch;
}
afterEach(() => {
  globalThis.fetch = realFetch;
  if (realWindow) Object.defineProperty(globalThis, "window", realWindow);
  else Reflect.deleteProperty(globalThis, "window");
});

describe("audit key ids (QA browser 20)", () => {
  it("masks a key id wherever a key is named, and nothing else", () => {
    assert.equal(maskedKeyId(KEY), "…6b7c");
    assert.equal(maskKeyIdsForDisplay(`/api/v1/workspace/api-keys/${KEY}`), "/api/v1/workspace/api-keys/…6b7c");
    assert.equal(maskKeyIdsForDisplay(`api_key:${KEY}`), "api_key:…6b7c");
    assert.equal(maskKeyIdsForDisplay(`/api/v1/iam/service-accounts/${KEY}/rotate`), "/api/v1/iam/service-accounts/…6b7c/rotate");
    // Workspace / job ids are not key ids: left exact.
    assert.equal(maskKeyIdsForDisplay(`/team/workspaces/${WS}/members`), `/team/workspaces/${WS}/members`);
    // A server-masked id stays masked; the page never rebuilds it.
    assert.equal(maskKeyIdsForDisplay("api_key:…6b7c"), "api_key:…6b7c");
  });

  it("says who gets full key ids", () => {
    assert.match(auditKeyIdHint(true), /last four/);
    assert.match(auditKeyIdHint(true), /Workspace admins get them in full/);
    assert.match(auditKeyIdHint(false), /your export carries them in full/);
  });

  it("reads the API's masking flag from the event list and the export", async () => {
    stub(() => new Response(JSON.stringify({ events: [{ id: "1", actor: "api_key:…6b7c" }], key_ids_masked: true }), {
      status: 200, headers: { "Content-Type": "application/json" },
    }));
    const page = await fetchAuditEventPage(10);
    assert.equal(page.keyIdsMasked, true);
    assert.equal(page.events[0].actor, "api_key:…6b7c");
    stub(() => new Response("id,actor\n1,api_key:…6b7c\n", { status: 200, headers: { "X-Datawrap-Key-Ids-Masked": "1" } }));
    const out = await exportAuditLog("csv");
    assert.equal(out.keyIdsMasked, true);
    assert.match(await out.blob.text(), /…6b7c/);
  });

  it("the Audit tab masks actor and resource and shows the hint as a note", () => {
    const page = readFileSync(path.join(here, "..", "pages", "SettingsPage.tsx"), "utf8");
    assert.match(page, /actor: maskKeyIdsForDisplay\(auditActorLabel\(ev\)\)/);
    assert.match(page, /resource: maskKeyIdsForDisplay\(ev\.resource\)/);
    assert.match(page, /role="note" data-testid="audit-key-id-hint"/);
    assert.match(page, /KEY_IDS_MASKED_HINT/);
    assert.ok(KEY_IDS_MASKED_HINT.length > 20);
  });
});
