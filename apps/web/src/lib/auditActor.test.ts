/**
 * QA (557f556c): the audit actor column printed full API-key ids. It shows the
 * key's name and a short masked id; a raw machine id never reaches the cell.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { auditActorLabel } from "./auditActor.js";

const ID = "3f2b9c1e-7d4a-4e8b-9a0c-5b6d7e8f9a1b";

describe("auditActorLabel", () => {
  it("prefers the server's display label", () => {
    assert.equal(auditActorLabel({ actor: `api_key:${ID}`, actor_display: "CI deploy (key …9a1b)" }), "CI deploy (key …9a1b)");
  });

  it("masks a raw key id when the server sent no label", () => {
    const label = auditActorLabel({ actor: `api_key:${ID}` });
    assert.equal(label, "API key …9a1b");
    assert.doesNotMatch(label, /3f2b9c1e/);
  });

  it("masks service accounts and SCIM tokens too", () => {
    assert.equal(auditActorLabel({ actor: `service_account:${ID}` }), "Service account …9a1b");
    assert.equal(auditActorLabel({ actor: `scim:${ID}` }), "SCIM token …9a1b");
  });

  it("leaves people and system actors as they are", () => {
    assert.equal(auditActorLabel({ actor: "ops@acme.example" }), "ops@acme.example");
    assert.equal(auditActorLabel({ actor: "" }), "system");
  });
});
