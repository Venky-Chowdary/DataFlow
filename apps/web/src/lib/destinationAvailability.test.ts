/**
 * Run: npx --yes tsx --test apps/web/src/lib/destinationAvailability.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { unavailableDestTypesFromCapabilities } from "./destinationAvailability.js";

describe("destination availability from /transfer/capabilities (D276)", () => {
  it("lists SQLite as unavailable with the API's reason when the host refuses it", () => {
    const caps = {
      destination_databases: ["postgresql", "mysql"],
      destination_databases_unavailable: [
        {
          id: "sqlite",
          reason: "Not available on cloud",
          message: "SQLite isn't available as a destination on Datawrap Cloud: …",
        },
      ],
    };
    const out = unavailableDestTypesFromCapabilities(caps);
    assert.equal(out.length, 1);
    assert.equal(out[0].id, "sqlite");
    assert.equal(out[0].reason, "Not available on cloud");
    assert.match(out[0].label, /sqlite/i);
    assert.match(out[0].message ?? "", /isn't available as a destination/);
  });

  it("offers nothing disabled when the API lists nothing (self-hosted, older API)", () => {
    assert.deepEqual(unavailableDestTypesFromCapabilities({ destination_databases: ["sqlite"] }), []);
    assert.deepEqual(unavailableDestTypesFromCapabilities({ destination_databases_unavailable: [] }), []);
    assert.deepEqual(unavailableDestTypesFromCapabilities(null), []);
  });

  it("ignores malformed entries instead of inventing a reason-less engine", () => {
    const out = unavailableDestTypesFromCapabilities({
      destination_databases_unavailable: [null, 3, { reason: "x" }, { id: "" }, { id: "sqlite" }],
    });
    assert.deepEqual(out.map((d) => d.id), ["sqlite"]);
    assert.equal(out[0].reason, "Not available here");
  });
});
