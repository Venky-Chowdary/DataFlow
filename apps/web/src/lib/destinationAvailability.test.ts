/**
 * Run: npx --yes tsx --test apps/web/src/lib/destinationAvailability.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  fileExportUnavailableFromCapabilities,
  localVariantNoteFor,
  localVariantNotesFromCapabilities,
  unavailableDestTypesFromCapabilities,
} from "./destinationAvailability.js";

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

describe("local-disk destinations from /transfer/capabilities (D276, local files)", () => {
  const caps = {
    destination_databases: ["postgresql", "duckdb", "apache_iceberg"],
    destination_file_formats: [],
    destination_file_formats_unavailable: [
      { id: "csv", reason: "Not available on cloud", message: "File exports aren't available as a destination …" },
      { id: "json", reason: "Not available on cloud", message: "File exports aren't available as a destination …" },
    ],
    destination_local_variants_unavailable: [
      { id: "duckdb", engine: "duckdb", variant: "Local file", reason: "Not available on cloud", message: "A local DuckDB file …" },
      { id: "apache_iceberg", engine: "iceberg", variant: "Local file", reason: "Not available on cloud", message: "Iceberg with a local warehouse …" },
    ],
  };

  it("reports file exports as unavailable with the API's reason", () => {
    const out = fileExportUnavailableFromCapabilities(caps);
    assert.equal(out?.reason, "Not available on cloud");
    assert.match(out?.message ?? "", /File exports aren't available/);
  });

  it("keeps DuckDB and Iceberg offered and notes only their local-file variant", () => {
    const notes = localVariantNotesFromCapabilities(caps);
    assert.deepEqual(notes.map((n) => n.id), ["duckdb", "apache_iceberg"]);
    assert.equal(localVariantNoteFor(notes, "duckdb")?.variant, "Local file");
    assert.equal(localVariantNoteFor(notes, "iceberg", "iceberg")?.id, "apache_iceberg");
    assert.equal(localVariantNoteFor(notes, "postgresql", "postgresql"), undefined);
  });

  it("says nothing when the API lists nothing (self-hosted, older API)", () => {
    assert.equal(fileExportUnavailableFromCapabilities({ destination_file_formats: ["csv"] }), null);
    assert.equal(fileExportUnavailableFromCapabilities({ destination_file_formats_unavailable: [] }), null);
    assert.deepEqual(localVariantNotesFromCapabilities(null), []);
  });
});
