/**
 * Overwrite recreate SSOT — Map must not treat a doomed live carrier as the write path.
 * Run: npx --yes tsx --test apps/web/src/lib/destSchemaRecreate.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  destSchemaIsRecreatedOnOverwrite,
  liveCarrierIsDoomedOnThisRun,
  stampLiveCarrierDoomed,
} from "./destSchemaRecreate";

describe("destSchemaIsRecreatedOnOverwrite", () => {
  it("matches the Python owner for relational vs CRM dests", () => {
    assert.equal(destSchemaIsRecreatedOnOverwrite("postgresql"), true);
    assert.equal(destSchemaIsRecreatedOnOverwrite("mysql"), true);
    assert.equal(destSchemaIsRecreatedOnOverwrite("salesforce"), false);
    assert.equal(destSchemaIsRecreatedOnOverwrite("hubspot"), false);
    assert.equal(destSchemaIsRecreatedOnOverwrite("mongodb"), false);
    assert.equal(destSchemaIsRecreatedOnOverwrite("minio"), false);
  });
});

describe("liveCarrierIsDoomedOnThisRun", () => {
  it("is true only for dest-exists overwrite that recreates DDL", () => {
    assert.equal(
      liveCarrierIsDoomedOnThisRun({
        syncMode: "full_refresh_overwrite",
        destDbType: "postgresql",
        destTableExists: true,
      }),
      true,
    );
    assert.equal(
      liveCarrierIsDoomedOnThisRun({
        syncMode: "full_refresh_append",
        destDbType: "postgresql",
        destTableExists: true,
      }),
      false,
    );
    assert.equal(
      liveCarrierIsDoomedOnThisRun({
        syncMode: "full_refresh_overwrite",
        destDbType: "salesforce",
        destTableExists: true,
      }),
      false,
    );
    assert.equal(
      liveCarrierIsDoomedOnThisRun({
        syncMode: "full_refresh_overwrite",
        destDbType: "postgresql",
        destTableExists: false,
      }),
      false,
    );
  });
});

describe("stampLiveCarrierDoomed", () => {
  it("stamps only existing dest columns and is a no-op when unchanged", () => {
    const rows = [
      { source: "amt_dec", existsInDestination: true },
      { source: "new_col", existsInDestination: false },
    ];
    const stamped = stampLiveCarrierDoomed(rows, true);
    assert.equal(stamped[0].liveCarrierDoomed, true);
    assert.equal(stamped[1].liveCarrierDoomed, undefined);
    assert.equal(stampLiveCarrierDoomed(stamped, true), stamped);
    const cleared = stampLiveCarrierDoomed(stamped, false);
    assert.equal(cleared[0].liveCarrierDoomed, undefined);
  });
});
