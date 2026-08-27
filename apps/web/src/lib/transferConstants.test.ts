/**
 * Run: npx --yes tsx --test apps/web/src/lib/transferConstants.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  availableSyncModes,
  formatSchemaPolicyLabel,
  formatSyncModeLabel,
  formatValidationModeLabel,
  schemaPolicyHonestyLine,
  syncModeHonestyLine,
} from "./transferConstants.js";

describe("availableSyncModes", () => {
  it("hides SCD2/mirror on Mongo destinations", () => {
    const modes = availableSyncModes({
      destDriver: "mongodb",
      sourceDriver: "postgresql",
      sourceKind: "database",
      isMultiStream: false,
    }).map((m) => m.id);
    assert.ok(!modes.includes("scd2"));
    assert.ok(!modes.includes("mirror"));
    assert.ok(modes.includes("full_refresh_append"));
    assert.ok(modes.includes("cdc"));
  });

  it("hides SCD2/mirror for multi-stream even on Postgres", () => {
    const modes = availableSyncModes({
      destDriver: "postgresql",
      sourceDriver: "postgresql",
      sourceKind: "database",
      isMultiStream: true,
    }).map((m) => m.id);
    assert.ok(!modes.includes("scd2"));
    assert.ok(!modes.includes("mirror"));
    assert.ok(modes.includes("cdc"));
  });

  it("hides CDC for dest stored-procedure / dest query writes", () => {
    const modes = availableSyncModes({
      destDriver: "postgresql",
      sourceDriver: "postgresql",
      sourceKind: "database",
      isMultiStream: false,
      destWriteMode: "procedure",
    }).map((m) => m.id);
    assert.ok(!modes.includes("cdc"));
    assert.ok(!modes.includes("scd2"));
    assert.ok(!modes.includes("mirror"));
    assert.ok(modes.includes("full_refresh_append"));
  });

  it("hides CDC for stored-procedure extracts", () => {
    const modes = availableSyncModes({
      destDriver: "postgresql",
      sourceDriver: "postgresql",
      sourceKind: "database",
      isMultiStream: false,
      sourceReadMode: "procedure",
    }).map((m) => m.id);
    assert.ok(!modes.includes("cdc"));
    assert.ok(!modes.includes("scd2"));
    assert.ok(!modes.includes("mirror"));
    assert.ok(modes.includes("full_refresh_append"));
    assert.ok(modes.includes("incremental_append"));
    assert.ok(modes.includes("incremental_deduped"));
  });

  it("hides CDC for file sources", () => {
    const modes = availableSyncModes({
      destDriver: "postgresql",
      sourceDriver: "",
      sourceKind: "file",
      isMultiStream: false,
    }).map((m) => m.id);
    assert.ok(!modes.includes("cdc"));
  });

  it("keeps SCD2 on SQL single-stream", () => {
    const modes = availableSyncModes({
      destDriver: "postgresql",
      sourceDriver: "mysql",
      sourceKind: "database",
      isMultiStream: false,
    }).map((m) => m.id);
    assert.ok(modes.includes("scd2"));
    assert.ok(modes.includes("mirror"));
  });
});

describe("formatSyncModeLabel", () => {
  it("labels full_refresh_mirror as Mirror, not underscored engine id", () => {
    assert.equal(formatSyncModeLabel("mirror"), "Mirror");
    assert.equal(formatSyncModeLabel("full_refresh_mirror"), "Mirror");
    assert.equal(formatSyncModeLabel("full_refresh_overwrite"), "Full overwrite");
    assert.equal(formatSyncModeLabel(""), "—");
  });

  it("does not reuse sync-mode formatting for schema policy or validation", () => {
    assert.equal(formatSchemaPolicyLabel("manual_review"), "Manual approval");
    assert.equal(formatValidationModeLabel("strict"), "Strict");
  });
});

describe("syncModeHonestyLine", () => {
  it("names leftover empty tables as dest-exists on Full append — no CREATE, no ALTER", () => {
    const line = syncModeHonestyLine("full_refresh_append", true);
    assert.match(line, /existing table/i);
    assert.match(line, /does not CREATE/i);
    assert.match(line, /does not ALTER/i);
    assert.match(line, /empty leftover/i);
  });

  it("names missing tables as create-new on Full append", () => {
    const line = syncModeHonestyLine("full_refresh_append", false);
    assert.match(line, /CREATE TABLE/i);
    assert.match(line, /create-new/i);
  });
});

describe("schemaPolicyHonestyLine", () => {
  it("manual approval does not ADD COLUMN and hard-narrow still pauses", () => {
    const line = schemaPolicyHonestyLine("manual_review");
    assert.match(line, /does not ADD COLUMN/i);
    assert.match(line, /Acknowledge|remap/i);
    assert.match(line, /type-narrow always pauses/i);
  });

  it("propagate adds columns but never silent type rewrite", () => {
    const line = schemaPolicyHonestyLine("propagate_columns");
    assert.match(line, /ADD COLUMN/i);
    assert.match(line, /Type narrow/i);
    assert.doesNotMatch(line, /rewrite destination types/i);
  });

  it("pause on drift includes additive changes", () => {
    const line = schemaPolicyHonestyLine("pause_on_change");
    assert.match(line, /including additive/i);
    assert.match(line, /scheduled beats/i);
  });

  it("type locked refuses silent cast", () => {
    const line = schemaPolicyHonestyLine("type_locked");
    assert.match(line, /silent-cast/i);
  });

  it("propagate_all shares the ADD COLUMN kernel — not a second mapper", () => {
    const line = schemaPolicyHonestyLine("propagate_all");
    assert.match(line, /ADD COLUMN kernel/i);
    assert.match(line, /every selected stream/i);
    assert.match(line, /Hard-breaking/i);
  });
});
