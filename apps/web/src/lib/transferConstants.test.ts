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
