/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioDataRules.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  jobStudioDataRules,
  namedStudioSchemaPolicy,
  namedStudioValidationMode,
  schemaPolicyBackfills,
  studioSchedulePolicies,
  writeViaStagingSupported,
} from "./studioDataRules.ts";

describe("studioDataRules", () => {
  it("does not invent a posture from an empty job", () => {
    assert.deepEqual(jobStudioDataRules({}), {
      validationMode: "",
      schemaPolicy: "",
    });
    assert.equal(namedStudioValidationMode("skip_preflight"), "");
    assert.equal(namedStudioSchemaPolicy(""), "");
  });

  it("restores migrate / type-lock from transfer_request first", () => {
    assert.deepEqual(
      jobStudioDataRules({
        validation_mode: "balanced",
        schema_policy: "manual_review",
        transfer_request: {
          validation_mode: "strict",
          schema_policy: "type_locked",
        },
      }),
      { validationMode: "strict", schemaPolicy: "type_locked" },
    );
  });

  it("accepts migration validation and pause-on-change", () => {
    assert.equal(namedStudioValidationMode("migration"), "migration");
    assert.equal(namedStudioSchemaPolicy("pause_on_change"), "pause_on_change");
    assert.equal(schemaPolicyBackfills("type_locked"), false);
  });

  it("restores propagate_all only when the job actually used it", () => {
    assert.equal(namedStudioSchemaPolicy("propagate_all"), "propagate_all");
    assert.equal(schemaPolicyBackfills("propagate_all"), true);
    assert.equal(jobStudioDataRules({ validation_mode: "strict" }).schemaPolicy, "");
  });

  it("copies Studio migrate / type-lock onto a new pipeline", () => {
    const payload = studioSchedulePolicies({
      validationMode: "strict",
      schemaPolicy: "type_locked",
      backfillNewFields: true,
    });
    assert.equal(payload.validation_mode, "strict");
    assert.equal(payload.schema_policy, "type_locked");
    assert.equal(payload.backfill_new_fields, false);
    assert.equal(payload.write_via_staging, false);
    assert.equal(payload.priority_column, "");
    assert.equal(payload.row_limit, 0);
    assert.equal("skip_preflight" in payload, false);
  });

  it("keeps propagate backfill only when the policy allows ADD COLUMN", () => {
    const payload = studioSchedulePolicies({
      validationMode: "balanced",
      schemaPolicy: "propagate_columns",
      backfillNewFields: true,
    });
    assert.equal(payload.schema_policy, "propagate_columns");
    assert.equal(payload.backfill_new_fields, true);
  });

  it("does not invent a schedule posture from empty Studio fields", () => {
    const payload = studioSchedulePolicies({});
    assert.equal(payload.validation_mode, undefined);
    assert.equal(payload.schema_policy, undefined);
    assert.equal(payload.backfill_new_fields, false);
    assert.equal(payload.write_via_staging, false);
    assert.equal(payload.row_limit, 0);
  });

  it("copies Advanced write knobs onto a scheduled pipeline", () => {
    const payload = studioSchedulePolicies({
      validationMode: "strict",
      schemaPolicy: "propagate_columns",
      backfillNewFields: true,
      writeViaStaging: true,
      priorityColumn: "updated_at",
      priorityDirection: "asc",
      rowLimit: 5000,
    });
    assert.equal(payload.write_via_staging, true);
    assert.equal(payload.priority_column, "updated_at");
    assert.equal(payload.priority_direction, "asc");
    assert.equal(payload.row_limit, 5000);
  });

  it("writeViaStagingSupported matches the SQL staging engine set", () => {
    assert.equal(writeViaStagingSupported("postgresql"), true);
    assert.equal(writeViaStagingSupported("postgres"), true);
    assert.equal(writeViaStagingSupported("mysql"), true);
    assert.equal(writeViaStagingSupported("snowflake"), true);
    assert.equal(writeViaStagingSupported("bigquery"), true);
    assert.equal(writeViaStagingSupported("mongodb"), false);
    assert.equal(writeViaStagingSupported("csv"), false);
    assert.equal(writeViaStagingSupported("s3"), false);
    assert.equal(writeViaStagingSupported(""), false);
  });

  it("stamps CDC snapshot_mode onto a scheduled pipeline and omits it on full refresh", () => {
    const cdc = studioSchedulePolicies({
      syncMode: "cdc",
      snapshotMode: "when_needed",
    });
    assert.equal(cdc.snapshot_mode, "when_needed");
    const full = studioSchedulePolicies({
      syncMode: "full_refresh_overwrite",
      snapshotMode: "when_needed",
    });
    assert.equal(full.snapshot_mode, undefined);
  });
});
