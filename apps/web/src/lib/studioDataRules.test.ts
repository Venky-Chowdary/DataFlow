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
});
