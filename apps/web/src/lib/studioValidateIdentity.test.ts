/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioValidateIdentity.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  buildValidateContractKey,
  formatValidateIdentitySummary,
  shortHash,
  studioScheduleValidateIdentity,
  validateContractStillHolds,
  type ValidateContractInput,
} from "./studioValidateIdentity.ts";

function base(over: Partial<ValidateContractInput> = {}): ValidateContractInput {
  return {
    syncMode: "full_refresh_append",
    primaryKeyField: "id",
    cursorField: "",
    validationMode: "strict",
    schemaPolicy: "manual_review",
    targetCollection: "jurty",
    destType: "snowflake",
    targetDb: "ANALYTICS",
    destKindMode: "database",
    destSchema: "PUBLIC",
    mappings: [{ source: "amt", target: "amt", transform: "none", destType: "NUMBER(9,6)" }],
    ...over,
  };
}

describe("studioValidateIdentity", () => {
  it("is stable when Advanced knobs do not change", () => {
    const a = buildValidateContractKey(base());
    const b = buildValidateContractKey(base());
    assert.equal(a, b);
    assert.equal(validateContractStillHolds(a, b), true);
  });

  it("invalidates when schema policy or locale changes after a green Validate", () => {
    const green = buildValidateContractKey(base());
    assert.notEqual(green, buildValidateContractKey(base({ schemaPolicy: "propagate_columns" })));
    assert.notEqual(green, buildValidateContractKey(base({ dateLocale: "DMY" })));
    assert.notEqual(green, buildValidateContractKey(base({ numberLocale: "EU" })));
    assert.equal(validateContractStillHolds(green, buildValidateContractKey(base({ dateLocale: "DMY" }))), false);
  });

  it("invalidates when write-via-staging, CDC, or priority change", () => {
    const green = buildValidateContractKey(base());
    assert.notEqual(green, buildValidateContractKey(base({ writeViaStaging: true })));
    assert.notEqual(
      green,
      buildValidateContractKey(base({ syncMode: "cdc", deliveryGuarantee: "exactly_once" })),
    );
    assert.notEqual(green, buildValidateContractKey(base({ snapshotMode: "never" })));
    assert.notEqual(green, buildValidateContractKey(base({ allowAppendOnly: true })));
    assert.notEqual(green, buildValidateContractKey(base({ priorityColumn: "updated_at" })));
    assert.notEqual(green, buildValidateContractKey(base({ priorityDirection: "asc" })));
    assert.notEqual(green, buildValidateContractKey(base({ rowLimit: 1000 })));
    assert.notEqual(green, buildValidateContractKey(base({ cdcRowFilter: "net" })));
    assert.notEqual(green, buildValidateContractKey(base({ multiSubnetFailover: true })));
  });

  it("invalidates when per-stream cursor semantics or recipe identity change", () => {
    const green = buildValidateContractKey(base());
    assert.notEqual(green, buildValidateContractKey(base({ cursorSemantics: "updated_at" })));
    assert.notEqual(
      green,
      buildValidateContractKey(
        base({
          streamFields: { orders: { cursorField: "updated_at", primaryKeyField: "id" } },
        }),
      ),
    );
    assert.notEqual(green, buildValidateContractKey(base({ shapeRecipeHash: "a".repeat(64) })));
    assert.notEqual(
      green,
      buildValidateContractKey(base({ shapeSteps: [{ op: "trim", column: "name" }] })),
    );
    assert.notEqual(green, buildValidateContractKey(base({ schemaDriftAcknowledged: true })));
  });

  it("does not invent create-new from a dest-exists mapping fingerprint", () => {
    const key = buildValidateContractKey(
      base({
        mappings: [{ source: "amt", target: "amt", destType: "NUMBER(9,6)", createNew: false }],
      }),
    );
    assert.match(key, /NUMBER\(9,6\)/);
    assert.doesNotMatch(key, /create-new/);
  });

  it("stamps Validate DDL identity onto a Studio schedule and does not invent hashes", () => {
    const empty = studioScheduleValidateIdentity(undefined);
    assert.equal(empty.approved_decision_artifact_hash, "");
    assert.equal(empty.approved_ddl_identity_hash, "");
    const stamped = studioScheduleValidateIdentity({
      proof_bundle: {
        decision_artifact_hash: "b".repeat(64),
        decision_artifact: { content_hash: "legacy".repeat(8) },
        ddl_identity: { ddl_identity_hash: "c".repeat(64) },
      },
    });
    assert.equal(stamped.approved_decision_artifact_hash, "b".repeat(64));
    assert.equal(stamped.approved_ddl_identity_hash, "c".repeat(64));
    const legacy = studioScheduleValidateIdentity({
      proof_bundle: { decision_artifact: { content_hash: "d".repeat(64) } },
    });
    assert.equal(legacy.approved_decision_artifact_hash, "d".repeat(64));
    assert.equal(legacy.approved_ddl_identity_hash, "");
  });

  it("shortHash truncates without inventing a stamp", () => {
    assert.equal(shortHash(""), "");
    assert.equal(shortHash("   "), "");
    assert.equal(shortHash("abc"), "abc");
    assert.equal(shortHash("a".repeat(64)), `${"a".repeat(8)}…${"a".repeat(4)}`);
  });

  it("formatValidateIdentitySummary does not invent hashes or treat empty as pinned", () => {
    const empty = formatValidateIdentitySummary(undefined);
    assert.equal(empty.pinned, false);
    assert.deepEqual(empty.missing, ["shape", "decision", "ddl"]);
    assert.equal(empty.shapeSteps, 0);
    const stamped = formatValidateIdentitySummary({
      shape_recipe: { steps: [{ op: "trim", column: "name" }] },
      approved_shape_recipe_hash: "a".repeat(64),
      approved_decision_artifact_hash: "b".repeat(64),
      approved_ddl_identity_hash: "c".repeat(64),
    });
    assert.equal(stamped.pinned, true);
    assert.equal(stamped.shapeSteps, 1);
    assert.deepEqual(stamped.missing, []);
    assert.equal(stamped.shapeHash, "a".repeat(64));
  });
});
