/**
 * Run: npx --yes tsx --test apps/web/src/lib/studioValidateIdentity.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  buildValidateContractKey,
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
});
