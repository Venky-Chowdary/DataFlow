/**
 * Run: npx --yes tsx --test apps/web/src/lib/pilotDataRules.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { mergeNamedDataRules, namedDataRules } from "./pilotDataRules.ts";

describe("pilotDataRules", () => {
  it("does not invent a posture from an empty preview", () => {
    assert.deepEqual(namedDataRules({}), {
      validationMode: "",
      schemaPolicy: "",
    });
    assert.deepEqual(namedDataRules({ skip_preflight: true } as never), {
      validationMode: "",
      schemaPolicy: "",
    });
  });

  it("keeps spoken migrate / type-lock rules from preview then plan", () => {
    const rules = mergeNamedDataRules(
      { validation_mode: "strict", schema_policy: "type_locked" },
      { validation_mode: "balanced" },
    );
    assert.equal(rules.validationMode, "balanced");
    assert.equal(rules.schemaPolicy, "type_locked");
  });

  it("never promotes propagate_all from a missing policy", () => {
    assert.equal(namedDataRules({ validation_mode: "strict" }).schemaPolicy, "");
  });
});
