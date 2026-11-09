import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  APPEND_COLLISION_UNREADABLE_RULE_ID,
  APPEND_KEY_COLLISION_RULE_ID,
  DUPLICATE_ROWS_OPTIONS,
  appendKeyCollisionBlocked,
  duplicateRowsDestExtra,
  normalizeDuplicateRowsMode,
} from "./duplicatePolicy";
import { buildValidateContractKey } from "./studioValidateIdentity";

const collisionGate = {
  id: "g6_target_ddl",
  status: "block",
  message:
    "Append would duplicate 2 existing destination key(s) on id — the destination enforces uniqueness, so the insert aborts.",
  details: { rule_id: APPEND_KEY_COLLISION_RULE_ID, remediation_kind: "change_sync_mode" },
};

describe("duplicate rows on append (Story 2)", () => {
  it("sends duplicate_policy only for a keyed choice on an append route", () => {
    assert.deepEqual(duplicateRowsDestExtra("insert", "full_refresh_append"), {});
    assert.deepEqual(duplicateRowsDestExtra(undefined, "full_refresh_append"), {});
    assert.deepEqual(duplicateRowsDestExtra("dedupe_skip", "full_refresh_append"), {
      duplicate_policy: "dedupe_skip",
    });
    assert.deepEqual(duplicateRowsDestExtra("upsert", "incremental_append"), {
      duplicate_policy: "upsert",
    });
    assert.deepEqual(duplicateRowsDestExtra("upsert", "full_refresh_overwrite"), {});
  });

  it("routes an unreadable enforced-key check to Duplicate rows too", () => {
    const unreadable = { ...collisionGate, details: { rule_id: APPEND_COLLISION_UNREADABLE_RULE_ID } };
    assert.equal(appendKeyCollisionBlocked({ gates: [unreadable] }), true);
    assert.equal(appendKeyCollisionBlocked({ gates: [{ ...unreadable, status: "warn" }] }), false);
  });

  it("accepts the backend aliases", () => {
    assert.equal(normalizeDuplicateRowsMode("skip_duplicates"), "dedupe_skip");
    assert.equal(normalizeDuplicateRowsMode("merge"), "upsert");
    assert.equal(normalizeDuplicateRowsMode("append"), "insert");
  });

  it("says plainly that the default stores another copy on a keyless rerun", () => {
    const insert = DUPLICATE_ROWS_OPTIONS.find((o) => o.id === "insert");
    assert.match(insert?.detail ?? "", /another copy on every rerun/);
    assert.match(insert?.detail ?? "", /refuses the run/);
  });

  it("detects the G6 key-collision block and nothing else", () => {
    assert.equal(appendKeyCollisionBlocked({ gates: [collisionGate] }), true);
    assert.equal(appendKeyCollisionBlocked({ gates: [{ ...collisionGate, status: "warn" }] }), false);
    assert.equal(
      appendKeyCollisionBlocked({
        gates: [{ ...collisionGate, details: { rule_id: "g6_target_ddl.append_key_collision_resolved" } }],
      }),
      false,
    );
    assert.equal(appendKeyCollisionBlocked(null), false);
  });

  it("changing the choice invalidates the validated contract", () => {
    const base = { syncMode: "full_refresh_append", mappings: [] } as unknown as Parameters<
      typeof buildValidateContractKey
    >[0];
    const insert = buildValidateContractKey(base);
    const skip = buildValidateContractKey({ ...base, duplicatePolicy: "dedupe_skip" });
    const upsert = buildValidateContractKey({ ...base, duplicatePolicy: "upsert" });
    assert.notEqual(skip, insert);
    assert.notEqual(upsert, skip);
    assert.equal(buildValidateContractKey({ ...base, duplicatePolicy: undefined }), insert);
  });
});
