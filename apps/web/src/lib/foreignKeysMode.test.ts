import assert from "node:assert/strict";
import test from "node:test";

import {
  FK_ORPHAN_ACK_TEXT,
  FOREIGN_KEYS_MODE_OPTIONS,
  fkOrphansNeedAcknowledgment,
  fkRouteNotices,
  foreignKeysDestExtra,
  normalizeForeignKeysMode,
} from "./foreignKeysMode";

test("request shape: default carry sends nothing, guard/ignore send foreign_keys_mode", () => {
  assert.deepEqual(foreignKeysDestExtra("carry"), {});
  assert.deepEqual(foreignKeysDestExtra(undefined), {});
  assert.deepEqual(foreignKeysDestExtra("guard"), { foreign_keys_mode: "guard" });
  assert.deepEqual(foreignKeysDestExtra("ignore"), { foreign_keys_mode: "ignore" });
  // Never the bare `foreign_keys` key: destination introspection owns it.
  assert.equal("foreign_keys" in foreignKeysDestExtra("guard"), false);
  // Unknown spellings fall back to the default rather than inventing a mode.
  assert.deepEqual(foreignKeysDestExtra("keep"), {});
  assert.equal(normalizeForeignKeysMode("IGNORE"), "ignore");
  assert.equal(normalizeForeignKeysMode("nonsense"), "carry");
});

test("three options, carry first and labelled default", () => {
  assert.deepEqual(FOREIGN_KEYS_MODE_OPTIONS.map((o) => o.id), ["carry", "guard", "ignore"]);
  assert.match(FOREIGN_KEYS_MODE_OPTIONS[0].label, /default/i);
});

test("sample orphans need the explicit acknowledgment; copy says rows are quarantined", () => {
  const blocked = {
    blockers: [
      {
        id: "constraint_fk",
        message:
          "1 distinct sample key(s) reference a parent row the destination does not hold (c.pid -> p.id).",
      },
    ],
  };
  assert.equal(fkOrphansNeedAcknowledgment(blocked), true);
  assert.equal(
    fkOrphansNeedAcknowledgment({ blockers: [{ id: "constraint_fk", message: "FK column unmapped" }] }),
    false,
  );
  assert.equal(fkOrphansNeedAcknowledgment(null), false);
  assert.match(FK_ORPHAN_ACK_TEXT, /quarantined/);
});

test("G22 parent-absent and catalog_probe_budget become readable notices", () => {
  const notices = fkRouteNotices({
    gates: [
      {
        id: "g22_dest_referential_integrity",
        status: "warn",
        message: "Parent table(s) r11par are not at the destination, so the foreign key is not carried.",
      },
      { id: "g22_dest_referential_integrity", status: "skip", message: "post-write proof" },
    ],
    warnings: [
      "plain string warning",
      {
        id: "catalog_probe_budget",
        severity: "warn",
        message: "The physical-storage catalog probe of help_record did not answer within its 10s per-statement catalog budget",
      },
    ],
  });
  assert.deepEqual(
    notices.map((n) => n.id),
    ["fk_parent_absent", "catalog_probe_budget"],
  );
  assert.match(notices[0].title, /not carried/);
  assert.match(notices[1].message, /help_record/);
  assert.deepEqual(fkRouteNotices({}), []);
});
