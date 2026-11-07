/**
 * Owner decision (round 8): Mirror soft-deletes by default; a hard delete is an
 * explicit setting that is announced, in the engine's own words, before the run.
 *
 * Run: npx --yes tsx --test apps/web/src/lib/mirrorDeleteMode.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { destHeadline, readConservationLedger } from "./conservationLedger";
import {
  MIRROR_HARD_DELETE_WARNING,
  MIRROR_LEDGER_HARD_TEXT,
  MIRROR_LEDGER_SOFT_TEXT,
  MIRROR_SOFT_DELETE_TEXT,
  contractsDeleteMode,
  mirrorDeleteText,
  mirrorLedgerText,
  normalizeMirrorDeleteMode,
  preflightHardDeleteWarning,
} from "./mirrorDeleteMode";
import { buildStreamContracts } from "./streamContracts";

const base = {
  streamNames: ["orders"],
  syncMode: "mirror",
  schemaPolicy: "manual_review",
  validationMode: "strict",
  fieldCount: 2,
  requiresCursor: false,
  requiresPrimaryKey: true,
  defaultCursor: "",
  defaultPrimaryKey: "id",
  streamFields: {},
};

describe("mirror delete wording", () => {
  it("soft delete is the default and says rows are kept and flagged _deleted", () => {
    assert.equal(normalizeMirrorDeleteMode(undefined), "soft");
    assert.equal(normalizeMirrorDeleteMode("anything"), "soft");
    assert.equal(mirrorDeleteText("soft"), MIRROR_SOFT_DELETE_TEXT);
    assert.match(MIRROR_SOFT_DELETE_TEXT, /^Soft delete \(default\)/);
    assert.match(MIRROR_SOFT_DELETE_TEXT, /kept and flagged _deleted/);
    assert.doesNotMatch(MIRROR_SOFT_DELETE_TEXT, /physically deleted/);
  });

  it("an explicit hard delete warns that destination rows will be physically deleted", () => {
    assert.equal(normalizeMirrorDeleteMode(" HARD "), "hard");
    assert.equal(mirrorDeleteText("hard"), MIRROR_HARD_DELETE_WARNING);
    assert.match(MIRROR_HARD_DELETE_WARNING, /destination rows .* will be physically deleted/);
    assert.match(MIRROR_HARD_DELETE_WARNING, /cannot be recovered/);
  });

  it("the ledger sentence follows the run's delete mode", () => {
    assert.equal(mirrorLedgerText("hard"), MIRROR_LEDGER_HARD_TEXT);
    assert.equal(mirrorLedgerText(""), MIRROR_LEDGER_SOFT_TEXT);
    const mirror = {
      conservation_kind: "mirror", balanced: true, rows_read: 3, rows_written: 3,
      writer_ack: 3, dest_count: 3, dest_count_before: 3, unaccounted: 0,
      rows_written_source: "gate8_dest_active_readback", active_count: 3,
      inferred_deletes: 1, reactivated: 0, deletes: 1, note: "",
    };
    const hard = readConservationLedger({ row_accounting: { ...mirror, mirror_delete_mode: "hard" } });
    assert.match(destHeadline({ status: "completed", row_accounting: hard }).title || "", /physically deleted/);
    const soft = readConservationLedger({ row_accounting: { ...mirror, mirror_delete_mode: "soft" } });
    assert.match(destHeadline({ status: "completed", row_accounting: soft }).title || "", /stay as _deleted rows/);
  });
});

describe("Studio and schedules carry the declared mode", () => {
  it("Studio sends delete_mode only for an explicit hard delete on a mirror run", () => {
    assert.equal(buildStreamContracts(base)[0].delete_mode, undefined);
    assert.equal(buildStreamContracts({ ...base, mirrorDeleteMode: "soft" })[0].delete_mode, undefined);
    assert.equal(buildStreamContracts({ ...base, mirrorDeleteMode: "hard" })[0].delete_mode, "hard");
    assert.equal(
      buildStreamContracts({ ...base, syncMode: "incremental_deduped", mirrorDeleteMode: "hard" })[0].delete_mode,
      undefined,
    );
  });

  it("a stored schedule contract with delete_mode=hard reads as hard", () => {
    assert.equal(contractsDeleteMode(undefined), "soft");
    assert.equal(contractsDeleteMode([{ name: "orders", sync_mode: "mirror" }]), "soft");
    assert.equal(contractsDeleteMode([{ name: "orders", delete_mode: "hard" }]), "hard");
    assert.equal(contractsDeleteMode([{ name: "orders", delete_mode: "hard", selected: false }]), "soft");
  });

  it("Validate shows the engine's hard-delete warning from the preflight result", () => {
    assert.equal(preflightHardDeleteWarning({ mirror_delete: { mode: "soft", physical_delete: false } }), null);
    assert.equal(
      preflightHardDeleteWarning({
        mirror_delete: { mode: "hard", physical_delete: true, message: MIRROR_HARD_DELETE_WARNING },
      }),
      MIRROR_HARD_DELETE_WARNING,
    );
    assert.equal(
      preflightHardDeleteWarning({ warnings: [{ id: "mirror_hard_delete", message: MIRROR_HARD_DELETE_WARNING }] }),
      MIRROR_HARD_DELETE_WARNING,
    );
    assert.equal(preflightHardDeleteWarning(null), null);
  });
});
