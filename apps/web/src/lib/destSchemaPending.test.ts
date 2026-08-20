/**
 * Unread destination schema is a catalog gap, not a fidelity verdict.
 *
 * Regression: a Snowflake → MySQL transfer into a table whose schema never
 * loaded showed 10 rows of `VARCHAR(16777216) → VARCHAR(16777216) loses
 * fidelity` behind a Risk Contract that no signature could clear, and the
 * unbounded warning list pushed the mapping grid off screen.
 *
 * Run: npx --yes tsx --test apps/web/src/lib/destSchemaPending.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acknowledgeMappingRisk,
  approveMappingsHonestly,
  countApproveEligible,
  isDestSchemaPending,
  mappingRequiresRiskAck,
  markMappingDestUnread,
  type EditableMapping,
} from "./mapping";
import { destTypeSelectOptions } from "./typeDisplay";
import {
  DEST_PROBE_FAILURE_COOLDOWN_MS,
  DEST_PROBE_TIMEOUT_MS,
  destProbeSpeedClass,
  shouldSkipAutoDestProbe,
} from "./destProbeTimeout";
import { groupMapBlockers, mapBlockerSummary, mappingBlocker } from "./mapBlockers";
import { isMappingReady, mappingTier, needsMappingReview } from "./columnWorkbench";

const THRESHOLD = 0.85;

/** Snowflake column mapped into a MySQL table whose catalog never answered. */
function pendingDestColumn(source: string): EditableMapping {
  return {
    source,
    target: source,
    confidence: 0.55,
    inferredType: "VARCHAR(16777216)",
    destType: "",
    sample: "EMP0000001",
    approved: false,
    assignmentStrategy: "pending_dest_schema",
    requiresReview: true,
    // The engine's fail-closed marker: no destination type was read.
    fidelity: "dest_type_unread",
    fidelityReason: "Destination column type has not been read from the destination yet",
  };
}

/** A real narrowing on a column whose destination type was actually read. */
function lossyExistingDest(): EditableMapping {
  return {
    source: "order_amt",
    target: "order_amount",
    confidence: 0.93,
    inferredType: "DECIMAL(10,4)",
    destType: "INTEGER",
    sample: "12.3456",
    approved: false,
    existsInDestination: true,
    transform: "cast_integer",
    fidelity: "lossy_cast",
    fidelityReason: "DECIMAL(10,4) → INTEGER drops scale 4",
    typeNarrowing: true,
    requiresReview: true,
  };
}

describe("pending destination schema is not a signable risk", () => {
  it("never asks for a Risk Contract", () => {
    const m = pendingDestColumn("employee_id");
    assert.equal(isDestSchemaPending(m), true);
    assert.equal(mappingRequiresRiskAck(m), false);
  });

  it("stays blocked even if Approve or a signature is attempted", () => {
    const m = pendingDestColumn("employee_id");
    assert.equal(mappingTier(m, THRESHOLD), "block");
    assert.equal(isMappingReady({ ...m, approved: true }, THRESHOLD), false);
    const signed = acknowledgeMappingRisk(m, { executionPolicy: "CAST_AND_CONTINUE" });
    assert.equal(isMappingReady({ ...signed, approved: true }, THRESHOLD), false);
    assert.equal(needsMappingReview(m, THRESHOLD), true);
  });

  it("blocks as an unloaded destination schema, never as a type path", () => {
    const blocker = mappingBlocker(pendingDestColumn("employee_id"), THRESHOLD);
    assert.ok(blocker);
    assert.equal(blocker.code, "dest_schema_unloaded");
    assert.equal(blocker.clearableFromMap, false);
    assert.doesNotMatch(blocker.title, /loses fidelity/i);
    // The invented fact: source type printed as the destination type.
    assert.doesNotMatch(blocker.title, /VARCHAR\(16777216\).*VARCHAR\(16777216\)/);
    assert.match(blocker.action, /Reload the destination schema/i);
  });

  it("is never cleared by Approve all", () => {
    // Regression: the Map fallback (mapping engine unreachable) produced rows the
    // bulk Approve path marked ready, unlocking Validate for a destination that
    // had never been read.
    const columns = ["employee_id", "age"].map(pendingDestColumn);
    assert.equal(countApproveEligible(columns), 0);
    for (const m of approveMappingsHonestly(columns)) {
      assert.equal(m.approved, false);
      assert.equal(m.requiresReview, true);
      assert.equal(isMappingReady(m, THRESHOLD), false);
    }
  });
});

describe("the Map warning area stays bounded", () => {
  it("collapses ten pending columns into one cause with one headline", () => {
    const columns = [
      "employee_id",
      "first_name",
      "last_name",
      "department",
      "job_title",
      "age",
      "salary_amount",
      "hire_date",
      "employment_status",
      "work_location",
    ].map(pendingDestColumn);
    const summary = mapBlockerSummary(columns, THRESHOLD);
    assert.equal(summary.blockers.length, 10);
    assert.equal(summary.destSchemaUnloadedOnly, true);
    assert.equal(summary.groups.length, 1);
    assert.equal(summary.groups[0].code, "dest_schema_unloaded");
    assert.equal(summary.groups[0].columns.length, 10);
    assert.match(summary.headline, /Destination schema not loaded/i);
    assert.doesNotMatch(summary.headline, /Risk Contract/i);
  });

  it("keeps distinct causes on distinct lines", () => {
    const groups = groupMapBlockers(
      mapBlockerSummary(
        [pendingDestColumn("employee_id"), pendingDestColumn("age"), lossyExistingDest()],
        THRESHOLD,
      ).blockers,
    );
    const codes = groups.map((g) => g.code).sort();
    assert.deepEqual(codes, ["dest_schema_unloaded", "risk_ack_required"]);
  });
});

describe("marking a row unread erases every destination claim", () => {
  /** What the mapping engine hands back before the destination is ever read. */
  function seededFromSourceOnly(): EditableMapping {
    return {
      source: "employee_id",
      target: "employee_id",
      confidence: 0.92,
      inferredType: "VARCHAR(16777216)",
      // The invented facts: source type echoed as the destination type, plus a
      // provenance line claiming the destination connector was consulted.
      destType: "VARCHAR(16777216)",
      sample: "EMP0000001",
      approved: true,
      createNew: true,
      existsInDestination: false,
      assignmentStrategy: "identity_passthrough",
      requiresReview: false,
      reason: "Inferred from live connector schema",
      fidelity: "preserve",
    };
  }

  it("drops the source-derived destination type, existence and approval", () => {
    const m = markMappingDestUnread(seededFromSourceOnly());
    assert.equal(m.destType, "");
    assert.equal(m.createNew, undefined);
    assert.equal(m.existsInDestination, undefined);
    assert.equal(m.approved, false);
    assert.equal(m.requiresReview, true);
    assert.equal(isDestSchemaPending(m), true);
    assert.equal(isMappingReady({ ...m, approved: true }, THRESHOLD), false);
  });

  it("replaces provenance that claims the destination was read", () => {
    const m = markMappingDestUnread(seededFromSourceOnly());
    assert.doesNotMatch(m.reason ?? "", /live connector schema/i);
    assert.doesNotMatch(m.reason ?? "", /create/i);
    assert.match(m.reason ?? "", /not read/i);
    assert.equal(mappingRequiresRiskAck(m), false);
    assert.equal(mappingBlocker(m, THRESHOLD)?.code, "dest_schema_unloaded");
  });

  it("offers no destination type to pick while the catalog is unread", () => {
    // Regression: passing the source type in as "current" put
    // `VARCHAR(16777216) — current` in the destination dropdown of a table
    // whose columns had never been read.
    const m = markMappingDestUnread(seededFromSourceOnly());
    const options = destTypeSelectOptions(
      isDestSchemaPending(m) && !m.destType ? undefined : m.destType || m.inferredType,
      "mysql",
    );
    assert.equal(
      options.some((o) => /current/i.test(o.label)),
      false,
    );
    assert.equal(
      options.some((o) => o.value === "VARCHAR(16777216)"),
      false,
    );
  });
});

describe("an unanswered destination probe returns the operator to retry", () => {
  it("waits for a warehouse cold start but not for an OLTP engine", () => {
    // A suspended Snowflake warehouse resumes on the first statement, so a slow
    // first answer is legitimate; MySQL either answers a catalog query fast or
    // is unreachable, and a 3-minute wait there left "Reading destination…"
    // disabled with no way to reach the reload control.
    assert.equal(destProbeSpeedClass("snowflake"), "warehouse");
    assert.equal(destProbeSpeedClass("bigquery"), "warehouse");
    assert.equal(destProbeSpeedClass("mysql"), "oltp");
    assert.equal(destProbeSpeedClass("postgresql"), "oltp");
    assert.equal(destProbeSpeedClass(undefined), "oltp");
    assert.ok(
      DEST_PROBE_TIMEOUT_MS.oltp < DEST_PROBE_TIMEOUT_MS.warehouse,
      "an OLTP probe must give up first",
    );
  });

  it("stops automatic probes from re-arming the disabled reload control", () => {
    // A failed probe clears the destination columns, Map re-runs on that state
    // change and probes again: while the host stayed down the control never left
    // "Reading destination…". Automatic probes back off; the operator's do not.
    const key = "conn|mysql|railway|public|Newdata";
    const failedAt = 1_000_000;
    assert.equal(shouldSkipAutoDestProbe(null, key, failedAt), false);
    assert.equal(
      shouldSkipAutoDestProbe({ key, at: failedAt }, key, failedAt + 500),
      true,
    );
    assert.equal(
      shouldSkipAutoDestProbe(
        { key, at: failedAt },
        key,
        failedAt + DEST_PROBE_FAILURE_COOLDOWN_MS + 1,
      ),
      false,
      "the destination must be re-probed once the backoff expires",
    );
    assert.equal(
      shouldSkipAutoDestProbe({ key, at: failedAt }, "conn|mysql|railway|public|Other", failedAt + 500),
      false,
      "another table has its own existence question",
    );
  });
});

describe("a measured lossy path still needs a signed contract", () => {
  it("is unaffected by the pending-schema exemption", () => {
    const m = lossyExistingDest();
    assert.equal(isDestSchemaPending(m), false);
    assert.equal(mappingRequiresRiskAck(m), true);
    assert.equal(isMappingReady({ ...m, approved: true }, THRESHOLD), false);
    const signed = acknowledgeMappingRisk(m, { executionPolicy: "QUARANTINE_ROW" });
    assert.equal(isMappingReady(signed, THRESHOLD), true);
    assert.equal(mappingBlocker(signed, THRESHOLD), null);
  });
});
