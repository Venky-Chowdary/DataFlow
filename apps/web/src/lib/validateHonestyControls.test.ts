/**
 * Module 16 — Validate honesty controls must not invent RI / population proof.
 * Run: npx --yes tsx --test apps/web/src/lib/validateHonestyControls.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildConversionClassHonesty,
  buildReferentialIntegrityHonesty,
  buildValidateHonestyControls,
  numberLocaleValidateAction,
  schemaDriftAllowsAcknowledge,
  schemaDriftRequiresRemap,
} from "./validateHonestyControls.ts";
import type { PreflightResult } from "./types.ts";

describe("validateHonestyControls", () => {
  it("never claims RI proven from sample orphan probe alone", () => {
    const preflight = {
      referential_integrity: {
        proven: false,
        coverage: "sample_orphan_probe",
        sample_orphan_probe_ran: true,
        population_orphan_probe_ran: false,
        population_orphan_count: null,
        note: "Sample only",
      },
      proof_bundle: { migration_proven: false },
    } as unknown as PreflightResult;

    const ri = buildReferentialIntegrityHonesty(preflight);
    assert.equal(ri.proven, false);
    assert.equal(ri.sampleRan, true);
    assert.equal(ri.populationRan, false);
    assert.match(ri.headline, /Sample orphan|population RI not proven/i);

    const honesty = buildValidateHonestyControls(preflight, {
      populationScanRequested: false,
    });
    assert.equal(honesty.migrationProven, false);
    assert.equal(honesty.populationScanRequested, false);
    assert.match(honesty.note, /population/i);
  });

  it("surfaces population proven only when engine stamps proven", () => {
    const preflight = {
      referential_integrity: {
        proven: true,
        coverage: "population_orphan_probe",
        sample_orphan_probe_ran: true,
        population_orphan_probe_ran: true,
        population_orphan_count: 0,
        note: "Population orphan detection proven",
      },
      proof_bundle: { migration_proven: false },
    } as unknown as PreflightResult;

    const ri = buildReferentialIntegrityHonesty(preflight);
    assert.equal(ri.proven, true);
    assert.match(ri.headline, /RI proven/i);
  });

  it("summarizes ConversionClass needs_user_approval from proof stamp", () => {
    const preflight = {
      proof_bundle: {
        conversion_contract: {
          version: "conversion_contract.v1",
          columns: [
            {
              source: "amount",
              target: "amount",
              conversion_class: "needs_user_approval",
              invents_capacity: true,
              requires_risk_contract: true,
            },
            {
              source: "id",
              target: "id",
              conversion_class: "lossless",
            },
          ],
        },
        ddl_identity: { ddl_identity_hash: "abc123" },
      },
    } as unknown as PreflightResult;

    const cc = buildConversionClassHonesty(preflight);
    assert.equal(cc.needsApproval, 1);
    assert.equal(cc.lossless, 1);
    assert.match(cc.headline, /Risk Contract|approval/i);

    const honesty = buildValidateHonestyControls(preflight, {
      populationScanRequested: true,
    });
    assert.equal(honesty.populationScanRequested, true);
    assert.equal(honesty.ddlIdentityHash, "abc123");
    assert.equal(honesty.historicalSuccess.measured, false);
    assert.match(honesty.historicalSuccess.headline, /unmeasured/i);
    assert.equal(honesty.historicalSuccess.headline.includes("%"), false);
    assert.equal(honesty.historicalSuccess.hasPercent, false);
  });

  it("surfaces measured historical success without inventing when absent", () => {
    const preflight = {
      proof_bundle: {
        historical_success: {
          measured: true,
          success_rate: 0.97,
          runs_observed: 4,
          rows_written_total: 200,
          rows_rejected_total: 6,
          never_invented: true,
        },
      },
    } as unknown as PreflightResult;
    const honesty = buildValidateHonestyControls(preflight);
    assert.equal(honesty.historicalSuccess.measured, true);
    assert.equal(honesty.historicalSuccess.successRate, 0.97);
    assert.equal(honesty.historicalSuccess.rowsKept, 194);
    assert.equal(honesty.historicalSuccess.rowsRejected, 6);
    assert.match(honesty.historicalSuccess.headline, /97\.0%/);
    assert.equal(honesty.historicalSuccess.hasPercent, true);
  });

  it("never prints a percent when measured is false even if success_rate is 0.99", () => {
    const honesty = buildValidateHonestyControls({
      proof_bundle: {
        historical_success: {
          measured: false,
          success_rate: 0.99,
          runs_observed: 0,
        },
      },
    } as unknown as PreflightResult);
    assert.equal(honesty.historicalSuccess.measured, false);
    assert.equal(honesty.historicalSuccess.successRate, null);
    assert.equal(honesty.historicalSuccess.headline.includes("%"), false);
  });

  it("Phase C12 — Decision Artifact honesty from Validate proof_bundle", () => {
    const hash = "a".repeat(64);
    const preflight = {
      proof_bundle: {
        decision_artifact_hash: hash,
        decision_artifact: {
          schema_version: "decision_artifact_v1",
          content_hash: hash,
        },
      },
    } as unknown as PreflightResult;
    const honesty = buildValidateHonestyControls(preflight);
    assert.equal(honesty.decisionArtifact.present, true);
    assert.equal(honesty.decisionArtifact.contentHash, hash);
    assert.match(honesty.decisionArtifact.headline, /Decision Artifact stamped/i);

    const missing = buildValidateHonestyControls({
      proof_bundle: {},
    } as unknown as PreflightResult);
    assert.equal(missing.decisionArtifact.present, false);
    assert.match(missing.decisionArtifact.headline, /missing/i);
  });

  it("never lets Acknowledge green a hard-breaking schema change", () => {
    assert.equal(
      schemaDriftAllowsAcknowledge({
        remediation_kind: "acknowledge_schema_drift",
        ack_required: true,
        schema_evolution: {
          action: "pause",
          should_pause: true,
          compatibility: "none",
          hard_breaking: [{ kind: "narrow_type", column: "amount" }],
        },
      }),
      false,
    );
    assert.equal(
      schemaDriftRequiresRemap({
        schema_evolution: {
          action: "pause",
          should_pause: true,
          compatibility: "none",
          hard_breaking: [{ kind: "type_change" }],
        },
      }),
      true,
    );
    assert.equal(
      schemaDriftAllowsAcknowledge({
        remediation_kind: "acknowledge_schema_drift",
        ack_required: true,
        schema_evolution: {
          action: "review",
          should_pause: false,
          compatibility: "forward",
          hard_breaking: [],
        },
      }),
      true,
    );
    assert.equal(
      schemaDriftAllowsAcknowledge({
        remediation_kind: "review_mappings",
        schema_evolution: { should_pause: true, compatibility: "none" },
      }),
      false,
    );
  });

  it("number locale set_locale names columns and does not invent ok", () => {
    assert.equal(numberLocaleValidateAction(null), null);
    assert.equal(
      numberLocaleValidateAction({
        number_locale_report: { decision: "ok", ambiguous_columns: [] },
      } as unknown as PreflightResult),
      null,
    );
    const action = numberLocaleValidateAction({
      number_locale_report: {
        decision: "set_locale",
        ambiguous_columns: [{ column: "amount" }, { column: "fee" }],
      },
    } as unknown as PreflightResult);
    assert.ok(action);
    assert.deepEqual(action.columns, ["amount", "fee"]);
    assert.match(action.message, /amount, fee/);
    assert.match(action.message, /Destination → Advanced/);
    assert.match(action.message, /Auto will not guess/);
  });
});
