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
  });
});
