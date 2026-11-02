/**
 * Run: npx --yes tsx --test apps/web/src/lib/contractBind.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  contractBindBlocksRun,
  contractBindFromPolicies,
  contractBindFromPreview,
  contractMatchesRoute,
  isSignedContractStatus,
} from "./contractBind.js";

describe("contractBind", () => {
  it("does not block when no contract is bound", () => {
    assert.equal(contractBindBlocksRun({ contractId: "", requireSigned: false }), "");
  });

  it("blocks require-signed with no selection", () => {
    assert.match(
      contractBindBlocksRun({ contractId: "", requireSigned: true }),
      /no contract is selected/,
    );
  });

  it("blocks a draft when require-signed is on", () => {
    assert.match(
      contractBindBlocksRun({
        contractId: "c1",
        requireSigned: true,
        selectedStatus: "DRAFT",
      }),
      /not SIGNED/,
    );
  });

  it("allows a signed bind", () => {
    assert.equal(
      contractBindBlocksRun({
        contractId: "c1",
        requireSigned: true,
        selectedStatus: "SIGNED",
      }),
      "",
    );
    assert.equal(isSignedContractStatus("signed"), true);
    assert.equal(isSignedContractStatus("DRAFT"), false);
  });

  it("reads plan policies and defaults require-signed when an id is set", () => {
    assert.deepEqual(contractBindFromPolicies({}), {
      contractId: "",
      requireSigned: false,
    });
    assert.deepEqual(
      contractBindFromPolicies({ contract_id: "c1" }),
      { contractId: "c1", requireSigned: true },
    );
    assert.deepEqual(
      contractBindFromPolicies({
        contract_id: "c1",
        require_signed_contract: false,
      }),
      { contractId: "c1", requireSigned: false },
    );
  });

  it("reads the Pilot Confirm preview bind", () => {
    assert.deepEqual(contractBindFromPreview(null), {
      contractId: "",
      requireSigned: false,
      breakerState: "",
    });
    assert.deepEqual(
      contractBindFromPreview({
        contract_id: "dfc-1",
        require_signed_contract: true,
        breaker_state: "closed",
      }),
      { contractId: "dfc-1", requireSigned: true, breakerState: "closed" },
    );
  });

  it("keeps signed contracts for the current source and destination", () => {
    const route = {
      source: { format: "csv", filename: "sample-orders.csv" },
      destination: { format: "postgresql", table: "QA_E2E_ds_mixed" },
    };
    assert.equal(
      contractMatchesRoute(
        {
          source: { format: "csv", filename: "sample-orders.csv" },
          destination: { format: "postgres", table: "qa_e2e_ds_mixed" },
        },
        route,
      ),
      true,
    );
    assert.equal(
      contractMatchesRoute(
        { source: { format: "csv" }, destination: { format: "postgresql" } },
        route,
      ),
      true,
    );
    assert.equal(
      contractMatchesRoute(
        {
          source: { format: "csv", filename: "other.csv" },
          destination: { format: "postgresql", table: "public.QA_E2E_ds_mixed" },
        },
        route,
      ),
      false,
    );
    assert.equal(
      contractMatchesRoute(
        {
          source: { format: "csv", filename: "sample-orders.csv" },
          destination: { format: "postgresql", table: "public.QA_E2E_ds_mixed" },
        },
        route,
      ),
      true,
    );
    assert.equal(
      contractMatchesRoute(
        {
          source: { format: "mysql", table: "orders" },
          destination: { format: "postgresql", table: "QA_E2E_ds_mixed" },
        },
        route,
      ),
      false,
    );
    assert.equal(
      contractMatchesRoute(
        { source: { format: "csv" }, destination: { format: "postgresql" } },
        { source: { format: "" }, destination: { format: "postgresql" } },
      ),
      false,
    );
  });
});
