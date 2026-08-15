/**
 * Run: npx --yes tsx --test apps/web/src/lib/contractBind.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  contractBindBlocksRun,
  contractBindFromPolicies,
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
});
