/** Shared Studio / schedule contract bind — fail-closed when SIGNED is required. */

export function contractBindBlocksRun(opts: {
  contractId: string;
  requireSigned: boolean;
  selectedStatus?: string;
}): string {
  const id = String(opts.contractId || "").trim();
  if (opts.requireSigned && !id) {
    return "Require signed is on but no contract is selected.";
  }
  const status = String(opts.selectedStatus || "").trim().toUpperCase();
  if (id && opts.requireSigned && status && status !== "SIGNED") {
    return "Contract is not SIGNED. Open Contracts, sign it, then return — or clear the selection.";
  }
  return "";
}

export function isSignedContractStatus(status: string | undefined | null): boolean {
  return String(status || "").trim().toUpperCase() === "SIGNED";
}

/** Read opt-in bind from a persisted transfer-plan policies object. */
export function contractBindFromPolicies(
  policies: Record<string, unknown> | null | undefined,
): { contractId: string; requireSigned: boolean } {
  const p = policies || {};
  const contractId = String(p.contract_id || "").trim();
  if (!contractId) return { contractId: "", requireSigned: false };
  if (Object.prototype.hasOwnProperty.call(p, "require_signed_contract")) {
    return { contractId, requireSigned: Boolean(p.require_signed_contract) };
  }
  return { contractId, requireSigned: true };
}
