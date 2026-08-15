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
