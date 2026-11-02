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

export type ContractRouteSide = {
  format?: string | null;
  table?: string | null;
  collection?: string | null;
  filename?: string | null;
};

export type ContractRoute = {
  source?: ContractRouteSide | null;
  destination?: ContractRouteSide | null;
};

function contractToken(value: unknown): string {
  return String(value ?? "").trim().toLowerCase();
}

/** postgres and postgresql are the same engine pair. File formats stay as typed. */
function contractFormat(value: unknown): string {
  const raw = contractToken(value);
  if (!raw) return "";
  if (raw.includes("postgres")) return "postgresql";
  return raw;
}

const CONTRACT_FILE_EXT = /\.(csv|tsv|json|jsonl|ndjson|parquet|avro|orc|xml|txt|xlsx|xls)$/;

function contractObjectName(side: Record<string, unknown> | null | undefined): string {
  if (!side) return "";
  const raw = contractToken(side.table || side.collection || side.filename || "");
  if (!raw) return "";
  // sample-orders.csv keeps its extension. public.orders is the table orders.
  if (CONTRACT_FILE_EXT.test(raw)) return raw;
  const parts = raw.split(".").filter(Boolean);
  return parts[parts.length - 1] || raw;
}

/**
 * Signed-contract dropdown filter. Fail closed when either format is missing.
 * A format-only contract still matches that engine pair. When both sides name
 * a table or file, the names must match.
 */
export function contractMatchesRoute(
  contract: {
    source?: Record<string, unknown> | null;
    destination?: Record<string, unknown> | null;
  },
  route: ContractRoute | null | undefined,
): boolean {
  const routeSource = route?.source || {};
  const routeDest = route?.destination || {};
  const sourceFormat = contractFormat(routeSource.format);
  const destFormat = contractFormat(routeDest.format);
  if (!sourceFormat || !destFormat) return false;
  const storedSource = contract.source || {};
  const storedDest = contract.destination || {};
  const storedSourceFormat = contractFormat(storedSource.format || storedSource.type);
  const storedDestFormat = contractFormat(storedDest.format || storedDest.type);
  if (!storedSourceFormat || !storedDestFormat) return false;
  if (storedSourceFormat !== sourceFormat || storedDestFormat !== destFormat) return false;
  const routeSourceName = contractObjectName(routeSource as Record<string, unknown>);
  const routeDestName = contractObjectName(routeDest as Record<string, unknown>);
  const storedSourceName = contractObjectName(storedSource);
  const storedDestName = contractObjectName(storedDest);
  if (routeSourceName && storedSourceName && routeSourceName !== storedSourceName) return false;
  if (routeDestName && storedDestName && routeDestName !== storedDestName) return false;
  return true;
}

/** Redacted Pilot Confirm preview — same bind shape as plan policies. */
export function contractBindFromPreview(
  preview: {
    contract_id?: string;
    require_signed_contract?: boolean;
    breaker_state?: string;
  } | null | undefined,
): { contractId: string; requireSigned: boolean; breakerState: string } {
  if (!preview) return { contractId: "", requireSigned: false, breakerState: "" };
  const bind = contractBindFromPolicies({
    contract_id: preview.contract_id,
    require_signed_contract: preview.require_signed_contract,
  });
  return {
    ...bind,
    breakerState: String(preview.breaker_state || "").trim(),
  };
}
