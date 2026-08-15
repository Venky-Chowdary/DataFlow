/**
 * Pilot run_schedule Confirm — redacted preview + fail-closed bind.
 *
 * Engine staging (`assert_schedule_run_allowed`) already refuses unsigned /
 * OPEN-breaker binds. This helper is the operator surface for the same facts.
 */

import { contractBindFromPreview } from "./contractBind";
import { contractBreakerBlocksRun } from "./contractBreakerUi";

export interface PilotSchedulePreview {
  schedule_id?: string;
  name?: string;
  source_connector_id?: string;
  dest_connector_id?: string;
  source_table?: string;
  dest_table?: string;
  sync_mode?: string;
  contract_id?: string;
  require_signed_contract?: boolean;
  enforce_contract?: boolean;
  breaker_state?: string;
}

export function schedulePreviewFromPayload(
  payload: Record<string, unknown> | null | undefined,
): PilotSchedulePreview {
  const preview = (payload?.preview && typeof payload.preview === "object"
    ? payload.preview
    : payload || {}) as Record<string, unknown>;
  return {
    schedule_id: String(preview.schedule_id || payload?.schedule_id || ""),
    name: String(preview.name || payload?.name || ""),
    source_connector_id: String(preview.source_connector_id || ""),
    dest_connector_id: String(preview.dest_connector_id || ""),
    source_table: String(preview.source_table || ""),
    dest_table: String(preview.dest_table || ""),
    sync_mode: String(preview.sync_mode || ""),
    contract_id: String(preview.contract_id || "").trim() || undefined,
    require_signed_contract: Boolean(preview.require_signed_contract),
    enforce_contract: Boolean(preview.enforce_contract),
    breaker_state: String(preview.breaker_state || "").trim() || undefined,
  };
}

export function isDestructiveSchedulePreview(preview: PilotSchedulePreview): boolean {
  return preview.sync_mode === "full_refresh_overwrite";
}

/** Empty string when Confirm may proceed. OPEN / half-open breaker blocks. */
export function scheduleConfirmBlocksRun(preview: PilotSchedulePreview): string {
  return contractBreakerBlocksRun(preview.breaker_state);
}

export function scheduleConfirmBind(preview: PilotSchedulePreview): {
  contractId: string;
  requireSigned: boolean;
  breakerState: string;
} {
  const bind = contractBindFromPreview({
    contract_id: preview.contract_id,
    require_signed_contract: preview.require_signed_contract,
  });
  return {
    contractId: bind.contractId,
    requireSigned: bind.requireSigned,
    breakerState: String(preview.breaker_state || ""),
  };
}
