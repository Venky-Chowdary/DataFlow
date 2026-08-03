/**
 * Shared Confirm handlers for staged Pilot mutations.
 *
 * PilotPage and the rail/FAB both need to approve the same ack-ledger actions.
 * Keeping the credential-free confirm path in one place means an overwrite
 * warning and an idempotent replay behave identically wherever the operator
 * presses Confirm.
 */

import { confirmCopilotAction, type CopilotPendingAction } from "./api";

export type PilotConfirmOutcome =
  | {
      kind: "create_connector";
      idempotent: boolean;
      name: string;
      type: string;
      connector_id?: string;
    }
  | {
      kind: "start_transfer";
      idempotent: boolean;
      job_id: string;
      source: string;
      destination: string;
      sync_mode: string;
      destructive: boolean;
    }
  | {
      kind: "run_schedule";
      idempotent: boolean;
      job_id: string;
      schedule_id: string;
      name: string;
    };

function payloadRecord(action: CopilotPendingAction): Record<string, unknown> {
  return (action.payload && typeof action.payload === "object"
    ? action.payload
    : {}) as Record<string, unknown>;
}

function previewRecord(payload: Record<string, unknown>): Record<string, unknown> {
  return (payload.preview && typeof payload.preview === "object"
    ? payload.preview
    : {}) as Record<string, unknown>;
}

export function isDestructiveTransfer(action: CopilotPendingAction): boolean {
  const payload = payloadRecord(action);
  const preview = previewRecord(payload);
  const syncMode = String(preview.sync_mode || payload.sync_mode || "");
  return Boolean(
    action.destructive
    || payload.destructive
    || syncMode === "full_refresh_overwrite",
  );
}

export async function confirmPilotPending(
  action: CopilotPendingAction,
): Promise<PilotConfirmOutcome> {
  const payload = payloadRecord(action);
  const preview = previewRecord(payload);

  if (action.type === "create_connector") {
    const ackId = String(payload.ack_id || "");
    if (!ackId) {
      throw new Error(
        "This approval is missing a server ack_id (credentials are not stored in the browser). Ask Pilot to create the connector again.",
      );
    }
    const res = await confirmCopilotAction({
      ack_id: ackId,
      actor: "pilot-ui",
      reason: "operator confirmed create_connector",
    });
    return {
      kind: "create_connector",
      idempotent: Boolean(res.idempotent),
      name: String(res.name || preview.name || "Connector"),
      type: String(res.type || preview.type || "connector"),
      connector_id: res.connector_id,
    };
  }

  if (action.type === "start_transfer") {
    const ackId = String(payload.ack_id || "");
    if (!ackId) {
      throw new Error(
        "This approval is missing a server ack_id. Ask Pilot to plan the transfer again.",
      );
    }
    const destructive = isDestructiveTransfer(action);
    const res = await confirmCopilotAction({
      ack_id: ackId,
      actor: "pilot-ui",
      reason: destructive
        ? "operator confirmed destructive start_transfer"
        : "operator confirmed start_transfer",
    });
    const jobId = String(res.job_id || "");
    if (!jobId) throw new Error("Transfer was confirmed but no job_id came back.");
    return {
      kind: "start_transfer",
      idempotent: Boolean(res.idempotent),
      job_id: jobId,
      source: String(preview.source || res.source || "source"),
      destination: String(preview.destination || res.destination || "destination"),
      sync_mode: String(preview.sync_mode || res.sync_mode || ""),
      destructive,
    };
  }

  if (action.type === "run_schedule") {
    const ackId = String(payload.ack_id || "");
    if (!ackId) {
      throw new Error(
        "This approval is missing a server ack_id. Ask Pilot to run the pipeline again.",
      );
    }
    const res = await confirmCopilotAction({
      ack_id: ackId,
      actor: "pilot-ui",
      reason: "operator confirmed run_schedule",
    });
    const jobId = String(res.job_id || "");
    if (!jobId) throw new Error("Pipeline was confirmed but no job_id came back.");
    return {
      kind: "run_schedule",
      idempotent: Boolean(res.idempotent),
      job_id: jobId,
      schedule_id: String(res.schedule_id || payload.schedule_id || preview.schedule_id || ""),
      name: String(res.name || payload.name || preview.name || "Pipeline"),
    };
  }

  throw new Error(`Unsupported Pilot action: ${action.type}`);
}

export function transferOverwriteMessage(action: CopilotPendingAction): string {
  const preview = previewRecord(payloadRecord(action));
  const destination = String(preview.destination || "the destination");
  return (
    `This will replace every row at ${destination} with the source. `
    + "The transfer will not start unless you confirm."
  );
}
