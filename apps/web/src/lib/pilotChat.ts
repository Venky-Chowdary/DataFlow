/**
 * Shared Datawrap Pilot chat helpers — PilotPage and AICopilot must behave identically
 * for context payloads, result-id tracking, and Confirm orchestration.
 */

import type { CopilotPendingAction } from "./api";
import {
  confirmPilotPending,
  isDestructiveTransfer,
  transferOverwriteMessage,
} from "./pilotConfirm";
import { extractPilotResultId } from "./pilotChatStore";
import type { ActiveDataContext, Screen } from "./types";

/** Shared screen labels for Pilot page + rail action chips. */
export const PILOT_SCREEN_LABELS: Record<string, string> = {
  dashboard: "Overview",
  pilot: "Datawrap Pilot",
  transfer: "Transfer Studio",
  connectors: "Connectors",
  jobs: "Jobs",
  settings: "Settings",
  schedules: "Pipelines",
  contracts: "Contracts",
  query: "Query",
  mcp: "MCP",
  docs: "Docs",
  benchmarks: "Proofs",
};

/** Apply non-mutating navigate suggestions from a chat turn. */
export function applyPilotSafeActions(
  actions: { risk?: string; type?: string; screen?: string; route?: string }[] | undefined,
  onNavigate?: (screen: Screen) => void,
): void {
  if (!onNavigate || !actions?.length) return;
  for (const a of actions) {
    if (a.risk === "mutate" || a.type === "studio") continue;
    const screen = a.screen || a.route;
    if ((a.type === "navigate" || !a.type) && screen) {
      onNavigate(screen as Screen);
    }
  }
}

/** Chip label for a suggested navigate/action. */
export function pilotActionChipLabel(action: {
  label?: string;
  screen?: string;
  route?: string;
}): string {
  const screen = action.screen || action.route;
  return action.label || (screen ? `Open ${PILOT_SCREEN_LABELS[screen] || screen}` : "Action");
}

export type ToastFn = (opts: {
  title: string;
  message: string;
  tone?: "success" | "error" | "info" | "warning";
}) => void;

export type ConfirmFn = (opts: {
  title: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  tone?: "danger" | "default";
}) => Promise<boolean>;

export type StudioDispatchFn = (action: {
  kind: string;
  label?: string;
  run_id?: string;
}) => void;

/** Build the data_context blob sent with every /copilot/chat turn. */
export function buildPilotDataContext(
  activeData: ActiveDataContext | null | undefined,
  opts: { sessionId: string; lastResultId?: string; nameFallback?: string },
): ActiveDataContext {
  return {
    name: activeData?.name || opts.nameFallback || "pilot",
    columns: activeData?.columns || [],
    row_count: activeData?.row_count ?? 0,
    filename: activeData?.filename,
    samples: activeData?.samples,
    preflight_run_id: activeData?.preflight_run_id,
    job_id: activeData?.job_id,
    validation_status: activeData?.validation_status,
    route: activeData?.route,
    blockers: activeData?.blockers,
    pilot_session_id: opts.sessionId,
    last_result_id: opts.lastResultId,
  };
}

/** Update last_result_id from a chat response (shared PilotPage + rail logic). */
export function nextPilotResultId(
  toolsUsed: { name: string; success: boolean; summary: string }[] | undefined,
  dataInsightLastId: string | undefined,
  previous: string | undefined,
): string | undefined {
  const liveTools = (toolsUsed || []).filter((t) =>
    ["sample_connector_object", "run_query", "filter_result", "aggregate_data"].includes(t.name),
  );
  const freshId = dataInsightLastId || extractPilotResultId(toolsUsed);
  if (freshId) return freshId;
  if (liveTools.length > 0 && liveTools.every((t) => !t.success)) return undefined;
  return previous;
}

export type PilotConfirmHandlers = {
  onNavigate?: (screen: Screen) => void;
  toast: ToastFn;
  confirm: ConfirmFn;
  dispatchStudioAction: StudioDispatchFn;
};

/**
 * Single Confirm implementation for rail + Pilot page.
 * Mutations only run after operator approval; Fix-bad-data opens Studio (does not rewrite rows in chat).
 */
export async function runPilotConfirm(
  action: CopilotPendingAction,
  handlers: PilotConfirmHandlers,
): Promise<"cleared" | "cancelled" | "unknown"> {
  const { onNavigate, toast, confirm, dispatchStudioAction } = handlers;

  if (
    action.type === "studio"
    || (
      action.kind
      && action.type !== "start_transfer"
      && action.type !== "create_connector"
      && action.type !== "run_schedule"
    )
  ) {
    dispatchStudioAction({
      kind: (action.kind || String(action.payload?.kind || "")) as string,
      label: action.label,
      run_id: action.run_id || (action.payload?.run_id as string | undefined),
    });
    onNavigate?.("transfer");
    toast({
      title: "Opening Fix bad data",
      message: action.label || "Confirm opens Transfer Studio — apply the fix there.",
      tone: "info",
    });
    return "cleared";
  }

  if (action.type === "run_schedule") {
    const res = await confirmPilotPending(action);
    if (res.kind !== "run_schedule") throw new Error("Unexpected confirm result");
    onNavigate?.("schedules");
    toast({
      title: res.idempotent ? "Pipeline already started" : "Pipeline started",
      message: `“${res.name}” → job ${res.job_id || "queued"}`,
      tone: "success",
    });
    return "cleared";
  }

  if (action.type === "create_connector") {
    const res = await confirmPilotPending(action);
    if (res.kind !== "create_connector") throw new Error("Unexpected confirm result");
    window.dispatchEvent(new CustomEvent("df2:connectors-changed"));
    onNavigate?.("connectors");
    toast({
      title: res.idempotent ? "Connector already saved" : "Connector saved",
      message: `“${res.name}” (${res.type}) is ready in Connectors.`,
      tone: "success",
    });
    return "cleared";
  }

  if (action.type === "start_transfer") {
    if (isDestructiveTransfer(action)) {
      const ok = await confirm({
        title: "Overwrite the destination?",
        message: transferOverwriteMessage(action),
        confirmLabel: "Overwrite & run",
        tone: "danger",
      });
      if (!ok) return "cancelled";
    }
    const res = await confirmPilotPending(action);
    if (res.kind !== "start_transfer") throw new Error("Unexpected confirm result");
    toast({
      title: res.idempotent ? "Transfer already running" : "Transfer started",
      message: `${res.source} → ${res.destination}`,
      tone: "success",
    });
    onNavigate?.("jobs");
    return "cleared";
  }

  toast({ title: "Unknown action", message: action.type, tone: "error" });
  return "unknown";
}
