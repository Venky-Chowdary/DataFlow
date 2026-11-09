/**
 * Validate studio primary — one owner for the step band.
 *
 * Charter: one root cause → one primary Button with a real destination.
 * The footer rail owns that teal control. Dashboard cards diagnose and offer
 * ghost/secondary copies only — never a second enabled primary.
 *
 * Execute unlocks only when API Validate passed AND decision === "approve".
 */
import type { PreflightResult } from "./types.js";
import type { DisplayBlocker } from "./validateIssueGrouping.js";
import {
  schemaDriftAllowsAcknowledge,
  schemaDriftRequiresRemap,
} from "./validateHonestyControls.js";

export type ValidateStudioPrimaryKind =
  | "run_preflight"
  | "open_live"
  | "primary_fix"
  | "hold_out"
  | "choose_policy"
  | "approve_mappings"
  | "execute"
  | "none";

export type ValidateActionFamily =
  | "map_open"
  | "identity"
  | "bad_data"
  | "privilege"
  | "connection"
  | "acknowledge_pii"
  | "acknowledge_drift"
  | "acknowledge_fk"
  | "approve_ddl"
  | "hold_out"
  | "approve_mappings"
  | "remap_column"
  | "orphan"
  | "dest_shape"
  | "run_preflight"
  | "execute"
  | "open_live"
  | "other";

export interface ValidateStudioPrimaryInput {
  preflight: PreflightResult | null;
  preflighting?: boolean;
  transferring?: boolean;
  mappingReviewCount?: number;
  riskAckPendingCount?: number;
  transferLaunch?: { jobId: string; rows: number } | null;
  executeBlocked?: boolean;
  hasPrimaryFix?: boolean;
  primaryFixLabel?: string;
  hasHoldOut?: boolean;
}

export interface ValidateStudioPrimary {
  kind: ValidateStudioPrimaryKind;
  family: ValidateActionFamily | null;
  label: string;
  enabled: boolean;
  executeLabel: "Execute" | "Execute (blocked)" | "Execute (review)" | null;
  executeIsPrimary: boolean;
  executeEnabled: boolean;
}

export type PromotedPrimaryFix = {
  destination: "map" | "connectors" | "ack_drift" | "ack_pii" | "ack_fk" | "ack_ddl";
  label: string;
};

export function actionFamilyFromLabel(label: string | undefined | null): ValidateActionFamily {
  if (!label) return "other";
  if (/identity|sync mode|primary key/i.test(label)) return "identity";
  if (/bad data|strip control|encoding/i.test(label)) return "bad_data";
  if (/hold.?out|rows held out/i.test(label)) return "hold_out";
  if (/grant write|privilege/i.test(label)) return "privilege";
  if (/credential|connection/i.test(label)) return "connection";
  if (/pii/i.test(label)) return "acknowledge_pii";
  if (/acknowledge drift|schema drift/i.test(label)) return "acknowledge_drift";
  if (/fk risk|foreign.?key/i.test(label)) return "acknowledge_fk";
  if (/approve mapping/i.test(label)) return "approve_mappings";
  if (/live progress/i.test(label)) return "open_live";
  if (/^execute/i.test(label)) return "execute";
  if (/run preflight/i.test(label)) return "run_preflight";
  if (/map|remap|target ddl|breaking change|include column|choose policy/i.test(label)) {
    return "map_open";
  }
  return "other";
}

/**
 * Dashboard remediations are never the studio primary. Map-open copies are
 * ghost (same destination as the rail). In-place acks stay secondary.
 */
export function dashboardCtaVariant(
  family: ValidateActionFamily,
): "ghost" | "secondary" {
  if (
    family === "map_open"
    || family === "run_preflight"
    || family === "open_live"
    || family === "execute"
  ) {
    return "ghost";
  }
  return "secondary";
}

export function transferDecisionOf(
  preflight: PreflightResult | null | undefined,
): "approve" | "review" | "block" | "pending" {
  const raw = preflight?.proof_bundle?.transfer_decision?.decision;
  if (raw === "approve" || raw === "review" || raw === "block") return raw;
  if (preflight) return "review";
  return "pending";
}

export function resolveValidateStudioPrimary(
  input: ValidateStudioPrimaryInput,
): ValidateStudioPrimary {
  const passed = Boolean(input.preflight?.passed);
  const decision = transferDecisionOf(input.preflight);
  const reviewGrade = Boolean(passed && decision === "review");
  const blocked = Boolean(input.preflight && !input.preflight.passed && !input.preflighting);
  const mappingBlocked = Boolean(
    input.preflight?.blockers?.some((b) => b.id.includes("mapping")),
  );
  const executeEnabled = !input.transferring && passed && !reviewGrade && !input.executeBlocked;
  const executeLabel = !input.preflight
    ? null
    : (input.executeBlocked || !passed)
      ? "Execute (blocked)"
      : reviewGrade
        ? "Execute (review)"
        : "Execute";

  if (input.transferLaunch) {
    return {
      kind: "open_live",
      family: "open_live",
      label: "Open live progress",
      enabled: true,
      executeLabel: null,
      executeIsPrimary: false,
      executeEnabled: false,
    };
  }

  if (!input.preflight) {
    return {
      kind: "run_preflight",
      family: "run_preflight",
      label: "Run preflight",
      enabled: !input.preflighting,
      executeLabel: null,
      executeIsPrimary: false,
      executeEnabled: false,
    };
  }

  if (blocked && input.hasPrimaryFix && input.primaryFixLabel) {
    return {
      kind: "primary_fix",
      family: actionFamilyFromLabel(input.primaryFixLabel),
      label: input.primaryFixLabel,
      enabled: true,
      executeLabel,
      executeIsPrimary: false,
      executeEnabled: false,
    };
  }

  if (blocked && (input.riskAckPendingCount ?? 0) > 0 && !input.hasPrimaryFix) {
    if (input.hasHoldOut !== false) {
      return {
        kind: "hold_out",
        family: "hold_out",
        label: "Run with rows held out",
        enabled: true,
        executeLabel,
        executeIsPrimary: false,
        executeEnabled: false,
      };
    }
    return {
      kind: "choose_policy",
      family: "map_open",
      label: "Choose policy on Map",
      enabled: true,
      executeLabel,
      executeIsPrimary: false,
      executeEnabled: false,
    };
  }

  if (
    blocked
    && mappingBlocked
    && (input.mappingReviewCount ?? 0) > 0
    && (input.riskAckPendingCount ?? 0) === 0
    && !input.hasPrimaryFix
  ) {
    return {
      kind: "approve_mappings",
      family: "approve_mappings",
      label: "Approve mappings",
      enabled: true,
      executeLabel,
      executeIsPrimary: false,
      executeEnabled: false,
    };
  }

  return {
    kind: "execute",
    family: "execute",
    label: executeLabel || "Execute",
    enabled: executeEnabled,
    executeLabel,
    executeIsPrimary: true,
    executeEnabled,
  };
}

type GateLike = { id?: string; details?: Record<string, unknown> | null };

/** Gates that disclose destination DDL (upsert unique key, create-new table) for approval. */
const DDL_APPROVAL_GATE_IDS = new Set(["g_upsert_dest_key", "g_create_dest_ddl"]);

/**
 * The g_upsert_dest_key gate behind a blocker, whether the blocker is the gate
 * itself or the root cause that absorbed it (kind ``upsert_dest_ddl``).
 */
export function ddlApprovalGate(
  blocker: GateLike | null | undefined,
  gates?: GateLike[] | null,
): GateLike | null {
  if (!blocker) return null;
  if (blocker.id && DDL_APPROVAL_GATE_IDS.has(blocker.id)) return blocker;
  const details = (blocker.details ?? {}) as Record<string, unknown>;
  const impacted = Array.isArray(details.impacted_gates) ? details.impacted_gates.map(String) : [];
  if (details.kind !== "upsert_dest_ddl" && !impacted.includes("g_upsert_dest_key")) return null;
  return (gates ?? []).find((g) => g?.id === "g_upsert_dest_key") ?? null;
}

/** A block whose only remedy the operator can give here is approving DDL Validate disclosed. */
export function ddlApprovalPending(
  blocker: GateLike | null | undefined,
  gates?: GateLike[] | null,
): boolean {
  const gate = ddlApprovalGate(blocker, gates);
  const details = (gate?.details ?? null) as Record<string, unknown> | null;
  return (
    details?.remediation_kind === "approve_ddl_or_dedupe"
    && details?.requires_approval === true
    && !Number(details?.duplicate_key_count ?? 0)
    && typeof details?.planned_ddl === "string"
    && String(details.planned_ddl).trim().length > 0
  );
}

/**
 * The blocked g_upsert_dest_key gate awaiting approval of the DDL it disclosed.
 * Engine root causes absorb the gate into a source-less display item, so the
 * Validate surfaces read the gate itself rather than the blocker.
 */
export function pendingDdlApproval(
  pf: { gates?: Array<GateLike & { status?: string }> } | null | undefined,
): GateLike | null {
  const gate = (pf?.gates ?? []).find((g) => DDL_APPROVAL_GATE_IDS.has(g?.id ?? "") && g.status === "block");
  return gate && ddlApprovalPending(gate) ? gate : null;
}

/** Validate recorded the operator's approval of the DDL it disclosed. */
export function ddlApprovalRecorded(
  pf: { gates?: Array<{ id?: string; details?: Record<string, unknown> | null }> } | null | undefined,
): boolean {
  return (pf?.gates ?? []).some(
    (g) => DDL_APPROVAL_GATE_IDS.has(g?.id ?? "") && g.details?.acknowledged === true,
  );
}

/**
 * When suggested_actions is empty, still put a real destination on the rail
 * so the dashboard never has to invent a second teal.
 */
export function promoteBlockedPrimaryFix(
  firstBlocker: DisplayBlocker | undefined,
  gates?: GateLike[] | null,
): PromotedPrimaryFix {
  if (!firstBlocker) return { destination: "map", label: "Open Map to fix" };
  const details = (firstBlocker.source?.details ?? null) as Record<string, unknown> | null;
  if (schemaDriftRequiresRemap(details)) {
    return { destination: "map", label: "Open Map to fix breaking change" };
  }
  if (schemaDriftAllowsAcknowledge(details)) {
    return { destination: "ack_drift", label: "Acknowledge drift for this run" };
  }
  if (details?.compliance_ack_required === true) {
    return { destination: "ack_pii", label: "Approve PII for this transfer" };
  }
  if (ddlApprovalPending(firstBlocker.source, gates)) {
    return { destination: "ack_ddl", label: "Approve destination DDL" };
  }
  const blob = [
    firstBlocker.message,
    firstBlocker.title,
    firstBlocker.source?.id,
    JSON.stringify(details || {}),
  ].join(" ");
  if (
    firstBlocker.source?.id === "constraint_fk"
    || details?.remediation_kind === "acknowledge_fk_risk"
    || /foreign key|fk_column_unmapped|destination_fk_metadata/i.test(blob)
  ) {
    return { destination: "ack_fk", label: "Acknowledge FK risk for this run" };
  }
  if (
    /privilege|GRANT|ACL|IAM|has_privileges/i.test(blob)
    && /g2|destination/i.test(firstBlocker.source?.id || blob)
  ) {
    return { destination: "connectors", label: "Grant write privilege" };
  }
  if (/authentication|not reachable|connection refused|credential/i.test(blob)) {
    return { destination: "connectors", label: "Fix connector credentials" };
  }
  const fix = firstBlocker.fix || "";
  if (fix && /map|remap|ddl|type/i.test(fix)) {
    return { destination: "map", label: fix.length > 48 ? "Open Map to fix" : fix };
  }
  return { destination: "map", label: "Open Map to fix" };
}

/** Exactly one enabled primary comes out of the resolver. */
export function studioPrimaryIsSingular(primary: ValidateStudioPrimary): boolean {
  const enabledStudio = primary.enabled && primary.kind !== "none" ? 1 : 0;
  const extraExecute =
    primary.executeIsPrimary && primary.executeEnabled && primary.kind !== "execute" ? 1 : 0;
  return enabledStudio + extraExecute <= 1;
}
