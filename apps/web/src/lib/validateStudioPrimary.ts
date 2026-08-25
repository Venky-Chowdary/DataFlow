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
  destination: "map" | "connectors" | "ack_drift" | "ack_pii" | "ack_fk";
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

/**
 * When suggested_actions is empty, still put a real destination on the rail
 * so the dashboard never has to invent a second teal.
 */
export function promoteBlockedPrimaryFix(
  firstBlocker: DisplayBlocker | undefined,
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
