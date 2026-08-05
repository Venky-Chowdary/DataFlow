/**
 * Module 10 — Validate Decision Path SSOT.
 *
 * Charter UX order (never dump duplicate gates as the primary story):
 *
 *   Root Cause → Affected Gates → Business Impact → Recommended Actions
 *   → Preview Changes → Risk Contract → Execute
 *
 * Pure presenter over engine root_causes / display blockers — does not change
 * gate outcomes.
 */
import type { PreflightResult } from "./types.js";
import {
  buildDisplayBlockers,
  type DisplayBlocker,
} from "./validateIssueGrouping.js";

export type DecisionPathStepId =
  | "root_cause"
  | "affected_gates"
  | "business_impact"
  | "recommended_actions"
  | "execution_policy"
  | "preview_changes"
  | "risk_contract"
  | "execute";

export interface DecisionPathStep {
  id: DecisionPathStepId;
  label: string;
  summary: string;
  detail?: string;
  status: "blocked" | "action" | "ready" | "info" | "locked" | "unlocked";
}

export interface ValidateDecisionPath {
  /** Ordered charter steps for the primary blocked decision. */
  steps: DecisionPathStep[];
  /** All root / residual blockers as decision cards. */
  decisions: Array<{
    key: string;
    kind: DisplayBlocker["kind"];
    title: string;
    steps: DecisionPathStep[];
  }>;
  executeUnlocked: boolean;
  migrationProven: boolean;
  riskContractIncomplete: boolean;
  headline: string;
  note: string;
}

const STEP_ORDER: DecisionPathStepId[] = [
  "root_cause",
  "affected_gates",
  "business_impact",
  "recommended_actions",
  "execution_policy",
  "preview_changes",
  "risk_contract",
  "execute",
];

function riskContractState(preflight: PreflightResult | null | undefined): {
  incomplete: boolean;
  summary: string;
  status: DecisionPathStep["status"];
} {
  const rc = preflight?.proof_bundle?.risk_contracts as
    | { incomplete?: boolean; missing_columns?: string[]; note?: string }
    | undefined;
  const incomplete = Boolean(rc?.incomplete);
  const missing = (rc?.missing_columns ?? []).slice(0, 5);
  if (incomplete) {
    return {
      incomplete: true,
      summary: missing.length
        ? `Migration Risk Contract required for: ${missing.join(", ")}`
        : "Migration Risk Contract required before Execute-approve.",
      status: "action",
    };
  }
  if (rc && rc.incomplete === false) {
    return {
      incomplete: false,
      summary: "Risk contracts complete for lossy columns (or none required).",
      status: "ready",
    };
  }
  return {
    incomplete: false,
    summary:
      "Open Map → sign a Migration Risk Contract with an explicit continue policy when fidelity is lossy.",
    status: "info",
  };
}

function stepsForBlocker(
  item: DisplayBlocker,
  opts: {
    executeUnlocked: boolean;
    risk: ReturnType<typeof riskContractState>;
  },
): DecisionPathStep[] {
  const { executeUnlocked, risk } = opts;
  const gates = (item.gateChips ?? []).map((g) => g.label).join(", ") || "See gate cards";
  const actions = [
    item.fix,
    ...(item.suggested_actions ?? []).map((a) => a.label || a.kind || "").filter(Boolean),
  ].filter(Boolean) as string[];
  const preview = (item.issues ?? []).slice(0, 4).join(" · ")
    || (item.suggested_actions?.[0]?.label
      ?? "Preview remap / policy on Map before Execute.");

  const byId: Record<DecisionPathStepId, DecisionPathStep> = {
    root_cause: {
      id: "root_cause",
      label: "Root Cause",
      summary: item.title,
      detail: item.message,
      status: "blocked",
    },
    affected_gates: {
      id: "affected_gates",
      label: "Affected Gates",
      summary: gates,
      detail: "One root cause may impact multiple gates — they are not separate problems.",
      status: "info",
    },
    business_impact: {
      id: "business_impact",
      label: "Business Impact",
      summary: item.impact || item.why || "Execute stays locked until this root cause is resolved.",
      detail: [
        typeof item.affectedRowsSample === "number"
          ? `Affected sample rows: ${item.affectedRowsSample}`
          : "",
        typeof item.estimatedTotalRows === "number"
          ? `Estimated population: ${item.estimatedTotalRows.toLocaleString()}`
          : "",
        item.confidenceNote || "",
        "Sample counts are not population proof.",
      ].filter(Boolean).join(" · ") || undefined,
      status: "blocked",
    },
    recommended_actions: {
      id: "recommended_actions",
      label: "Recommended Actions",
      summary: actions[0] || "Remap on Map or mint a Migration Risk Contract.",
      detail: actions.slice(1).join(" · ") || undefined,
      status: "action",
    },
    execution_policy: {
      id: "execution_policy",
      label: "Execution Policy",
      summary: risk.incomplete
        ? (item.quarantinePolicy
          ? `Quarantine posture: ${item.quarantinePolicy}. Choose an explicit continue policy on Map (no hidden default).`
          : "Choose an explicit execution policy on Map — FAIL_JOB / STOP_* / QUARANTINE_ROW / CAST_AND_CONTINUE / …")
        : "Signed continue-policy Risk Contract(s) present — write path follows those policies.",
      detail: [
        item.rollbackPolicy
          ? `Rollback posture: ${item.rollbackPolicy}`
          : "Rollback posture: DOCUMENT_ONLY",
        item.rollbackExecutable === true
          ? "Rollback availability: DISCARD_STAGING executable (staging only)."
          : "Rollback availability: not executable here — warehouse restore / DBA runbook only.",
        "No hidden defaults — policy must be selected before Sign Risk Contract.",
      ].join(" "),
      status: risk.incomplete ? "action" : "ready",
    },
    preview_changes: {
      id: "preview_changes",
      label: "Preview Changes",
      summary: preview,
      status: "info",
    },
    risk_contract: {
      id: "risk_contract",
      label: "Risk Contract",
      summary: risk.summary,
      status: risk.status,
    },
    execute: {
      id: "execute",
      label: "Execute",
      summary: executeUnlocked
        ? "Execute unlocked — still not migration_proven until post-write Gate-8 full_checksum."
        : "Execute locked until root causes and required Risk Contracts clear.",
      status: executeUnlocked ? "unlocked" : "locked",
    },
  };

  return STEP_ORDER.map((id) => byId[id]);
}

/**
 * Build the Validate decision path. Prefer engine root_causes via buildDisplayBlockers.
 */
export function buildValidateDecisionPath(
  preflight: PreflightResult | null | undefined,
  opts?: {
    syncMode?: string;
    executeUnlocked?: boolean;
  },
): ValidateDecisionPath {
  const unlocked = Boolean(opts?.executeUnlocked ?? preflight?.passed);
  const risk = riskContractState(preflight);
  const blockers = preflight ? buildDisplayBlockers(preflight, opts?.syncMode) : [];
  const migrationProven = Boolean(preflight?.proof_bundle?.migration_proven);

  const decisions = blockers.map((item) => ({
    key: item.key,
    kind: item.kind,
    title: item.title,
    steps: stepsForBlocker(item, { executeUnlocked: unlocked, risk }),
  }));

  const primary = decisions[0]?.steps ?? STEP_ORDER.map((id) => ({
    id,
    label: id.replace(/_/g, " "),
    summary: unlocked ? "No blocking root causes." : "Run Validate to surface root causes.",
    status: (unlocked ? "ready" : "info") as DecisionPathStep["status"],
  }));

  // Align execute step on empty path.
  if (decisions.length === 0) {
    const emptyRisk = riskContractState(preflight);
    return {
      steps: [
        {
          id: "root_cause",
          label: "Root Cause",
          summary: "No blocking root causes",
          status: "ready",
        },
        {
          id: "affected_gates",
          label: "Affected Gates",
          summary: "All required gates clear or skipped with honesty",
          status: "ready",
        },
        {
          id: "business_impact",
          label: "Business Impact",
          summary: "Execute may proceed under the active validation mode contract",
          status: "ready",
        },
        {
          id: "recommended_actions",
          label: "Recommended Actions",
          summary: "Export proof after Run — do not treat Validate alone as migration proven",
          status: "info",
        },
        {
          id: "execution_policy",
          label: "Execution Policy",
          summary: "No continue policy active — Map must choose an explicit policy before signing a Risk Contract",
          status: "info",
        },
        {
          id: "preview_changes",
          label: "Preview Changes",
          summary: "No pending remap / contract changes required",
          status: "ready",
        },
        {
          id: "risk_contract",
          label: "Risk Contract",
          summary: emptyRisk.summary,
          status: emptyRisk.status,
        },
        {
          id: "execute",
          label: "Execute",
          summary: unlocked
            ? "Execute unlocked — migration_proven requires post-write full_checksum"
            : "Execute locked",
          status: unlocked ? "unlocked" : "locked",
        },
      ],
      decisions: [],
      executeUnlocked: unlocked,
      migrationProven,
      riskContractIncomplete: risk.incomplete,
      headline: unlocked ? "Ready to Execute" : "Validate to unlock Execute",
      note: "Execute-ready is not migration proven. Post-write Gate-8 full_checksum is required for migration_proven.",
    };
  }

  return {
    steps: primary,
    decisions,
    executeUnlocked: unlocked,
    migrationProven,
    riskContractIncomplete: risk.incomplete,
    headline: `${decisions.length} root cause(s) · follow decision path before Execute`,
    note: "One root cause may affect many gates. Risk Contract is required for intentional lossy paths. Sample Validate never claims population proof.",
  };
}

export function decisionPathStepLabels(): string[] {
  return [
    "Root Cause",
    "Affected Gates",
    "Business Impact",
    "Recommended Actions",
    "Execution Policy",
    "Preview Changes",
    "Risk Contract",
    "Execute",
  ];
}
