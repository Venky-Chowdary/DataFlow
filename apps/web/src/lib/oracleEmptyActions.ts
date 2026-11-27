/**
 * Studio's side of the Oracle '' gate (API: services/oracle_empty_not_null.py).
 *
 * Oracle stores '' as NULL, so Validate blocks a NOT NULL column that may
 * receive one (or warns when it reuses an earlier check). The gate lists its
 * actions on `gate.details.suggested_actions`; Studio offers a button only for
 * the ones it performs end to end:
 *
 * - `oracle_default_transform` — maps `default:<value>` (the operator names the value);
 * - `oracle_empty_as_null` — "Create <col> as NULL-able": the API signs the contract;
 * - `oracle_empty_check_now` (also `oracle_empty_check_again`) — the API issues a signed
 *   Check-now request;
 * - `oracle_fix_connection` — opens Connectors.
 *
 * What Studio cannot do (replacing the source's values) is shown as text, never
 * as a button. The signed contract / request is kept on the mapping exactly as
 * returned and sent with the next Validate, where the API verifies it.
 */
import type { AskOptions } from "../components/ui/ConfirmDialog";
import { applyDefaultValue, uiTransformToEngine, type EditableMapping } from "./mapping";
import type { PreflightGate, PreflightResult, ValidationSuggestedAction } from "./types";

export const ORACLE_EMPTY_GATE_ID = "g3o_oracle_empty_not_null";
export const ORACLE_EMPTY_AS_NULL = "oracle_empty_as_null";
export const ORACLE_EMPTY_CHECK_NOW = "oracle_empty_check_now";
export const ORACLE_DEFAULT_TRANSFORM = "oracle_default_transform";
export const ORACLE_FIX_CONNECTION = "oracle_fix_connection";

/** Action kinds of this gate that Studio performs itself. */
export const STUDIO_ORACLE_EMPTY_KINDS: ReadonlySet<string> = new Set([
  ORACLE_EMPTY_AS_NULL,
  ORACLE_EMPTY_CHECK_NOW,
  ORACLE_DEFAULT_TRANSFORM,
  ORACLE_FIX_CONNECTION,
]);

export interface OracleEmptyAction extends ValidationSuggestedAction {
  id?: string;
  budget_seconds?: number;
  risk_acceptance?: { option: string; columns: string[]; [key: string]: unknown };
}

export interface OracleEmptySignBody {
  mappings: { source: string; target: string; target_type?: string; transform?: string }[];
  risk_acceptance: { option: string; columns: string[]; reason?: string };
  dest_table: string;
}

export interface OracleEmptySignResponse {
  option: string;
  key: string;
  signed: ({ source: string; target: string } & Record<string, unknown>)[];
}

/** The gate when it blocks or warns (a reused result is stated as a warning). */
export function oracleEmptyGate(preflight: PreflightResult | null | undefined): PreflightGate | null {
  const gate = (preflight?.gates ?? []).find((g) => g.id === ORACLE_EMPTY_GATE_ID);
  return gate && (gate.status === "block" || gate.status === "warn") ? gate : null;
}

/** Buttons for what Studio performs; plain-text notes for what it cannot. */
export function oracleEmptyActions(preflight: PreflightResult | null | undefined): {
  buttons: OracleEmptyAction[];
  notes: string[];
} {
  const gate = oracleEmptyGate(preflight);
  const raw = (gate?.details?.suggested_actions ?? []) as OracleEmptyAction[];
  const buttons: OracleEmptyAction[] = [];
  const notes: string[] = [];
  const seen = new Set<string>();
  for (const action of Array.isArray(raw) ? raw : []) {
    if (!action || typeof action !== "object" || !action.kind) continue;
    const key = `${action.id ?? action.kind}|${action.column ?? ""}|${action.label ?? ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (STUDIO_ORACLE_EMPTY_KINDS.has(String(action.kind))) buttons.push(action);
    else if (action.label) notes.push(action.label);
  }
  return { buttons, notes };
}

function namedColumns(action: OracleEmptyAction): string[] {
  const cols = action.risk_acceptance?.columns;
  return Array.isArray(cols) ? cols.map((c) => String(c)).filter(Boolean) : [];
}

/** The request body for an action the API signs, over the mappings it names. */
export function oracleSignBody(
  action: OracleEmptyAction,
  mappings: EditableMapping[],
  destTable: string,
  reason?: string,
): OracleEmptySignBody {
  const option = String(action.risk_acceptance?.option || action.kind);
  const columns = namedColumns(action);
  const wanted = new Set(columns.map((c) => c.toLowerCase()));
  const named = mappings.filter((m) => m.target && wanted.has(m.target.toLowerCase()));
  return {
    mappings: named.map((m) => ({
      source: m.source,
      target: m.target,
      target_type: m.destType || undefined,
      transform: uiTransformToEngine(m.transform, m.engineTransform),
    })),
    risk_acceptance: { option, columns, ...(reason ? { reason } : {}) },
    dest_table: destTable,
  };
}

/** Put what the API returned on the mapping pairs it was signed for. */
export function mergeOracleSigned(
  mappings: EditableMapping[],
  res: OracleEmptySignResponse,
): { next: EditableMapping[]; hit: number } {
  const field = res.key === "empty_as_null_contract"
    ? "emptyAsNullContract"
    : res.key === "empty_check_now"
      ? "emptyCheckNow"
      : null;
  if (!field) return { next: mappings, hit: 0 };
  let hit = 0;
  const next = mappings.map((m) => {
    const signed = res.signed.find((s) => s.source === m.source && s.target === m.target);
    const value = signed?.[res.key];
    if (!signed || !value || typeof value !== "object") return m;
    hit += 1;
    return { ...m, [field]: value as Record<string, unknown> };
  });
  return { next, hit };
}

/** Map `default:<value>` on the named target. A blank value is refused (it would not be "non-empty"). */
export function applyOracleDefaultValue(
  mappings: EditableMapping[],
  target: string,
  value: string,
): { next: EditableMapping[]; hit: number } {
  if (!value.trim()) return { next: mappings, hit: 0 };
  let hit = 0;
  const next = mappings.map((m) => {
    if (!target || m.target.toLowerCase() !== target.toLowerCase()) return m;
    hit += 1;
    return applyDefaultValue(m, value);
  });
  return { next, hit };
}

export type OracleActionOutcome =
  | { status: "applied"; next: EditableMapping[]; title: string; message: string }
  | { status: "cancelled" }
  | { status: "failed"; title: string; message: string }
  | { status: "open_connectors" };

export interface OracleActionDeps {
  mappings: EditableMapping[];
  destTable: string;
  ask: (options: AskOptions) => Promise<Record<string, string> | null>;
  sign: (body: OracleEmptySignBody) => Promise<OracleEmptySignResponse>;
}

/**
 * Perform one gate action. Nothing is claimed until it took effect: a signing
 * the API refused, a column no mapping carries, or a cancelled dialog changes
 * nothing and says so.
 */
export async function performOracleEmptyAction(
  action: OracleEmptyAction,
  deps: OracleActionDeps,
): Promise<OracleActionOutcome> {
  const kind = String(action.kind);
  if (kind === ORACLE_FIX_CONNECTION) return { status: "open_connectors" };
  if (kind === ORACLE_DEFAULT_TRANSFORM) {
    const target = String(action.column || action.target || "");
    const answer = await deps.ask({
      title: `Default for empty values in ${target}`,
      message: "Empty and blank values are written as this value; every other value is unchanged.",
      confirmLabel: "Map default",
      fields: [{ name: "value", label: "Value written for empty cells", required: true }],
    });
    if (!answer) return { status: "cancelled" };
    const value = answer.value ?? "";
    if (!value.trim()) {
      return { status: "failed", title: "No value", message: "Name a non-blank value; nothing was changed." };
    }
    const { next, hit } = applyOracleDefaultValue(deps.mappings, target, value);
    if (!hit) {
      return { status: "failed", title: "Column not found", message: `No mapping writes ${target}; nothing was changed.` };
    }
    return {
      status: "applied", next, title: "Default mapped — re-validating",
      message: `${target} now writes "${value}" for empty values. Re-running Validate.`,
    };
  }
  if (kind !== ORACLE_EMPTY_AS_NULL && kind !== ORACLE_EMPTY_CHECK_NOW) {
    return { status: "failed", title: "Not available in Studio", message: action.label || kind };
  }
  const columns = namedColumns(action);
  let reason = "";
  if (kind === ORACLE_EMPTY_AS_NULL) {
    const answer = await deps.ask({
      title: `Create ${columns.join(", ")} as NULL-able`,
      message: "Oracle stores '' as NULL. The column is created without NOT NULL and empty strings land as NULL. "
        + "This is signed as a risk contract in your name.",
      confirmLabel: "Sign and re-validate",
      tone: "warning",
      fields: [{ name: "reason", label: "Why empty strings may land as NULL", required: true }],
    });
    if (!answer) return { status: "cancelled" };
    reason = (answer.reason ?? "").trim();
    if (!reason) return { status: "failed", title: "No reason", message: "A reason is required; nothing was signed." };
  } else {
    const answer = await deps.ask({
      title: `Check ${columns.join(", ")} now`,
      message: action.label,
      confirmLabel: "Check now",
      fields: [],
    });
    if (!answer) return { status: "cancelled" };
  }
  const body = oracleSignBody(action, deps.mappings, deps.destTable, reason || undefined);
  if (!body.mappings.length) {
    return { status: "failed", title: "Column not found", message: `No mapping writes ${columns.join(", ")}.` };
  }
  let res: OracleEmptySignResponse;
  try {
    res = await deps.sign(body);
  } catch (err) {
    return { status: "failed", title: "Not recorded", message: err instanceof Error ? err.message : String(err) };
  }
  const { next, hit } = mergeOracleSigned(deps.mappings, res);
  if (!hit) return { status: "failed", title: "Not recorded", message: "The server signed nothing." };
  return kind === ORACLE_EMPTY_AS_NULL
    ? { status: "applied", next, title: "NULL-able approved — re-validating",
      message: `Signed for ${columns.join(", ")}. Re-running Validate.` }
    : { status: "applied", next, title: "Checking now — re-validating",
      message: `Validate reads ${columns.join(", ")} first, within its longer budget.` };
}
