/**
 * Dest-exists shape contract — Validate consumes G15 / source_coverage.shape_contract.
 * One primary action. Extra source columns are never silent-dropped.
 */

export type DestExistsPrimaryAction =
  | "review_map"
  | "confirm_or_remap"
  | "reload_dest_schema"
  | "confirm_add"
  | "continue_validate";

export interface DestExistsShapeContract {
  shape?: string;
  headline?: string;
  detail?: string;
  primary_action?: string;
  unaccounted_sources?: string[];
  extra_source_columns?: string[];
  false_friend_sources?: string[];
  columns?: Array<{ source?: string; kind?: string }>;
  dest_only?: Array<{ target?: string; kind?: string }>;
  counts?: Record<string, number>;
  write_by?: string;
}

export interface DestExistsPrimaryCta {
  kind: string;
  label: string;
  column?: string;
}

const ACTION_LABEL: Record<string, string> = {
  review_map: "Open Map to remap extra columns",
  confirm_or_remap: "Confirm this pair",
  reload_dest_schema: "Reload destination schema",
  confirm_add: "Review ADD COLUMN proposals",
  continue_validate: "Continue — dest-only columns stay off SET",
};

const ACTION_KIND: Record<string, string> = {
  review_map: "review_mappings",
  confirm_or_remap: "confirm_or_remap",
  reload_dest_schema: "reload_dest_schema",
  confirm_add: "confirm_add",
  continue_validate: "continue_validate",
};

export function extraSourceColumnsFromContract(
  contract: DestExistsShapeContract | null | undefined,
): string[] {
  if (!contract) return [];
  const named = [
    ...(contract.extra_source_columns || []),
    ...(contract.unaccounted_sources || []),
  ];
  return [...new Set(named.map((c) => String(c || "").trim()).filter(Boolean))];
}

export function destOnlyPreserveColumns(
  contract: DestExistsShapeContract | null | undefined,
): string[] {
  if (!contract?.dest_only) return [];
  return contract.dest_only
    .filter((c) => (c.kind || "") === "dest_only_preserve" && c.target)
    .map((c) => String(c.target));
}

export function destExistsPrimaryCta(
  contract: DestExistsShapeContract | null | undefined,
): DestExistsPrimaryCta | null {
  if (!contract) return null;
  const action = String(contract.primary_action || "").trim();
  if (!action || action === "continue_validate") return null;
  const extras = extraSourceColumnsFromContract(contract);
  const friends = [
    ...(contract.false_friend_sources || []),
    ...((contract.columns || [])
      .filter((c) => (c.kind || "") === "false_friend" && c.source)
      .map((c) => String(c.source))),
  ].filter(Boolean);
  const kind = ACTION_KIND[action] || "review_mappings";
  const label = ACTION_LABEL[action] || ACTION_LABEL.review_map;
  const cta: DestExistsPrimaryCta = { kind, label };
  const focus = kind === "confirm_or_remap" ? (friends[0] || extras[0]) : extras[0];
  if (focus && (kind === "review_mappings" || kind === "confirm_or_remap" || kind === "confirm_add")) {
    cta.column = focus;
  }
  return cta;
}

export function callableExtractNote(
  preflight: {
    callable_extract?: { note?: string; mode?: string };
    source_fk_catalog?: { skipped?: boolean; note?: string };
  } | null | undefined,
  job?: {
    source_read_mode?: string;
    transfer_request?: {
      source?: {
        source_read_mode?: string;
        extra?: { source_read_mode?: string };
        [key: string]: unknown;
      };
      [key: string]: unknown;
    };
  } | null,
): string {
  const stamped = String(preflight?.callable_extract?.note || "").trim();
  if (stamped) return stamped;
  if (preflight?.source_fk_catalog?.skipped) {
    return String(preflight.source_fk_catalog.note || "").trim();
  }
  const mode = String(
    job?.source_read_mode
    || job?.transfer_request?.source?.source_read_mode
    || job?.transfer_request?.source?.extra?.source_read_mode
    || "",
  ).trim().toLowerCase();
  if (mode === "procedure" || mode === "query") {
    return (
      "Result-set snapshot — FK catalog, uniqueness GROUP BY, and "
      + "population orphan scans are not run against a procedure name."
    );
  }
  return "";
}

export function shapeContractFromPreflight(preflight: {
  source_coverage?: { shape_contract?: DestExistsShapeContract };
  gates?: Array<{ id?: string; details?: Record<string, unknown> }>;
} | null | undefined): DestExistsShapeContract | null {
  const fromCoverage = preflight?.source_coverage?.shape_contract;
  if (fromCoverage && typeof fromCoverage === "object") return fromCoverage;
  const gate = (preflight?.gates || []).find((g) => g.id === "g15_dest_exists_shape");
  const details = gate?.details;
  if (!details || typeof details !== "object") return null;
  if (!details.shape && !details.primary_action && !details.headline) return null;
  return details as DestExistsShapeContract;
}
