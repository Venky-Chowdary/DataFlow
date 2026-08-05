import type { EditableMapping } from "./mapping";
import { mappingRequiresRiskAck } from "./mapping";
import { GATE_CATALOG } from "./preflightGates";
import type { PreflightGate, PreflightResult } from "./types";

/** Canonical local gate order — matches GATE_CATALOG (unique IDs). */
const GATE_IDS = GATE_CATALOG
  .map((g) => g.id)
  .filter((id) => id !== "schema_drift") as string[];

function applyTransform(value: unknown, transform?: string): unknown {
  if (value == null || value === "") return value;
  const s = String(value);
  switch (transform) {
    case "trim":
      return s.trim();
    case "upper":
      return s.toUpperCase();
    case "lower":
      return s.toLowerCase();
    case "hash_pii": {
      let h = 5381;
      for (let i = 0; i < s.length; i += 1) h = (h * 33) ^ s.charCodeAt(i);
      return `sha256:${(h >>> 0).toString(16).padStart(8, "0")}`;
    }
    case "datetime":
    case "date_iso":
      return s;
    case "decimal":
    case "cast_number": {
      const n = Number(s.replace(/,/g, ""));
      return Number.isFinite(n) ? n : value;
    }
    case "boolean":
    case "cast_boolean": {
      // Strict wire only — match transform_engine._STRICT_BOOL_* (no yes/y invent).
      const t = s.toLowerCase();
      if (["true", "t", "1"].includes(t)) return true;
      if (["false", "f", "0"].includes(t)) return false;
      return value;
    }
    default:
      return value;
  }
}

export interface LocalPreflightInput {
  columns: string[];
  rowCount: number;
  mappings: EditableMapping[];
  sampleRows?: Record<string, unknown>[];
  confidenceThreshold?: number;
  destKind?: "database" | "file_export";
}

/** True when this preflight was produced entirely in the browser (no API gates). */
export function isLocalPreflight(preflight: { run_id?: string } | null | undefined): boolean {
  return Boolean(preflight?.run_id?.startsWith("pf_local_"));
}

/** Client-side preflight for file → file export when the API is unavailable. */
export function runLocalPreflight(input: LocalPreflightInput): PreflightResult {
  const threshold = input.confidenceThreshold ?? 0.85;
  const isFileExport = input.destKind !== "database";
  const blockers: PreflightResult["blockers"] = [];
  const gates: PreflightGate[] = [];

  const pass = (id: string, message: string, scope?: Record<string, unknown>) => {
    gates.push({
      id,
      status: "pass",
      message,
      duration_ms: 1,
      details: scope ? { evidence_scope: scope } : undefined,
    });
  };
  const skip = (id: string, message: string, scope?: Record<string, unknown>) => {
    gates.push({
      id,
      status: "skip",
      message,
      duration_ms: 0,
      details: scope ? { evidence_scope: scope } : undefined,
    });
  };
  const block = (id: string, message: string, scope?: Record<string, unknown>) => {
    gates.push({
      id,
      status: "block",
      message,
      duration_ms: 1,
      details: scope ? { evidence_scope: scope } : undefined,
    });
    blockers.push({ id, message });
  };

  const rows = input.sampleRows ?? [];

  if (!input.columns.length || input.rowCount < 1) {
    block("g1_source", "No readable rows in source file.", {
      kind: "source", coverage: "n/a", note: "No readable rows",
    });
  } else {
    pass("g1_source", `${input.rowCount.toLocaleString()} rows · ${input.columns.length} columns profiled locally.`, {
      kind: "source", coverage: "sample", sample_rows: Math.min(rows.length || input.rowCount, 20),
      note: "Browser-local file profile",
    });
  }

  if (isFileExport) {
    skip("g2_destination", "File export — no remote destination connection required.", {
      kind: "destination_connectivity", coverage: "n/a", note: "File export — no remote destination",
    });
  } else {
    block("g2_destination", "Database destination requires API preflight.", {
      kind: "destination_connectivity", coverage: "n/a", note: "Requires API",
    });
  }

  const mappedSources = new Set(input.mappings.map((m) => m.source));
  const unmapped = input.columns.filter((c) => !mappedSources.has(c));
  const intentionalOmits = input.mappings.filter(
    (m) => m.transform === "omit" || (m as { intentionalOmit?: boolean }).intentionalOmit,
  );
  const activeMaps = input.mappings.filter((m) => m.transform !== "omit");
  // GA: boolean riskAcknowledged alone never clears — need Risk Contract policy.
  const CONTINUE_POLICIES = new Set([
    "QUARANTINE_ROW",
    "SKIP_ROW",
    "CAST_AND_CONTINUE",
    "TRANSFORM_AND_CONTINUE",
    "STOP_COLUMN",
  ]);
  const hasClearingContract = (m: (typeof activeMaps)[number]) => {
    const pol = String(m.riskContract?.execution_policy || "").toUpperCase();
    return Boolean(m.riskAcknowledged && m.riskContract && CONTINUE_POLICIES.has(pol));
  };
  const riskUnacked = activeMaps.filter(
    (m) => mappingRequiresRiskAck(m) && !hasClearingContract(m),
  );
  // Ready ≡ operator Approve — never invent G4 PASS from confidence alone.
  const unapproved = activeMaps.filter((m) => !m.approved);
  const lowConfidence = activeMaps.filter(
    (m) => !m.approved && m.confidence < threshold && !hasClearingContract(m),
  );
  if (unmapped.length > 0) {
    block("g3_schema_contract", `${unmapped.length} source column(s) have no mapping.`, {
      kind: "schema_contract", coverage: "full_schema", note: "Unmapped source columns",
    });
  } else if (riskUnacked.length > 0) {
    block(
      "g3_schema_contract",
      `${riskUnacked.length} mapping(s) have unacked fidelity risk — Accept risk on Map before Validate can pass.`,
      { kind: "schema_contract", coverage: "full_schema", columns: input.mappings.length },
    );
  } else {
    const omitNote = intentionalOmits.length
      ? ` · ${intentionalOmits.length} intentionally omitted`
      : "";
    pass("g3_schema_contract", `All source columns accounted for (mapped or omitted).${omitNote}`, {
      kind: "schema_contract", coverage: "full_schema", columns: input.mappings.length,
      intentional_omits: intentionalOmits.map((m) => m.source),
    });
  }

  if (riskUnacked.length > 0) {
    block(
      "g4_mapping_confidence",
      `${riskUnacked.length} mapping(s) need Accept risk on Map (lossy/mutate/STRUCT/specialty).`,
      { kind: "mapping_confidence", coverage: "full_schema", columns: input.mappings.length },
    );
  } else if (unapproved.length > 0) {
    block(
      "g4_mapping_confidence",
      `${unapproved.length} mapping(s) need Approve on Map before Validate can pass.`,
      { kind: "mapping_confidence", coverage: "full_schema", columns: input.mappings.length },
    );
  } else if (lowConfidence.length > 0) {
    block(
      "g4_mapping_confidence",
      `${lowConfidence.length} mapping(s) below ${(threshold * 100).toFixed(0)}% confidence — review in Map step.`,
      { kind: "mapping_confidence", coverage: "full_schema", columns: input.mappings.length },
    );
  } else {
    pass("g4_mapping_confidence", `${input.mappings.length} mapping(s) operator-approved for Validate.`, {
      kind: "mapping_confidence", coverage: "full_schema", columns: input.mappings.length,
    });
  }

  let transformOk = true;
  for (const m of input.mappings) {
    for (const row of rows.slice(0, 20)) {
      try {
        applyTransform(row[m.source], m.transform === "none" ? undefined : m.transform);
      } catch {
        transformOk = false;
        break;
      }
    }
    if (!transformOk) break;
  }
  if (!transformOk) {
    block("g5_dry_run", "A transform failed on sample rows.", {
      kind: "dry_run", coverage: "sample", sample_rows: Math.min(rows.length, 20),
    });
  } else {
    pass("g5_dry_run", `Dry-run transforms passed on ${Math.min(rows.length, 20)} sample row(s).`, {
      kind: "dry_run", coverage: "sample", sample_rows: Math.min(rows.length, 20),
    });
  }

  if (isFileExport) {
    skip("g6_target_ddl", "No DDL for file export.", {
      kind: "target_ddl", coverage: "n/a", note: "File export — no DDL",
    });
  } else {
    block("g6_target_ddl", "DDL validation requires API.", {
      kind: "target_ddl", coverage: "n/a", note: "Requires API",
    });
  }

  pass("g7_capacity", `${input.rowCount.toLocaleString()} rows within local export capacity.`, {
    kind: "capacity", coverage: "estimated", note: "Local export capacity estimate",
  });

  if (isFileExport) {
    skip("g8_reconciliation", "Reconciliation runs after API-backed transfer.", {
      kind: "reconciliation", coverage: "pending", note: "Post-write Gate-8 requires API transfer",
    });
  } else {
    block("g8_reconciliation", "Reconciliation requires API.", {
      kind: "reconciliation", coverage: "n/a", note: "Requires API",
    });
  }

  skip("g9_data_integrity", "Browser-local export — uniqueness/null integrity requires API sample probe.", {
    kind: "data_integrity", coverage: "n/a",
    note: "Not a full-table integrity probe — do not treat as gate-pass evidence",
  });

  skip("g9_sync_contract", "Full refresh file export — sync contract not applicable.", {
    kind: "sync_contract", coverage: "n/a",
  });
  skip("g10_schema_policy", "Browser-only — schema policy gate skipped; requires API.", {
    kind: "schema_policy", coverage: "n/a",
  });
  skip("g11_validation_posture", "Browser-only — validation posture skipped; requires API.", {
    kind: "validation_posture", coverage: "n/a",
  });

  const passedCount = gates.filter((g) => g.status === "pass").length;
  const skippedCount = gates.filter((g) => g.status === "skip").length;
  const totalGates = GATE_IDS.length;
  const passed = blockers.length === 0;
  const avgConfidence =
    input.mappings.length > 0
      ? input.mappings.reduce((sum, m) => sum + m.confidence, 0) / input.mappings.length
      : 0;

  // Local export never runs remote DDL / destination probe / post-write reconcile —
  // grade as "review" so the UI cannot be read as production-governed proof.
  // Do not invent a numeric quality score — sample mapping confidence ≠ profiled quality.
  const qualityGrade: "excellent" | "good" | "review" | "not_profiled" = "not_profiled";
  const confidenceBand: "high" | "medium" | "low" =
    avgConfidence >= 0.9 ? "high" : avgConfidence >= 0.75 ? "medium" : "low";
  // Cap readiness — skipped production gates must not look like a full API pass.
  const readinessCap = isFileExport ? 72 : 99;
  const readinessScore = passed
    ? Math.round(Math.min(readinessCap, 55 + avgConfidence * 17))
    : Math.round(avgConfidence * 50);

  const localWarnings = isFileExport
    ? [
        "Browser-only validation — destination reachability, DDL, and reconciliation were not executed.",
        "No Job Theater proof or destination checksum until the API runs this route.",
        `${skippedCount} gate(s) skipped because they require a live API-backed destination.`,
      ]
    : ["Database destinations require API preflight — local checks cannot approve remote writes."];

  return {
    passed,
    passed_count: passedCount,
    total_gates: totalGates,
    readiness_score: readinessScore,
    run_id: `pf_local_${Math.random().toString(16).slice(2, 10)}`,
    gates,
    blockers,
    proof_bundle: {
      passed,
      semantic_mapping_score: avgConfidence,
      semantic_notes: [
        "Local browser validation — start the API for production gates (destination probe, DDL, reconcile).",
        ...localWarnings.slice(0, 2),
      ],
      quality_score: null,
      confidence_band: confidenceBand,
      quality_grade: qualityGrade,
      evidence_summary: passed
        ? "Local preflight cleared mapping/transform checks in the browser. Destination DDL, capacity, and reconciliation still require the API."
        : "Resolve blockers before exporting.",
      compliance: {
        risk_score: input.mappings.some((m) => m.isPii && m.transform !== "hash_pii") ? 0.55 : 0.28,
        requires_review: true,
        tags: [
          "local_preflight",
          ...(isFileExport ? ["file_export"] : []),
          ...input.mappings.filter((m) => m.isPii).map((m) => `pii:${m.source}`),
        ],
      },
      reconciliation: {
        passed: false,
        preview: true,
        phase: "pre_write_simulation",
        post_write_pending: true,
        message: "Not run — full reconciliation requires an API-backed transfer.",
      },
      transfer_decision: {
        decision: passed ? "review" : "block",
        blockers: blockers.map((b) => b.message),
        reason: passed
          ? "Local export checks passed — treat as demo-grade until API validation runs."
          : blockers[0]?.message ?? "Validation failed.",
        warnings: localWarnings,
      },
    },
  };
}
