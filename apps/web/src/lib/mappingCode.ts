/**
 * Map-step P2: mapping-as-code rows, imported documents, computed columns.
 *
 * The document schema, version and checksum belong to the API
 * (`services.mapping_document`); this module only converts between Studio
 * rows and the document's `spec.mappings`. Computed columns are Transform
 * (shape) steps — the one expression engine — named after the destination
 * column they feed.
 */
import { findDestColumn, remapToDestination, type DestColumnCatalog } from "./destColumnCatalog";
import { isIntentionalOmit, type EditableMapping } from "./mapping";
import type { ShapeStepWire } from "./shape";

export interface DocumentMapping {
  source: string;
  target: string;
  source_type?: string;
  target_type?: string;
  transform?: string;
  intentional_omit?: boolean;
  omit_reason?: string;
  omit_reason_text?: string;
  create_new?: boolean;
}

export function documentRows(rows: EditableMapping[]): DocumentMapping[] {
  return rows.map((m) => {
    if (isIntentionalOmit(m)) {
      return {
        source: m.source,
        target: "",
        transform: "omit",
        intentional_omit: true,
        ...(m.omitReason ? { omit_reason: m.omitReason } : {}),
        ...(m.omitReasonText ? { omit_reason_text: m.omitReasonText } : {}),
      };
    }
    const out: DocumentMapping = { source: m.source, target: m.target };
    if (m.inferredType) out.source_type = m.inferredType;
    if (m.destType) out.target_type = m.destType;
    out.transform = m.engineTransform || "none";
    if (m.createNew || m.existsInDestination === false) out.create_new = true;
    return out;
  });
}

/** Apply an imported document onto the rows; every change still needs Approve. */
export function applyImportedMappings(
  rows: EditableMapping[],
  imported: DocumentMapping[],
  catalog: DestColumnCatalog | null,
): { rows: EditableMapping[]; applied: number; unmatched: string[] } {
  const bySource = new Map(imported.map((m) => [m.source, m]));
  const known = new Set(rows.map((r) => r.source));
  let applied = 0;
  const next = rows.map((r) => {
    const m = bySource.get(r.source);
    if (!m) return r;
    applied += 1;
    if (m.intentional_omit || m.transform === "omit") {
      return {
        ...r,
        target: "",
        transform: "omit" as EditableMapping["transform"],
        omitReason: (m.omit_reason || r.omitReason) as EditableMapping["omitReason"],
        omitReasonText: m.omit_reason_text || r.omitReasonText,
        approved: false,
      };
    }
    return remapToDestination(r, m.target, catalog);
  });
  return { rows: next, applied, unmatched: imported.map((m) => m.source).filter((s) => !known.has(s)) };
}

export type ComputedSource =
  | { kind: "expression"; target: string; expression: string }
  | { kind: "constant"; target: string; value: string };

export function computedStep(c: ComputedSource): ShapeStepWire {
  return c.kind === "expression"
    ? { op: "derive_column", options: { to: c.target, expression: c.expression } }
    : { op: "constant_column", options: { to: c.target, value: c.value } };
}

/**
 * CURRENT_TIMESTAMP is not a computed value here: the Transform engine is
 * deterministic so preview, Validate, Execute and a resume write the same
 * row. A load time comes from the destination's own clock default.
 */
export function loadTimeAdvice(
  column: { name: string; hasDefault: boolean; default: string | null } | undefined,
): { ok: boolean; message: string } {
  if (column?.hasDefault) {
    return {
      ok: true,
      message: `Leave ${column.name} unmapped: the destination stamps ${column.default ?? "its default"} on insert.`,
    };
  }
  return {
    ok: false,
    message:
      "A clock is not a Transform value: preview, Validate, Execute and a resume would each see a different time. "
      + "Give the destination column a DEFAULT CURRENT_TIMESTAMP and leave it unmapped.",
  };
}

export function destColumnForAdvice(catalog: DestColumnCatalog | null, name: string) {
  const c = findDestColumn(catalog, name);
  return c ? { name: c.name, hasDefault: c.hasDefault, default: c.default } : undefined;
}
