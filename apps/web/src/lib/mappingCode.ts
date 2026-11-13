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
import { engineTransformToUi, isIntentionalOmit, type EditableMapping } from "./mapping";
import { stampStepSourceTable, type ShapeStepWire } from "./shape";

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

/** A row with no transform and no omission: what an imported pair starts from. */
function clearedRow(r: EditableMapping): EditableMapping {
  return {
    ...r,
    transform: "none",
    engineTransform: undefined,
    intentionalOmit: undefined,
    omitReason: undefined,
    omitReasonText: undefined,
    archiveReference: undefined,
  } as EditableMapping;
}

/**
 * Apply an imported document onto the rows; every change still needs Approve.
 *
 * The document replaces each matched row's decision outright: transforms and
 * omissions the editor held since are cleared first, then the document's
 * target, transform, type and create-new are applied, so exporting the result
 * gives the same document back.
 */
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
    const base = clearedRow(r);
    if (m.intentional_omit || m.transform === "omit") {
      return {
        ...base,
        target: "",
        transform: "omit" as EditableMapping["transform"],
        engineTransform: "omit",
        omitReason: m.omit_reason as EditableMapping["omitReason"],
        omitReasonText: m.omit_reason_text,
        approved: false,
      };
    }
    const remapped = remapToDestination(base, m.target, catalog);
    const engine = m.transform && m.transform !== "none" ? m.transform : undefined;
    const live = findDestColumn(catalog, remapped.target);
    return {
      ...remapped,
      transform: engineTransformToUi(engine),
      engineTransform: engine,
      // The live table's type wins; a column still to create takes the document's.
      destType: live ? remapped.destType : m.target_type || remapped.destType,
      ...(live ? {} : { createNew: Boolean(m.create_new) || remapped.createNew }),
      approved: false,
    };
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

export interface TemplateUnbound {
  source: string;
  target: string;
  missing: string;
}

/** One line per template pair the server could not re-bind, saying why. */
export function templateUnboundNote(u: TemplateUnbound): string {
  const head = `${u.source} → ${u.target}`;
  if (u.missing === "target_unwritable") {
    return `${head}: ${u.target} is filled by the database (generated or identity), so it is left alone.`;
  }
  if (u.missing === "both") return `${head}: no matching source or destination column here.`;
  return `${head}: no matching ${u.missing} column here (or it is already bound).`;
}

/**
 * Add computed-column steps from the Map step to the Transform steps.
 *
 * Each step is stamped with the stream the Map step is editing (only when the
 * run has two or more streams, as Transform's own steps are), and replaces a
 * step that already produces the same column on that same stream.
 */
export function mergeComputedSteps(
  prev: ShapeStepWire[],
  added: ShapeStepWire[],
  stream: { table: string; tableCount: number },
): ShapeStepWire[] {
  const stamped = added.map((s) => stampStepSourceTable(s, stream.table, stream.tableCount));
  const key = (s: ShapeStepWire) => `${s.source_table ?? ""}\u0000${s.op}:${String(s.options?.to ?? "")}`;
  const replaced = new Set(stamped.map(key));
  return [...prev.filter((p) => !replaced.has(key(p))), ...stamped];
}
