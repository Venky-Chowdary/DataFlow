/**
 * What an incremental cursor means, and what that permits.
 *
 * Mirrors `apps/api/services/cursor_semantics.py`: the engine is the authority,
 * and this exists so Studio states the same verdict before Validate runs rather
 * than showing "Valid" for a contract the engine will refuse. A column's name
 * cannot establish its meaning — `created_at` exists on tables whose rows are
 * updated in place — so nothing here is inferred from a name.
 */

export const CURSOR_SEMANTICS = [
  "modification_timestamp",
  "insert_only",
  "monotonic_sequence",
  "cdc_position",
  "business_date",
] as const;

export type CursorSemantics = (typeof CURSOR_SEMANTICS)[number] | "";

export const CURSOR_SEMANTICS_LABELS: Record<string, string> = {
  modification_timestamp: "Updated on every change (insert and update)",
  insert_only: "Set on insert; rows are never updated",
  monotonic_sequence: "Always-increasing generated value (identity / sequence)",
  cdc_position: "Change-log position from the source",
  business_date: "Business or calendar date (not insert order)",
};

/** Semantics under which a row changed after it was read is read again. */
const CAPTURES_UPDATES = new Set(["modification_timestamp", "cdc_position"]);

/** Semantics under which a new row always sorts after the watermark. */
const MONOTONIC_ON_INSERT = new Set([
  "insert_only",
  "modification_timestamp",
  "monotonic_sequence",
  "cdc_position",
]);

const MODES_REQUIRING_CURSOR = new Set([
  "incremental_append",
  "incremental_deduped",
  "cdc",
]);

/** Modes whose contract is that a changed source row reaches the destination. */
const MODES_PROMISING_UPDATE_CAPTURE = new Set([
  "incremental_deduped",
  "scd2",
  "mirror",
  "cdc",
]);

export interface CursorSemanticsVerdict {
  status: "ok" | "block" | "not_applicable";
  reason: string;
  /** The one thing the operator should do. Alternatives are secondary. */
  primaryAction: string;
  alternatives: string[];
  capturesUpdates: boolean;
  monotonicOnInsert: boolean;
}

const NOT_APPLICABLE: CursorSemanticsVerdict = {
  status: "not_applicable",
  reason: "",
  primaryAction: "",
  alternatives: [],
  capturesUpdates: false,
  monotonicOnInsert: false,
};

export function evaluateCursorSemantics(input: {
  syncMode: string;
  cursorField: string;
  declared: string;
  validationMode?: string;
}): CursorSemanticsVerdict {
  const mode = (input.syncMode || "").trim().toLowerCase();
  const cursor = (input.cursorField || "").trim();
  if (!MODES_REQUIRING_CURSOR.has(mode) && mode !== "scd2") return NOT_APPLICABLE;
  if (!cursor) return NOT_APPLICABLE;

  const declared = (input.declared || "").trim().toLowerCase();
  if (declared && !CURSOR_SEMANTICS.includes(declared as never)) {
    return {
      status: "block",
      reason: `Unknown cursor semantics "${declared}".`,
      primaryAction: `Declare what ${cursor} means in the source`,
      alternatives: [],
      capturesUpdates: false,
      monotonicOnInsert: false,
    };
  }

  const capturesUpdates = CAPTURES_UPDATES.has(declared);
  const monotonic = MONOTONIC_ON_INSERT.has(declared);
  const promisesUpdates = MODES_PROMISING_UPDATE_CAPTURE.has(mode);

  if (monotonic) {
    return {
      status: "ok",
      reason:
        promisesUpdates && !capturesUpdates
          ? `${cursor} does not move when a row is updated — this sync reflects `
            + `source changes only because you declared the source makes none. `
            + `New rows are captured in full.`
          : `${cursor} captures ${capturesUpdates ? "inserts and updates" : "inserts only"}.`,
      primaryAction: "",
      alternatives: [],
      capturesUpdates,
      monotonicOnInsert: true,
    };
  }

  let reason: string;
  if (declared === "business_date") {
    reason =
      `${cursor} comes from a calendar rather than the insert order, so a row `
      + `inserted with an earlier date stays behind the watermark and is never read.`;
  } else if (promisesUpdates) {
    reason =
      `This sync keeps the destination in step with changed source rows, but `
      + `nothing states that ${cursor} moves when a row changes. An update that `
      + `leaves it untouched is never re-read, and the run still reports success.`;
  } else if ((input.validationMode || "strict").toLowerCase() === "balanced") {
    return {
      status: "ok",
      reason:
        `${cursor} is undeclared — balanced validation accepts it, and this run `
        + `claims insert capture only: neither completeness nor update capture is proven.`,
      primaryAction: "",
      alternatives: [],
      capturesUpdates: false,
      monotonicOnInsert: false,
    };
  } else {
    reason =
      `Nothing states that a row inserted into this source always carries a `
      + `${cursor} value later than the ones already read. A backdated insert lands `
      + `behind the watermark and is skipped permanently, with no error.`;
  }

  return {
    status: "block",
    reason,
    primaryAction: declared
      ? "Select a cursor the source assigns in insert order, or maintains on every change"
      : `Declare what ${cursor} means in the source`,
    alternatives: [
      "Switch to Full refresh to re-read the whole source each run",
      "Use CDC, which reads the source's change log instead of a column",
    ],
    capturesUpdates,
    monotonicOnInsert: false,
  };
}

/** True when this stream's cursor contract is not yet safe for the sync mode. */
export function cursorContractNeedsReview(input: {
  syncMode: string;
  cursorField: string;
  declared: string;
  validationMode?: string;
}): boolean {
  return evaluateCursorSemantics(input).status === "block";
}
