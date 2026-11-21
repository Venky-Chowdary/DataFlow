/**
 * "Explain this" text (UI pass 2, item 6): deterministic, from the gate
 * catalog (lib/preflightGates.ts) and the verdict catalog (lib/verdict.ts).
 * No model is asked; the popover says so. Each explanation is three short
 * answers: what this checks or means, what this result means, what to do.
 */
import { gateCatalogEntry } from "./preflightGates";
import { verdictPresentation, type VerdictState } from "./verdict";
import type { CheckOutcome } from "./gateChecklist";

export interface Explanation {
  title: string;
  what: string;
  result: string;
  next: string;
  /** Where the words come from: never a model. */
  source: string;
}

const CATALOG_SOURCE = "Written from Datawrap's check catalog, not by AI.";

/** What to do when a gate fails, for the gates people meet most. */
const GATE_FIX: Record<string, string> = {
  g1_source: "Test the source connection, then check the table or query still exists and this login can read it.",
  g2_destination: "Give this login write (and create, for new tables) on the destination schema, or pick a schema it can write.",
  g3_schema_contract: "Widen the destination column, or remap the source column to a type that holds its values.",
  g4_mapping_confidence: "Review the low-confidence mappings on Map and approve or change each one.",
  g5_dry_run: "Open the failing sample rows; fix the transform or the mapping that rejects them.",
  g6_target_ddl: "Change the column types or keys the DDL cannot create, or let Datawrap create a new table.",
  g7_capacity: "Free space on the destination or load fewer rows per run.",
  g8_reconciliation: "Check the identity mappings and keys on Map: the sample did not survive the write path unchanged.",
  g9_data_integrity: "Fix the source rows named in the findings (encoding, required nulls, duplicate keys), or hold them in quarantine.",
  g9_sync_contract: "Pick a cursor column and primary key that the sync mode needs, or change the sync mode.",
  g10_schema_policy: "Accept the schema change on Map, or change the schema policy for this route.",
  g13_source_coverage: "Map each listed source column, or mark it as intentionally left out.",
  g14_destination_requirements: "Map, default or generate each required destination column the findings name.",
};

export function explainGate(gateId: string, outcome: CheckOutcome, message = ""): Explanation {
  const entry = gateCatalogEntry(gateId);
  const title = entry.label;
  const what = entry.rule || "This check guards the transfer before any row is written.";
  const fix = GATE_FIX[entry.id] ?? "Open the findings below, fix the cause, then re-run Validate.";
  const said = message.trim() ? ` The engine said: "${message.trim().replace(/\s+/g, " ").slice(0, 160)}".` : "";
  switch (outcome) {
    case "blocking":
      return { title, what, result: `This check failed, so Run stays locked until it passes.${said}`, next: `${fix} Then re-run Validate.`, source: CATALOG_SOURCE };
    case "could_not_run":
      return {
        title,
        what,
        result: `The engine could not finish this check, so nothing was proven either way. It is not counted as passed.${said}`,
        next: "Re-run Validate. If it happens again, test the connection and check this login can read the catalog.",
        source: CATALOG_SOURCE,
      };
    case "review":
      return { title, what, result: `This check found something a person should look at.${said}`, next: "Read the findings. Validate's decision at the top says whether Run is open; while it reads Review, Run stays locked.", source: CATALOG_SOURCE };
    case "after_run":
      return { title, what, result: "This proof needs the loaded data, so it runs after the transfer, not in Validate.", next: "Nothing to do now. The result appears in the run's Trust Report.", source: CATALOG_SOURCE };
    case "not_applicable":
      return { title, what, result: "Nothing on this route needs this check, so it did not run.", next: "Nothing to do.", source: CATALOG_SOURCE };
    case "pending":
      return { title, what, result: "This check is still running.", next: "Wait for Validate to finish.", source: CATALOG_SOURCE };
    default:
      return { title, what, result: "This check passed on this run.", next: "Nothing to do.", source: CATALOG_SOURCE };
  }
}

const VERDICT_NEXT: Record<VerdictState, string> = {
  proven: "Nothing to do. Sign off the Trust Report when your reviewers are ready.",
  explained: "Read the rules that explain each difference in the Trust Report, then sign off.",
  no_difference_found: "Run full verification if you need every row compared before sign-off.",
  unexplained: "Open the Trust Report's differences, fix or accept each one, then re-verify.",
  unverifiable: "Open the Trust Report to see what evidence is missing, then re-run with verification on.",
  not_checked: "Wait for the run to finish; verification starts after the load.",
  checking: "Wait a moment while the verdict loads.",
  not_loaded: "Open the Trust Report, or refresh the list.",
};

const VERDICT_HOW: Record<VerdictState, string> = {
  proven: "Every row was compared by key, and source and destination hold the same values.",
  explained: "Every row was compared. Some differ, and a rule (a cast, a rounding, a declared transform) accounts for each one.",
  no_difference_found: "The rows that were checked match, but the check was a sample or a read-back, not every row.",
  unexplained: "Rows differ between source and destination and no rule accounts for them.",
  unverifiable: "The run kept too little evidence to decide: counts only, no compare, or the run stopped first.",
  not_checked: "The run has not finished, so nothing has been compared yet.",
  checking: "The verdict is being read from the Trust Report.",
  not_loaded: "The verdict could not be read just now.",
};

export function explainVerdict(state: VerdictState): Explanation {
  const shown = verdictPresentation(state);
  return {
    title: shown.isVerdict ? `Verdict: ${shown.word}` : shown.word,
    what: shown.meaning,
    result: VERDICT_HOW[state],
    next: VERDICT_NEXT[state],
    source: "Written from Datawrap's verdict rules, not by AI.",
  };
}
