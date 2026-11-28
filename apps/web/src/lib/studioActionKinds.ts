/**
 * Validate suggested-action kinds Studio performs end to end (ValidateDashboard
 * handleSuggestedAction + TransferPage applySuggestedAction). A kind outside
 * this set is never offered as a button and never reported as "Applied".
 */
import { STUDIO_ORACLE_EMPTY_KINDS } from "./oracleEmptyActions";

export const STUDIO_ACTION_KINDS: ReadonlySet<string> = new Set([
  "change_target_type",
  "add_transform",
  "map_column",
  "review_mappings",
  "rerun_mapping",
  "check_connection",
  "normalize_control_chars",
  "quarantine_and_rerun",
  "open_bad_data_fix",
  "fix_source_keys",
  "confirm_or_remap",
  "reload_dest_schema",
  "confirm_add",
  "continue_validate",
  "fix_orphans",
  "run_population_orphan_scan",
  ...STUDIO_ORACLE_EMPTY_KINDS,
]);

export function studioPerforms(kind: string | undefined | null): boolean {
  return STUDIO_ACTION_KINDS.has(String(kind || ""));
}
