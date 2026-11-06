/**
 * The confirm a destination recovery asks before it acts (QA C9 review).
 *
 * The plan comes from the API's dry run, the same disclosure the MCP preview
 * carries: a rebuild with no key REPLACES the table, and the dialog says so,
 * names the table, and is a danger confirm. Nothing here decides what happens;
 * it only words what the server said it will do.
 */
import type { ConfirmOptions } from "../components/ui/ConfirmDialog";
import type { DestinationRecoveryPlan } from "./api";
import type { PipelineSchedule } from "./types";

export function recoveryConfirmOptions(
  plan: DestinationRecoveryPlan,
  sched: Pick<PipelineSchedule, "name" | "dest_table">,
): ConfirmOptions {
  const table = sched.dest_table || "the destination";
  const rebuild = plan.action === "rebuild";
  return {
    title: rebuild
      ? (plan.destructive ? `Replace ${table}?` : `Rebuild ${table} from the source?`)
      : `Accept ${table} as it is?`,
    message: plan.effect,
    confirmLabel: rebuild ? (plan.destructive ? "Replace and rebuild" : "Rebuild") : "Accept (rebaseline)",
    cancelLabel: "Cancel",
    tone: plan.destructive ? "danger" : rebuild || plan.destination_missing ? "warning" : "default",
    facts: [
      { label: "Schedule", value: sched.name || "This schedule" },
      { label: "Destination table", value: table },
      { label: "Replaces the table", value: plan.destructive ? "Yes" : "No" },
    ],
  };
}
