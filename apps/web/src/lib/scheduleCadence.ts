import type { PipelineSchedule } from "./types";

const INTERVAL_LABEL: Record<string, string> = {
  hourly: "Every hour",
  daily: "Daily",
  weekly: "Weekly",
};

/** Operator cadence. The server label wins; a cron without one is still not the preset. */
export function scheduleCadenceLabel(
  sched: Pick<PipelineSchedule, "interval" | "cron" | "cadence_label">,
): string {
  if (sched.cadence_label) return sched.cadence_label;
  if (sched.cron) return `Cron ${sched.cron}`;
  return INTERVAL_LABEL[sched.interval] ?? sched.interval;
}

export function storedSchedulePreset(
  sched: Pick<PipelineSchedule, "interval" | "interval_preset"> | null | undefined,
): "hourly" | "daily" | "weekly" {
  const candidates = [sched?.interval_preset, sched?.interval];
  for (const value of candidates) {
    if (value === "hourly" || value === "daily" || value === "weekly") return value;
  }
  return "daily";
}
