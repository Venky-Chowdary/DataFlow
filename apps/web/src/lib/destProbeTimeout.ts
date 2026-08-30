/**
 * How long the destination-only schema probe may run before Studio calls the
 * destination unknown.
 *
 * A warehouse resumes a suspended compute cluster on the first statement, so a
 * cold Snowflake/BigQuery/Databricks answer legitimately takes minutes. An OLTP
 * engine either answers a catalog query in well under a second or is not
 * reachable, and waiting three minutes there left the operator on a disabled
 * "Reading destination…" control with no way to reach the retry.
 */
export type DestProbeSpeedClass = "warehouse" | "oltp";

export const DEST_PROBE_TIMEOUT_MS: Record<DestProbeSpeedClass, number> = {
  warehouse: 180_000,
  oltp: 45_000,
};

const WAREHOUSE_DRIVERS = [
  "snowflake",
  "bigquery",
  "databricks",
  "redshift",
  "synapse",
  "athena",
  "fabric",
  "duckdb",
  "clickhouse",
];

export function destProbeSpeedClass(destType: string | undefined): DestProbeSpeedClass {
  const t = (destType || "").toLowerCase();
  return WAREHOUSE_DRIVERS.some((d) => t.includes(d)) ? "warehouse" : "oltp";
}

/**
 * How long an *automatic* probe waits after the same destination failed.
 *
 * A failed probe clears the destination columns, which is itself a state change
 * that re-runs Map, which probes again — an unreachable host therefore held the
 * reload control disabled indefinitely. Operator-initiated probes (Reload,
 * Validate) always run immediately; only the automatic ones back off.
 */
export const DEST_PROBE_FAILURE_COOLDOWN_MS = 15_000;

export function shouldSkipAutoDestProbe(
  lastFailure: { key: string; at: number } | null,
  key: string,
  now: number,
): boolean {
  if (!lastFailure || lastFailure.key !== key) return false;
  return now - lastFailure.at < DEST_PROBE_FAILURE_COOLDOWN_MS;
}
