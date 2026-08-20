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
