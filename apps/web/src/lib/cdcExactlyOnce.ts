/**
 * CDC delivery guarantee — Studio SSOT.
 *
 * Default is at_least_once. exactly_once is opt-in dest-owned watermark
 * delivery (transactional SQL wired). Never invent at_most_once.
 * Append-only is incompatible with exactly-once.
 */

export const CDC_DELIVERY_AT_LEAST_ONCE = "at_least_once";
export const CDC_DELIVERY_EXACTLY_ONCE = "exactly_once";

export type CdcDeliveryGuarantee =
  | typeof CDC_DELIVERY_AT_LEAST_ONCE
  | typeof CDC_DELIVERY_EXACTLY_ONCE;

const EOS_WIRED_DESTS = new Set([
  "sqlite",
  "postgresql",
  "postgres",
  "mysql",
  "mariadb",
  "sqlserver",
  "mssql",
  "azure_sql",
  "azure_sql_database",
  "amazon_rds_sql_server",
  "duckdb",
  "generic_sql",
  "oracle",
  "oracle_db",
  "oracle_autonomous_warehouse",
  "snowflake",
]);

export function namedCdcDeliveryGuarantee(
  raw?: string | null,
): CdcDeliveryGuarantee {
  const value = String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  if (value === "exactly_once" || value === "eos" || value === "exactlyonce") {
    return CDC_DELIVERY_EXACTLY_ONCE;
  }
  return CDC_DELIVERY_AT_LEAST_ONCE;
}

export function exactlyOnceWiredDest(destType?: string | null): boolean {
  const dest = String(destType || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  return EOS_WIRED_DESTS.has(dest);
}

/** Job / Theater payload — API fields are string | null, never invent EOS. */
export type JobStudioDeliverySource = {
  delivery_guarantee?: string | null;
  cdc_delivery?: string | null;
  exactly_once_active?: boolean | null;
  transfer_request?: { delivery_guarantee?: string | null } | null;
  destination_summary?: Record<string, unknown> | null;
} | null | undefined;

export function jobStudioDeliveryGuarantee(
  job: JobStudioDeliverySource,
): CdcDeliveryGuarantee {
  const req = job?.transfer_request?.delivery_guarantee;
  const summary = job?.destination_summary;
  const summaryActive = Boolean(summary?.exactly_once_active);
  const summaryDelivery = typeof summary?.cdc_delivery === "string"
    ? summary.cdc_delivery
    : "";
  const fromSummary =
    summaryActive
    || job?.exactly_once_active
    || summaryDelivery === "exactly_once"
      ? CDC_DELIVERY_EXACTLY_ONCE
      : "";
  return namedCdcDeliveryGuarantee(
    req || job?.delivery_guarantee || fromSummary || job?.cdc_delivery,
  );
}

export function cdcDeliveryResultCopy(input: {
  cdcDelivery?: string | null;
  exactlyOnceActive?: boolean;
  destLsn?: string | null;
  fenceEpoch?: number | null;
}): string {
  const delivery = namedCdcDeliveryGuarantee(input.cdcDelivery);
  if (input.exactlyOnceActive || delivery === CDC_DELIVERY_EXACTLY_ONCE) {
    const parts = ["exactly_once dest-owned watermark · dest authoritative · not platform-wide"];
    if (input.destLsn) parts.push(`dest LSN ${input.destLsn}`);
    if (input.fenceEpoch) parts.push(`fence ${input.fenceEpoch}`);
    return parts.join(" · ");
  }
  return "at-least-once · not platform exactly-once";
}

export function studioDeliveryGuarantee(input: {
  syncMode?: string;
  deliveryGuarantee?: string;
  allowAppendOnly?: boolean;
  callableSource?: boolean;
}): CdcDeliveryGuarantee {
  if (String(input.syncMode || "").trim().toLowerCase() !== "cdc") {
    return CDC_DELIVERY_AT_LEAST_ONCE;
  }
  if (input.allowAppendOnly || input.callableSource) {
    return CDC_DELIVERY_AT_LEAST_ONCE;
  }
  return namedCdcDeliveryGuarantee(input.deliveryGuarantee);
}
