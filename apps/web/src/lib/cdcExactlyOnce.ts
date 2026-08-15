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

export function jobStudioDeliveryGuarantee(job: {
  delivery_guarantee?: string;
  cdc_delivery?: string;
  transfer_request?: { delivery_guarantee?: string };
  destination_summary?: { exactly_once_active?: boolean; cdc_delivery?: string };
} | null | undefined): CdcDeliveryGuarantee {
  const req = job?.transfer_request?.delivery_guarantee;
  const summary = job?.destination_summary;
  const fromSummary =
    summary?.exactly_once_active || summary?.cdc_delivery === "exactly_once"
      ? CDC_DELIVERY_EXACTLY_ONCE
      : "";
  return namedCdcDeliveryGuarantee(
    req || job?.delivery_guarantee || fromSummary || job?.cdc_delivery,
  );
}

export function cdcDeliveryResultCopy(input: {
  cdcDelivery?: string | null;
  exactlyOnceActive?: boolean;
}): string {
  const delivery = namedCdcDeliveryGuarantee(input.cdcDelivery);
  if (input.exactlyOnceActive || delivery === CDC_DELIVERY_EXACTLY_ONCE) {
    return "exactly_once dest-owned watermark · not platform-wide";
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
