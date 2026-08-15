/**
 * CDC delivery guarantee — Studio SSOT.
 *
 * Default is at_least_once. exactly_once is opt-in dest-owned watermark
 * delivery (sqlite wired). Never invent at_most_once. Append-only is
 * incompatible with exactly-once.
 */

export const CDC_DELIVERY_AT_LEAST_ONCE = "at_least_once";
export const CDC_DELIVERY_EXACTLY_ONCE = "exactly_once";

export type CdcDeliveryGuarantee =
  | typeof CDC_DELIVERY_AT_LEAST_ONCE
  | typeof CDC_DELIVERY_EXACTLY_ONCE;

const EOS_WIRED_DESTS = new Set(["sqlite"]);

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

export function studioDeliveryGuarantee(input: {
  syncMode?: string;
  deliveryGuarantee?: string;
  allowAppendOnly?: boolean;
}): CdcDeliveryGuarantee {
  if (String(input.syncMode || "").trim().toLowerCase() !== "cdc") {
    return CDC_DELIVERY_AT_LEAST_ONCE;
  }
  if (input.allowAppendOnly) {
    return CDC_DELIVERY_AT_LEAST_ONCE;
  }
  return namedCdcDeliveryGuarantee(input.deliveryGuarantee);
}
