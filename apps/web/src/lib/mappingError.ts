/**
 * Map-step error surfacing (D-BP-10).
 *
 * When the egress allowlist blocks the connector host, schema probes return a
 * refusal message — but nothing named the cause, so the Map step stalled on
 * "Preparing column mappings" forever with no error. These helpers turn a
 * refusal into an actionable inline error: the blocked host and the setting
 * to change, plus a timeout fallback when the probe never returns.
 */

/** Operator-visible copy for the map-step watchdog timeout. */
export const MAPPING_TIMEOUT_MS = 90_000;
export const mappingTimeoutCopy =
  "Column mapping did not finish in 90 seconds — the API may be unreachable " +
  "or the schema probe is hanging. Check the API is up, then retry.";

const EGRESS_PATTERN =
  /egress|allowlist|non-routable|refused connector host|private (range|network|ip)/i;

/**
 * When a schema/mapping failure smells like a blocked-egress refusal, append
 * the actionable remedy: which host was refused and which setting unblocks it.
 * Non-egress messages pass through unchanged.
 */
export function withEgressHint(
  message: string,
  destHost?: string,
  sourceHost?: string,
): string {
  const msg = message || "";
  if (!EGRESS_PATTERN.test(msg)) return msg;
  // The probe refusal already carries the allowlist instruction — appending
  // the hint a second time duplicated it in the same message (D-BP-16).
  if (/_EGRESS_ALLOWLIST\b/i.test(msg)) return msg;
  const host = destHost || sourceHost || "";
  return (
    `${msg} — the destination host${host ? ` “${host}”` : ""} is blocked by ` +
    "the API's egress allowlist. Add the host to " +
    "DATAWRAP_EGRESS_ALLOWLIST on the API service, or pick a reachable host."
  );
}

/** First sentence of a mapping error — the concise line the Map step shows
 * inline; the full text lives in the collapsible Details block (D-BP-16). */
export function mappingErrorSummary(message: string): string {
  const first = (message || "").split(/(?<=[.!?])\s| — /)[0].trim();
  return first || "Column mappings could not be loaded.";
}
