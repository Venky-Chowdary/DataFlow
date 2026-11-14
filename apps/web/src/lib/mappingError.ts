/**
 * Map-step error surfacing (D-BP-10).
 *
 * When the egress policy blocks the connector host, schema probes return the
 * API's classified refusal — the Map step shows it inline instead of stalling
 * on "Preparing column mappings", plus a timeout fallback when the probe never
 * returns.
 */

/** Operator-visible copy for the map-step watchdog timeout. */
export const MAPPING_TIMEOUT_MS = 90_000;
export const mappingTimeoutCopy =
  "Column mapping did not finish in 90 seconds — the API may be unreachable " +
  "or the schema probe is hanging. Check the API is up, then retry.";

/**
 * A schema/mapping failure as the API worded it.
 *
 * The API classifies an egress refusal itself (error code
 * `network.egress_blocked`: "Datawrap's network policy doesn't allow
 * connections to this host. Ask your administrator to allow this host for
 * your workspace."). This used to match the text and append a server
 * environment-variable name and "the destination host" even when the source
 * host was the one refused (ERR-39); it now passes the API's copy through.
 * The host arguments are kept for callers and ignored.
 */
export function withEgressHint(
  message: string,
  _destHost?: string,
  _sourceHost?: string,
): string {
  return message || "";
}

/** First sentence of a mapping error — the concise line the Map step shows
 * inline; the full text lives in the collapsible Details block (D-BP-16). */
export function mappingErrorSummary(message: string): string {
  const first = (message || "").split(/(?<=[.!?])\s| — /)[0].trim();
  return first || "Column mappings could not be loaded.";
}
