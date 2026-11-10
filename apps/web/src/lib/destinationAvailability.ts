/**
 * Destination engines this host refuses (D276).
 *
 * On the hosted cloud SQLite is not a destination: the file would live on one
 * server. The API says so in `/transfer/capabilities`
 * (`destination_databases_unavailable`: id, reason, message); the browser never
 * guesses it from the hostname.
 */
import { getConnectorDefaults } from "./connectorTypes";

export interface UnavailableDestType {
  id: string;
  label: string;
  /** Short label from the API, e.g. "Not available on cloud". */
  reason: string;
  /** Full operator sentence from the API. */
  message?: string;
}

export function unavailableDestTypesFromCapabilities(caps: unknown): UnavailableDestType[] {
  const raw = (caps as { destination_databases_unavailable?: unknown } | null)
    ?.destination_databases_unavailable;
  if (!Array.isArray(raw)) return [];
  const out: UnavailableDestType[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object") continue;
    const { id, reason, message } = entry as { id?: unknown; reason?: unknown; message?: unknown };
    if (typeof id !== "string" || !id) continue;
    out.push({
      id,
      label: getConnectorDefaults(id).label,
      reason: typeof reason === "string" && reason ? reason : "Not available here",
      ...(typeof message === "string" && message ? { message } : {}),
    });
  }
  return out;
}
