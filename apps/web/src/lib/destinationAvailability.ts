/**
 * Destinations this host refuses (D276).
 *
 * On the hosted cloud a destination that writes to the server's own disk is
 * not offered: SQLite, file exports, and the local-file variant of DuckDB and
 * Iceberg. The API says so in `/transfer/capabilities`
 * (`destination_databases_unavailable`, `destination_file_formats_unavailable`,
 * `destination_local_variants_unavailable`: id, reason, message); the browser
 * never guesses it from the hostname.
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

/** Why file exports are refused on this host (`destination_file_formats_unavailable`), or null. */
export interface FileExportAvailability {
  reason: string;
  message?: string;
}

export function fileExportUnavailableFromCapabilities(caps: unknown): FileExportAvailability | null {
  const raw = (caps as { destination_file_formats_unavailable?: unknown } | null)
    ?.destination_file_formats_unavailable;
  if (!Array.isArray(raw)) return null;
  for (const entry of raw) {
    if (!entry || typeof entry !== "object") continue;
    const { reason, message } = entry as { reason?: unknown; message?: unknown };
    return {
      reason: typeof reason === "string" && reason ? reason : "Not available here",
      ...(typeof message === "string" && message ? { message } : {}),
    };
  }
  return null;
}

/**
 * An offered engine whose local-file variant this host refuses
 * (`destination_local_variants_unavailable`): DuckDB stays offered for
 * MotherDuck, Iceberg for a remote warehouse and catalog.
 */
export interface LocalVariantNote {
  id: string;
  engine: string;
  /** e.g. "Local file". */
  variant: string;
  reason: string;
  message?: string;
}

export function localVariantNotesFromCapabilities(caps: unknown): LocalVariantNote[] {
  const raw = (caps as { destination_local_variants_unavailable?: unknown } | null)
    ?.destination_local_variants_unavailable;
  if (!Array.isArray(raw)) return [];
  const out: LocalVariantNote[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object") continue;
    const { id, engine, variant, reason, message } = entry as Record<string, unknown>;
    if (typeof id !== "string" || !id) continue;
    out.push({
      id,
      engine: typeof engine === "string" && engine ? engine : id,
      variant: typeof variant === "string" && variant ? variant : "Local file",
      reason: typeof reason === "string" && reason ? reason : "Not available here",
      ...(typeof message === "string" && message ? { message } : {}),
    });
  }
  return out;
}

/** The note for the selected destination type (catalog id or driver), if any. */
export function localVariantNoteFor(
  notes: LocalVariantNote[],
  destType: string,
  driverType = "",
): LocalVariantNote | undefined {
  if (!destType) return undefined;
  return notes.find((n) => n.id === destType || n.engine === destType || (driverType !== "" && n.engine === driverType));
}
