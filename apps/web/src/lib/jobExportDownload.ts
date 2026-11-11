/**
 * Download action for a Transfer Studio file export.
 *
 * A server job's export is fetched through `GET /api/v1/jobs/{id}/export/download`:
 * on the hosted cloud the file lives in a private bucket and the API answers
 * with a short-lived presigned URL (+ the SHA-256 Gate-8 bound to it); a
 * self-hosted export answers with its `/transfer/download/<file>` path. 410
 * means the export passed its retention and was deleted.
 */
import type { TransferResult } from "./types";

export interface JobExportDownload {
  storage: "object_store" | "local";
  url: string;
  expires_at?: string;
  expires_in?: number;
  filename?: string;
  object_key?: string;
  size_bytes?: number;
  rows?: number;
  sha256?: string;
  /** When the export itself is deleted (retention), not the URL. */
  retained_until?: string;
}

export type ExportDownloadTarget =
  | { kind: "job"; jobId: string; filename: string }
  | { kind: "browser"; href: string; filename: string };

export const EXPORT_EXPIRED_MESSAGE =
  "This export has expired and was deleted. Run the transfer again to export a fresh copy.";

export class ExportExpiredError extends Error {
  constructor() {
    super(EXPORT_EXPIRED_MESSAGE);
    this.name = "ExportExpiredError";
  }
}

type SummaryLike = { storage?: unknown; object_key?: unknown; download_url?: unknown; filename?: unknown };

/** Which download, if any, a finished transfer offers. */
export function exportDownloadTarget(result: Pick<TransferResult, "success" | "job_id" | "destination" | "destination_summary">): ExportDownloadTarget | null {
  if (!result.success) return null;
  // The browser-side export (API offline) already holds its file as a Blob URL.
  const browserHref = result.destination?.download_url;
  if (browserHref) {
    return {
      kind: "browser",
      href: browserHref,
      filename: result.destination?.filename || `export.${result.destination?.format || "json"}`,
    };
  }
  const ds = (result.destination_summary ?? {}) as SummaryLike;
  const jobId = String(result.job_id || "");
  const stored = ds.storage === "object_store" && Boolean(ds.object_key);
  const local = typeof ds.download_url === "string" && ds.download_url.startsWith("/api/v1/transfer/download/");
  if (jobId && (stored || local)) {
    return { kind: "job", jobId, filename: String(ds.filename || "export") };
  }
  return null;
}

/** "Kept until …" copy for an export held in the export bucket. */
export function exportRetentionNote(summary: { storage?: unknown; expires_at?: unknown } | undefined, now: Date = new Date()): string {
  if (!summary || summary.storage !== "object_store" || typeof summary.expires_at !== "string") return "";
  const until = new Date(summary.expires_at);
  if (Number.isNaN(until.getTime())) return "";
  if (until.getTime() <= now.getTime()) return EXPORT_EXPIRED_MESSAGE;
  return `Kept until ${until.toLocaleString()}, then deleted automatically.`;
}

/** The error to show for a failed `/export/download` response. */
export function exportDownloadError(status: number, apiMessage: string): Error {
  if (status === 410) return new ExportExpiredError();
  if (status === 404) return new Error(apiMessage || "This job has no file export.");
  return new Error(apiMessage || "Could not prepare the export download.");
}
