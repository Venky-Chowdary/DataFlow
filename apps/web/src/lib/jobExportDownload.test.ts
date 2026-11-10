/**
 * Run: npx --yes tsx --test apps/web/src/lib/jobExportDownload.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  EXPORT_EXPIRED_MESSAGE,
  ExportExpiredError,
  exportDownloadError,
  exportDownloadTarget,
  exportRetentionNote,
} from "./jobExportDownload.js";

describe("file export download action", () => {
  it("downloads a bucket export through the job's signed-download route", () => {
    const target = exportDownloadTarget({
      success: true,
      job_id: "job-1",
      destination: { database: "", collection: "" },
      destination_summary: {
        storage: "object_store",
        object_key: "exports/ws/job-1/export_job-1.csv",
        filename: "export_job-1.csv",
        download_url: "/api/v1/jobs/job-1/export/download",
      } as never,
    });
    assert.deepEqual(target, { kind: "job", jobId: "job-1", filename: "export_job-1.csv" });
  });

  it("uses the same route for a self-hosted export in the exports folder", () => {
    const target = exportDownloadTarget({
      success: true,
      job_id: "job-2",
      destination_summary: { filename: "orders.csv", download_url: "/api/v1/transfer/download/orders.csv" },
    });
    assert.equal(target?.kind, "job");
  });

  it("keeps the in-browser export's Blob URL and offers nothing for other results", () => {
    const browser = exportDownloadTarget({
      success: true,
      destination: { database: "local", collection: "a.csv", filename: "a.csv", download_url: "blob:x" },
    });
    assert.deepEqual(browser, { kind: "browser", href: "blob:x", filename: "a.csv" });
    assert.equal(exportDownloadTarget({ success: true, job_id: "j", destination_summary: { table: "t" } }), null);
    assert.equal(
      exportDownloadTarget({ success: false, job_id: "j", destination_summary: { storage: "object_store", object_key: "k" } as never }),
      null,
    );
  });

  it("says when a bucket export is kept until, and that 410 means expired", () => {
    const now = new Date("2026-11-10T12:00:00Z");
    assert.match(
      exportRetentionNote({ storage: "object_store", expires_at: "2026-11-12T12:00:00Z" }, now),
      /^Kept until .+, then deleted automatically\.$/,
    );
    assert.equal(exportRetentionNote({ storage: "object_store", expires_at: "2026-11-09T12:00:00Z" }, now), EXPORT_EXPIRED_MESSAGE);
    assert.equal(exportRetentionNote({ expires_at: "2026-11-12T12:00:00Z" }, now), "");
    const expired = exportDownloadError(410, "export expired");
    assert.ok(expired instanceof ExportExpiredError);
    assert.equal(expired.message, EXPORT_EXPIRED_MESSAGE);
    assert.equal(exportDownloadError(404, "Job not found").message, "Job not found");
  });
});
