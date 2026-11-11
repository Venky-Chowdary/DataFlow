/**
 * Jobs grid model: verification, attention, sort, CSV and layout persistence.
 * Run: npx tsx --test apps/web/src/lib/jobGrid.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  csvCell,
  DEFAULT_JOBS_GRID_PREFS,
  DEFAULT_JOBS_GRID_SORT,
  jobDetailSubtitle,
  jobNeedsAttention,
  jobStatusShortLabel,
  jobVerificationDisplay,
  jobsCsv,
  JOBS_CSV_HEADER,
  jobVerification,
  nextJobsGridSort,
  readJobsGridPrefs,
  sortJobsForGrid,
  verificationBadgeClass,
  writeJobsGridPrefs,
} from "./jobGrid.js";
import type { TransferJob } from "./types.js";

function job(id: string, patch: Partial<TransferJob> = {}): TransferJob {
  return {
    _id: id,
    name: id,
    status: "completed",
    source_type: "postgresql",
    source_name: "src",
    destination_type: "snowflake",
    destination_database: "wh",
    destination_collection: id,
    records_processed: 10,
    created_at: "2026-11-10T10:00:00",
    ...patch,
  } as TransferJob;
}

const independent = {
  passed: true,
  coverage: "full_checksum",
  checksum_scope: "whole_table",
  source_checksum: "abc",
  target_checksum: "abc",
  source_checksum_provenance: "independent_source_reread",
  source_rows: 10,
  target_rows: 10,
};

describe("jobVerification", () => {
  it("uses the detail card's Gate-8 rules on the list summary", () => {
    const proven = jobVerification(job("a", { verification_summary: independent }));
    assert.equal(proven.label, "Independent re-read");
    assert.equal(proven.tone, "ok");
    assert.equal(verificationBadgeClass(proven.tone), "df2-badge df2-badge-live");

    const failed = jobVerification(job("b", { verification_summary: { passed: false, message: "4 rows differ" } }));
    assert.equal(failed.label, "Failed");
    assert.equal(failed.tone, "danger");
    assert.match(failed.title, /4 rows differ/);

    const sample = jobVerification(job("c", {
      verification_summary: { passed: true, coverage: "sample", sample_compare: { passed: true, compared: 50 } },
    }));
    assert.equal(sample.label, "Sample verified");
    assert.equal(sample.tone, "warn", "a sample is never a green population proof");
  });

  it("relabels a last-table digest on a multi-table run like the detail does", () => {
    const multi = jobVerification(job("m", {
      verification_summary: independent,
      multi_stream: true,
      stream_names: ["orders", "customers"],
    }));
    assert.equal(multi.label, "Last stream checksum");
    assert.equal(multi.tone, "warn");
  });

  it("never invents a verdict", () => {
    assert.equal(jobVerification(job("r", { status: "running" })).label, "Pending");
    assert.equal(jobVerification(job("q", { status: "queued", verification_summary: independent })).label, "Pending");
    assert.equal(jobVerification(job("f", { status: "failed" })).label, "Not run");
    assert.equal(jobVerification(job("x", { status: "cancelled" })).label, "Not run");
    const none = jobVerification(job("n"));
    assert.equal(none.label, "Not captured");
    assert.equal(none.tone, "muted");
    assert.equal(jobVerification(job("n2", { verification_summary: {} })).label, "Not captured");
  });

  it("reports quarantined rows next to the verdict, not instead of it", () => {
    const view = jobVerification(job("q", { rejected_rows: 37, verification_summary: independent }));
    assert.equal(view.quarantined, 37);
    assert.equal(view.label, "Independent re-read");
  });
});

describe("jobVerificationDisplay (plain labels over the unchanged classifier)", () => {
  const show = (patch: Partial<TransferJob>) => jobVerificationDisplay(jobVerification(job("v", patch)));

  it("says Verified / Verified (sample) / Failed / Pending / Not run", () => {
    const verified = show({ verification_summary: independent });
    assert.equal(verified.label, "Verified");
    assert.equal(verified.tone, "ok");
    assert.match(verified.title, /Independent re-read/, "the technical basis stays in the tooltip");
    const sample = show({ verification_summary: { passed: true, coverage: "sample", sample_compare: { passed: true, compared: 5 } } });
    assert.equal(sample.label, "Verified (sample)");
    assert.equal(sample.tone, "warn");
    assert.equal(show({ verification_summary: { passed: false } }).label, "Failed");
    assert.equal(show({ status: "running" }).label, "Pending");
    assert.equal(show({ status: "failed" }).label, "Not run");
    assert.equal(show({}).label, "Not run", "no recorded verdict is not a verification");
    assert.match(show({}).title, /no Gate-8 verdict/i);
  });

  it("never upgrades a partial or unproven verdict to Verified", () => {
    const writerAck = show({ verification_summary: { passed: true, phase: "writer_ack", message: "Verified by writer" } });
    assert.equal(writerAck.label, "Unverified");
    assert.match(writerAck.title, /Writer ack/);
    const batch = show({ verification_summary: { passed: true, checksum_scope: "written_batch_keys" } });
    assert.equal(batch.label, "Verified (partial)");
    assert.equal(batch.tone, "warn");
  });

  it("shows held-out rows as Quarantined N unless the verification failed", () => {
    const held = show({ verification_summary: independent, rejected_rows: 37 });
    assert.equal(held.label, "Quarantined 37");
    assert.equal(held.tone, "warn");
    assert.match(held.title, /Independent re-read.*37 rows held out/s);
    assert.equal(show({ verification_summary: { passed: false }, rejected_rows: 3 }).label, "Failed");
  });
});

describe("jobStatusShortLabel", () => {
  it("keeps grid status to one short word, the full label stays available", () => {
    assert.equal(jobStatusShortLabel("completed_with_quarantine"), "Quarantined");
    assert.equal(jobStatusShortLabel("completed_with_warnings"), "Warnings");
    assert.equal(jobStatusShortLabel("completed"), "Completed");
    assert.equal(jobStatusShortLabel("failed"), "Failed");
    assert.equal(jobStatusShortLabel("needs_approval"), "Needs approval");
  });
});

describe("jobNeedsAttention", () => {
  it("is a failed run or a failed Gate-8 verification", () => {
    assert.equal(jobNeedsAttention(job("a", { status: "failed" })), true);
    assert.equal(jobNeedsAttention(job("b", { verification_summary: { passed: false } })), true);
    assert.equal(jobNeedsAttention(job("c", { verification_summary: independent })), false);
    assert.equal(jobNeedsAttention(job("d")), false);
    assert.equal(jobNeedsAttention(job("e", { status: "running" })), false);
    assert.equal(jobNeedsAttention(job("f", { status: "completed", cancel_requested: true })), false);
  });
});

describe("sortJobsForGrid", () => {
  const name = (j: TransferJob) => j.name || j._id;
  const rows = [
    job("charlie", { records_processed: 5, started_at: "2026-11-10T10:00:00", completed_at: "2026-11-10T10:05:00" }),
    job("alpha", { records_processed: 50, started_at: "2026-11-10T09:00:00", completed_at: "2026-11-10T09:01:00" }),
    job("bravo", { records_processed: undefined as unknown as number, status: "failed" }),
  ];

  it("keeps the server order (newest first) for the default sort", () => {
    assert.equal(sortJobsForGrid(rows, DEFAULT_JOBS_GRID_SORT, { name }), rows);
  });

  it("sorts by name, rows and duration with missing values last", () => {
    const ids = (list: TransferJob[]) => list.map((j) => j._id);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "job", dir: "asc" }, { name })), ["alpha", "bravo", "charlie"]);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "job", dir: "desc" }, { name })), ["charlie", "bravo", "alpha"]);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "rows", dir: "desc" }, { name })), ["alpha", "charlie", "bravo"]);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "rows", dir: "asc" }, { name })), ["charlie", "alpha", "bravo"]);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "duration", dir: "asc" }, { name })), ["alpha", "charlie", "bravo"]);
    assert.deepEqual(ids(sortJobsForGrid(rows, { column: "status", dir: "asc" }, { name })), ["bravo", "charlie", "alpha"]);
  });

  it("is stable for equal keys", () => {
    const same = [job("one"), job("two"), job("three")];
    assert.deepEqual(
      sortJobsForGrid(same, { column: "mode", dir: "asc" }, { name }).map((j) => j._id),
      ["one", "two", "three"],
    );
  });

  it("flips the same column and starts numbers newest/largest first", () => {
    assert.deepEqual(nextJobsGridSort(DEFAULT_JOBS_GRID_SORT, "started"), { column: "started", dir: "asc" });
    assert.deepEqual(nextJobsGridSort(DEFAULT_JOBS_GRID_SORT, "job"), { column: "job", dir: "asc" });
    assert.deepEqual(nextJobsGridSort({ column: "job", dir: "asc" }, "rows"), { column: "rows", dir: "desc" });
  });
});

describe("jobsCsv", () => {
  it("writes the header, one row per job and RFC 4180 quoting", () => {
    const csv = jobsCsv(
      [
        job("6731abcd", {
          name: 'orders, "east"',
          status: "failed",
          sync_mode: "incremental_append",
          error: "line one\nline two",
          started_at: "2026-11-10T10:00:00",
          completed_at: "2026-11-10T10:00:38",
        }),
        job("6731ef01", { verification_summary: independent, rejected_rows: 3 }),
      ],
      { name: (j) => j.name || j._id },
    );
    const lines = csv.split("\r\n");
    assert.equal(lines[0], JOBS_CSV_HEADER.join(","));
    assert.ok(lines[1].startsWith('6731abcd,"orders, ""east""",Failed,postgresql,src,snowflake,'));
    assert.match(csv, /Incremental append/);
    assert.match(csv, /,38s,2026-11-10T10:00:00\.000Z,2026-11-10T10:00:38\.000Z,"line one\nline two"/);
    assert.match(lines.at(-2) || "", /,Quarantined 3,Independent re-read,3,/);
    assert.equal(lines.at(-1), "", "the file ends with a line break");
  });

  it("defuses cells a spreadsheet would run as a formula", () => {
    assert.equal(csvCell("=HYPERLINK(1)"), "'=HYPERLINK(1)");
    assert.equal(csvCell("+1"), "'+1");
    assert.equal(csvCell("-2"), "'-2");
    assert.equal(csvCell("@x"), "'@x");
    assert.equal(csvCell(null), "");
    assert.equal(csvCell(12), "12");
  });
});

describe("grid layout preferences", () => {
  function storage(seed: Record<string, string> = {}, fail = false) {
    const data = new Map(Object.entries(seed));
    return {
      getItem: (k: string) => { if (fail) throw new Error("blocked"); return data.get(k) ?? null; },
      setItem: (k: string, v: string) => { if (fail) throw new Error("quota"); data.set(k, v); },
      data,
    };
  }

  function withWindow<T>(store: ReturnType<typeof storage>, run: () => T): T {
    const previous = (globalThis as { window?: unknown }).window;
    (globalThis as { window?: unknown }).window = { localStorage: store };
    try {
      return run();
    } finally {
      (globalThis as { window?: unknown }).window = previous;
    }
  }

  it("round-trips density and hidden columns per key", () => {
    const store = storage();
    withWindow(store, () => {
      writeJobsGridPrefs({ density: "comfortable", hidden: ["mode", "duration"] }, "k:a@example.com");
      assert.deepEqual(readJobsGridPrefs("k:a@example.com"), { density: "comfortable", hidden: ["mode", "duration"] });
      assert.deepEqual(readJobsGridPrefs("k:b@example.com"), DEFAULT_JOBS_GRID_PREFS, "another operator keeps the default");
    });
  });

  it("ignores unknown or required columns and bad JSON", () => {
    const store = storage({
      tampered: JSON.stringify({ density: "huge", hidden: ["status", "job", "bogus", "rows", "rows"] }),
      broken: "{not json",
    });
    withWindow(store, () => {
      assert.deepEqual(readJobsGridPrefs("tampered"), { density: "compact", hidden: ["rows"] });
      assert.deepEqual(readJobsGridPrefs("broken"), DEFAULT_JOBS_GRID_PREFS);
    });
  });

  it("survives blocked storage", () => {
    const store = storage({}, true);
    withWindow(store, () => {
      assert.deepEqual(readJobsGridPrefs("k"), DEFAULT_JOBS_GRID_PREFS);
      assert.doesNotThrow(() => writeJobsGridPrefs({ density: "compact", hidden: [] }, "k"));
    });
  });
});

describe("jobDetailSubtitle", () => {
  it("names the engines and the mode", () => {
    assert.equal(
      jobDetailSubtitle({ source_type: "sqlserver", destination_type: "snowflake", sync_mode: "incremental_deduped" }),
      "SQL Server → Snowflake · Incremental deduped",
    );
    assert.equal(jobDetailSubtitle({ source_type: "mysql", destination_type: "postgresql" }), "MySQL → PostgreSQL");
  });
});
