/**
 * QA browser 4: a run wrote 998 rows and held 2 out; "Open quarantine" did
 * nothing and the job page said there were no quarantined rows.
 *
 * Two causes, both pinned here:
 * - the job root is created with ``rejected_rows: 0``; a route that records its
 *   hold-outs on the summary / row-conservation ledger / Gate-8 was read as 0,
 *   so the quarantine section never rendered and the button had no target;
 * - the Gate-8 card scrolled to the first ``.df2-theater-v3-quarantine``, which
 *   is the "Compared to prior loads" panel whenever a load history exists.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { readRejectedRows } from "./jobEvidence.js";
import { THEATER_QUARANTINE_ID, scrollToTheaterQuarantine } from "./quarantineTarget.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const theater = readFileSync(path.join(here, "..", "components", "JobTheater.tsx"), "utf8");
const result = readFileSync(path.join(here, "..", "components", "transfer", "TransferResultDashboard.tsx"), "utf8");

describe("Open quarantine (QA browser 4)", () => {
  it("counts the rows the ledger held out when the job root says 0", () => {
    const job = {
      status: "completed_with_quarantine",
      rejected_rows: 0,
      destination_summary: { rejected_rows: 0 },
      row_accounting: { rows_read: 1000, rows_written: 998, rows_quarantined: 2 },
    };
    assert.equal(readRejectedRows(job), 2);
    assert.equal(readRejectedRows({ rejected_rows: 0, reconciliation: { rejected_rows: 2 } }), 2);
    assert.equal(readRejectedRows({ rejected_rows: 0, destination_summary: { rejected_rows: 2 } }), 2);
  });

  it("keeps a positive stamp and reports a clean run as 0", () => {
    assert.equal(readRejectedRows({ rejected_rows: 2, row_accounting: { rows_quarantined: 9 } }), 2);
    assert.equal(readRejectedRows({ rejected_rows: 0, row_accounting: { rows_quarantined: 0 } }), 0);
    assert.equal(readRejectedRows({}), 0);
  });

  it("scrolls to the quarantine section by its id", () => {
    const calls: string[] = [];
    const el = { scrollIntoView: () => calls.push("scrolled") };
    const doc = { getElementById: (id: string) => (id === THEATER_QUARANTINE_ID ? el : null) };
    assert.equal(scrollToTheaterQuarantine(doc as unknown as Document), true);
    assert.deepEqual(calls, ["scrolled"]);
    assert.equal(scrollToTheaterQuarantine({ getElementById: () => null } as unknown as Document), false);
  });

  it("wires every Theater button and the section to one condition and one target", () => {
    assert.match(theater, /<section id=\{THEATER_QUARANTINE_ID\}/);
    assert.match(theater, /\{showQuarantineSection && \(/);
    assert.equal((theater.match(/onOpenQuarantine=\{openQuarantine\}/g) || []).length, 2);
    assert.doesNotMatch(theater, /querySelector\("\.df2-theater-v3-quarantine"\)/);
  });

  it("the Studio result reads the same count as the job page", () => {
    assert.match(result, /readRejectedRows\(\{/);
    assert.doesNotMatch(result, /ds\?\.rejected_rows\s*\n\s*\?\?/);
  });
});
