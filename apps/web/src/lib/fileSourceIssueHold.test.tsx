/**
 * QA browser 3: a value in the file that does not fit its column sent Studio
 * back to Source. The decision now stays on Validate.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { fileSourceIssueHold } from "./fileSourceIssueHold.js";
import { FileSourceIssuePanel } from "../components/transfer/FileSourceIssuePanel.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const page = readFileSync(path.join(here, "..", "pages", "TransferPage.tsx"), "utf8");
const REPORT = {
  ok: false, rows_scanned: 1000, issues: ["Row 642, column d: '31/02/2025' is not a real calendar date"], issue_count: 1,
};

describe("file values that do not fit (QA browser 3)", () => {
  it("strict asks for a decision, naming the row and value", () => {
    const hold = fileSourceIssueHold(REPORT, "strict");
    assert.ok(hold);
    assert.match(hold.title, /1 value/);
    assert.match(hold.issues[0], /642/);
    assert.match(hold.message, /Balanced/);
  });

  it("balanced and lenient hold the rows out at write — no decision needed", () => {
    assert.equal(fileSourceIssueHold(REPORT, "balanced"), null);
    assert.equal(fileSourceIssueHold(REPORT, "lenient"), null);
    assert.equal(fileSourceIssueHold({ ...REPORT, ok: true }, "strict"), null);
    assert.equal(fileSourceIssueHold(null, "strict"), null);
  });

  it("renders both choices with the issue", () => {
    const html = renderToStaticMarkup(createElement(FileSourceIssuePanel, {
      hold: fileSourceIssueHold(REPORT, "strict")!, onHoldOut: () => {}, onFixFile: () => {},
    }));
    assert.match(html, /role="alert"/);
    assert.match(html, /31\/02\/2025/);
    assert.match(html, /Hold these rows out \(Balanced\)/);
    assert.match(html, /Fix the file/);
  });

  it("Validate and Execute stay on Validate instead of jumping to Source", () => {
    assert.doesNotMatch(page, /CSV type issue\(s\) found — fix source data/);
    assert.doesNotMatch(page, /CSV type issue\(s\) — fix source file before writing/);
    assert.equal((page.match(/fileSourceIssueHold\(parsed\?\.validation/g) || []).length, 2);
    assert.match(page, /<FileSourceIssuePanel/);
    assert.match(page, /executePreflight\(undefined, "balanced"\)/);
  });
});
