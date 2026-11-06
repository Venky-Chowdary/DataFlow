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
import { fileSourceIssueHold, isCalendarHoldoutFinding } from "./fileSourceIssueHold.js";
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
    assert.match(html, /Hold out these rows in quarantine \(Balanced validation\)/);
    assert.match(html, /Fix the file/);
  });

  it("Validate and Execute stay on Validate instead of jumping to Source", () => {
    assert.doesNotMatch(page, /CSV type issue\(s\) found — fix source data/);
    assert.doesNotMatch(page, /CSV type issue\(s\) — fix source file before writing/);
    assert.equal((page.match(/fileSourceIssueHold\(parsed\?\.validation/g) || []).length, 2);
    assert.match(page, /<FileSourceIssuePanel/);
    assert.match(page, /executePreflight\(undefined, "balanced"\)/);
  });

  it("a non-existent date is a holdout finding, not a remap", () => {
    assert.equal(isCalendarHoldoutFinding("row 642 order_date: Invalid date: '31/02/2025' is not a real calendar date"), true);
    assert.equal(isCalendarHoldoutFinding("status (VARCHAR) → flag (BOOLEAN) confidence 40% < 70%"), false);
    const at = page.indexOf("isCalendarHoldoutFinding(integrityText)");
    assert.ok(at > 0 && at < page.indexOf("if (looksLikeBadMapping && !encodingOnly)"), "checked before the remap branch");
  });

  it("offers Balanced only when Balanced holds every issue out", () => {
    const num = { ...REPORT, issues: ["row 3 · qty: Invalid integer: 'abc'"], issue_count: 1 };
    assert.equal(fileSourceIssueHold(num, "strict")?.canHoldOut, true);
    const word = { ...REPORT, issues: ["row 3 · flag: Invalid boolean: 'maybe'"], issue_count: 1 };
    const hold = fileSourceIssueHold(word, "strict")!;
    assert.equal(hold.canHoldOut, false);
    assert.doesNotMatch(hold.message, /Balanced validation holds/);
    const html = renderToStaticMarkup(createElement(FileSourceIssuePanel, { hold, onHoldOut: () => {}, onFixFile: () => {} }));
    assert.doesNotMatch(html, /Balanced validation\)/);
    assert.match(html, /Fix the file/);
    // Issues we cannot see (count above the listed ones) are not assumed holdable.
    assert.equal(fileSourceIssueHold({ ...REPORT, issue_count: 40 }, "strict")?.canHoldOut, false);
  });

  it("the panel's remedy is worded as the API's explain action", () => {
    const api = readFileSync(path.join(here, "..", "..", "..", "api", "services", "validation_assistant.py"), "utf8");
    assert.match(api, /in quarantine \(Balanced "\s*\n\s*f?"validation\) and load the rest/);
  });
});
