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
  ok: false, rows_scanned: 1000, issues: ["row 642 · d: expected date, got '2024-02-30'"], issue_count: 1,
  holdout_modes: { balanced: true, lenient: true },
};

describe("file values that do not fit (QA browser 3)", () => {
  it("strict asks for a decision, naming the row and value", () => {
    const hold = fileSourceIssueHold(REPORT, "strict");
    assert.ok(hold);
    assert.match(hold.title, /1 value/);
    assert.match(hold.issues[0], /642/);
    assert.equal(hold.holdoutMode, "balanced");
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
    assert.match(html, /2024-02-30/);
    assert.match(html, /switches the whole run to Balanced validation/);
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

  it("offers Balanced only when the API says Balanced holds every issue out", () => {
    const num = { ...REPORT, issues: ["row 3 · qty: expected integer, got 'abc'"], holdout_modes: { balanced: false, lenient: true } };
    const hold = fileSourceIssueHold(num, "strict")!;
    assert.equal(hold.canHoldOut, false);
    assert.match(hold.message, /only Lenient validation, from the API or MCP/);
    const html = renderToStaticMarkup(createElement(FileSourceIssuePanel, { hold, onHoldOut: () => {}, onFixFile: () => {} }));
    assert.doesNotMatch(html, /switches the whole run/);
    assert.match(html, /Fix the file/);
    // No API answer is no offer (a capped list, an older API).
    assert.equal(fileSourceIssueHold({ ...REPORT, holdout_modes: undefined }, "strict")?.canHoldOut, false);
    assert.doesNotMatch(readFileSync(path.join(here, "fileSourceIssueHold.ts"), "utf8"), /invalid \(\?:integer/);
  });

  it("the panel's remedy is worded as the API's explain action", () => {
    const api = readFileSync(path.join(here, "..", "..", "..", "api", "services", "validation_assistant.py"), "utf8");
    assert.match(api, /switches the whole "\s*\n\s*f"run to \{label_mode\} validation\) and load the rest/);
  });
});
