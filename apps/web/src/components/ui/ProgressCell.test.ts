/**
 * UX-16 (found by the axe gate on a workspace with data): Overview's job
 * progress bars had no accessible name, and contract rows were buttons that
 * held a button.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ProgressCell } from "./ProgressCell.js";

describe("progress bar name", () => {
  it("names the bar", () => {
    const html = renderToStaticMarkup(createElement(ProgressCell, { value: 40 }));
    assert.match(html, /role="progressbar" aria-label="Job progress" aria-valuenow="40"/);
  });
});

describe("contract rows", () => {
  it("are list items, not buttons around buttons", () => {
    const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "pages", "ContractsPage.tsx"), "utf8");
    const row = page.slice(page.indexOf('className={`df2-contract-row df2-card-interactive'), page.indexOf("df2-contract-row-identity"));
    assert.match(row, /role="listitem"/);
    assert.doesNotMatch(row, /role="button"|tabIndex=\{0\}/);
  });
});
