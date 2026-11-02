import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const page = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "TransferPage.tsx"), "utf8");
const toggle = page.slice(page.indexOf('className="df2-policy-toggle df2-source-ocr-toggle"'));
const block = toggle.slice(0, toggle.indexOf("</label>"));

describe("source OCR toggle when OCR is not installed", () => {
  it("is a disabled, described control rather than a live empty box", () => {
    assert.ok(block.length > 0, "OCR toggle not found");
    assert.match(block, /disabled=\{ocrStatus\?\.available === false\}/);
    assert.match(block, /checked=\{enableOcr && ocrStatus\?\.available !== false\}/);
    assert.match(block, /aria-describedby="df2-source-ocr-status"/);
    assert.match(block, /<small id="df2-source-ocr-status">/);
  });

  it("does not tell a touch user to hover", () => {
    assert.doesNotMatch(block, /hover for install/i);
  });
});
