/** UX-41: the Tesseract install message was shown to end users in Studio. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

describe("Studio OCR toggle copy", () => {
  it("speaks to the user, not the host's package list", () => {
    const studio = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "pages", "TransferPage.tsx"), "utf8");
    const toggle = studio.slice(studio.indexOf('className="df2-policy-toggle df2-source-ocr-toggle"'), studio.indexOf('id="df2-source-ocr-status"') + 600);
    assert.doesNotMatch(toggle, /Tesseract|pytesseract|pypdfium2|Pillow|pip install/);
    assert.match(toggle, /ask your administrator to enable OCR/);
  });
});
