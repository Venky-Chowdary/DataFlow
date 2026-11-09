import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

/**
 * QA1 retest of #372.
 * S11: Studio's create-new ran CREATE TABLE with no approve control because
 * neither its multipart Execute nor its persisted plan declared the schema
 * policy, so the create-new DDL gate (declared policies only) never applied.
 * S7: the Clear key control sat inside `.df2-map-footer-status`, which the
 * 1024-1366px layout hides.
 */
const here = dirname(fileURLToPath(import.meta.url));
const read = (...p: string[]) => readFileSync(join(here, ...p), "utf8");

describe("S11: Studio declares its schema policy", () => {
  it("both multipart Execute builders send schema_policy_declared", () => {
    const api = read("api.ts");
    const declared = api.match(/formData\.append\("schema_policy_declared", "true"\)/g) || [];
    assert.equal(declared.length, 2);
  });

  it("the persisted plan's policies declare it", () => {
    const page = read("..", "pages", "TransferPage.tsx");
    assert.match(page, /schema_policy: schemaPolicy,\s*(?:\/\/[^\n]*\n\s*)?schema_policy_declared: true/);
  });
});

describe("S7: Clear key is reachable at every width", () => {
  it("is not inside the footer status strip compact widths hide", () => {
    const step = read("..", "pages", "transfer", "TransferMapStep.tsx");
    const start = step.indexOf('<div className="df2-map-footer-status"');
    const end = step.indexOf('<div className="df2-map-footer-actions">');
    assert.ok(start > 0 && end > start);
    const clear = step.indexOf('data-testid="map-clear-primary-key"');
    assert.ok(clear > end, "Clear key must render in .df2-map-footer-actions");
  });

  it("the compact rule still hides the status strip (so the move is what fixes it)", () => {
    const css = read("..", "styles", "enterprise-ui.css");
    assert.match(css, /\.df2-map-footer-status\s*\{\s*display:\s*none/);
    assert.doesNotMatch(css, /\.df2-map-footer-actions\s*\{[^}]*display:\s*none/);
  });
});
