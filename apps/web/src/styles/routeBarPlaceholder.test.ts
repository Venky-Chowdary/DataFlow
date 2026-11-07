import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const css = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "transfer-studio.css"), "utf8");

describe("route bar destination placeholder", () => {
  it("is an invisible spacer, not an empty bordered square", () => {
    const rule = css.match(/\.df2-route-bar-placeholder-icon\s*\{([^}]*)\}/);
    assert.ok(rule, "placeholder rule missing");
    const body = rule[1];
    assert.match(body, /border:\s*0;/);
    assert.doesNotMatch(body, /border:\s*1px/);
    assert.match(body, /width:\s*18px;/);
  });
});
