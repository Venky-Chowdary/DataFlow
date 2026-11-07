import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));

function sheet(name: string): string {
  return readFileSync(join(stylesDir, name), "utf8");
}

function sheets(): Array<[string, string]> {
  return readdirSync(stylesDir)
    .filter((name) => name.endsWith(".css"))
    .map((name) => [name, sheet(name)]);
}

/** Raw z-index declarations like `z-index: 12` or `z-index: -1 !important`. */
const RAW_Z = /z-index:\s*(-?\d+)/g;
/** Custom-property definitions — the scale itself is allowed to hold numbers. */
const TOKEN_DEF = /--df-z-[a-z-]+:\s*-?\d+\s*;/g;

describe("z-index stays on the --df-z-* scale", () => {
  it("no stylesheet declares a raw numeric z-index", () => {
    const offenders: string[] = [];
    for (const [name, css] of sheets()) {
      // Strip the scale's own definitions before looking for raw uses.
      const body = css.replace(TOKEN_DEF, "");
      for (const match of body.matchAll(RAW_Z)) {
        offenders.push(`${name}: z-index ${match[1]}`);
      }
    }
    assert.deepEqual(offenders, [], "raw z-index values must use var(--df-z-*)");
  });

  it("the scale defines every tier the sheets reference", () => {
    const tokens = sheet("tokens.css");
    const defined = new Set([...tokens.matchAll(/(--df-z-[a-z-]+):\s*-?\d+/g)].map((m) => m[1]));
    assert.ok(defined.size >= 10, "expected the --df-z-* scale in tokens.css");
    const used = new Set<string>();
    for (const [, css] of sheets()) {
      for (const match of css.matchAll(/var\((--df-z-[a-z-]+)/g)) used.add(match[1]);
    }
    for (const name of used) {
      assert.ok(defined.has(name), `z-index token ${name} is used but not defined in tokens.css`);
    }
  });
});
