import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));

function sheets(): Array<[string, string]> {
  return readdirSync(stylesDir)
    .filter((name) => name.endsWith(".css"))
    .map((name) => [name, readFileSync(join(stylesDir, name), "utf8")]);
}

/** 11px floor: nothing in the product may render below it (UI-9). */
const FLOOR_PX = 11;
const FONT_PX = /font-size:\s*(\d+(?:\.\d+)?)px/g;
const FONT_REM = /font-size:\s*(\d+(?:\.\d+)?)rem/g;
const REM_PX = 16; // UA root — html never re-declares font-size

describe("type floor: no font-size below 11px", () => {
  it("no stylesheet declares font-size under the floor", () => {
    const offenders: string[] = [];
    for (const [name, css] of sheets()) {
      for (const match of css.matchAll(FONT_PX)) {
        if (parseFloat(match[1]) < FLOOR_PX) offenders.push(`${name}: ${match[1]}px`);
      }
      for (const match of css.matchAll(FONT_REM)) {
        if (parseFloat(match[1]) * REM_PX < FLOOR_PX) offenders.push(`${name}: ${match[1]}rem`);
      }
    }
    assert.deepEqual(offenders, [], "font-size below the 11px floor must use var(--df-fs-xs)");
  });

  it("html keeps the UA rem root (font-size lives on body only)", () => {
    const tokens = readFileSync(join(stylesDir, "tokens.css"), "utf8");
    const htmlBlock = /html,\s*body,\s*#root\s*\{[^}]*\}/.exec(tokens);
    assert.ok(htmlBlock, "expected the html,body,#root base rule in tokens.css");
    assert.ok(!/font-size/.test(htmlBlock[0]), "font-size on html rescales every rem — declare it on body");
    assert.match(tokens, /body\s*\{[^}]*font-size:\s*var\(--df-fs-body\)/, "body must take --df-fs-body");
  });
});
