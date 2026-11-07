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

const MEDIA = /@media[^{]*/g;
const BOUND = /(min|max)-width:\s*(\d+(?:\.\d+)?)px/g;

describe("media query breakpoints", () => {
  it("no viewport width matches both a min- and a max-width query", () => {
    // At the same pixel, `@media (min-width: Npx)` and `@media (max-width: Npx)`
    // both match — two rulesets fight and later-source-order wins silently.
    // Convention here: the min-width query owns the boundary; the max side
    // sits one px below (767/768, 1023/1024).
    const mins = new Set<number>();
    const maxs = new Set<number>();
    for (const [, css] of sheets()) {
      for (const query of css.matchAll(MEDIA)) {
        for (const b of query[0].matchAll(BOUND)) {
          (b[1] === "min" ? mins : maxs).add(parseFloat(b[2]));
        }
      }
    }
    const collisions = [...mins].filter((n) => maxs.has(n)).sort((a, b) => a - b);
    assert.deepEqual(collisions, []);
  });

  it("no width:100vw declaration (full-bleed must not count the scrollbar)", () => {
    const offenders: string[] = [];
    for (const [name, css] of sheets()) {
      // `width:` only — `max-width: 100vw` is a harmless cap (the element can
      // never exceed the viewport), not a full-bleed size declaration.
      for (const match of css.matchAll(/(?<![-\w])width:\s*100vw\b/g)) {
        offenders.push(`${name}: ${match[0]}`);
      }
    }
    assert.deepEqual(offenders, [], "width:100vw includes the scrollbar — use the breakout margins");
  });
});
