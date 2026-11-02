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

const DEFINED = /(--[a-zA-Z][a-zA-Z0-9-]*)\s*:/g;
const VAR_NO_FALLBACK = /var\((--[a-zA-Z][a-zA-Z0-9-]*)\s*\)/g;

// Custom properties written by JS at runtime (style.setProperty / inline
// style objects). Referenced without a CSS definition on purpose.
const JS_DRIVEN = new Set([
  "--i", "--p", "--step", "--cols", "--active", "--reveal-i",
]);

describe("every var() resolves", () => {
  it("var(--x) without a fallback points at a defined or JS-driven token", () => {
    const defined = new Set<string>();
    for (const [, css] of sheets()) {
      for (const match of css.matchAll(DEFINED)) defined.add(match[1]);
    }
    const offenders: string[] = [];
    for (const [name, css] of sheets()) {
      for (const match of css.matchAll(VAR_NO_FALLBACK)) {
        const token = match[1];
        if (!defined.has(token) && !JS_DRIVEN.has(token)) {
          offenders.push(`${name}: ${token}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      "var() with no fallback and no definition resolves to nothing — give it a fallback or define the token",
    );
  });
});
