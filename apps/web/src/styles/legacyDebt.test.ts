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

/**
 * Debt ratchets (UI-5 / UI-10): the backlog is too large to migrate in one
 * change, but it may only ever shrink. Each ceiling is the count measured
 * after the token migration — any new raw value fails the test.
 */
const CEILINGS = {
  rawHexUses: 3471,       // #[0-9a-f]{3,8} outside token definitions
  distinctHex: 277,
  important: 3213,        // !important declarations
  rawRadius: 1490,        // border-radius: <number>
};

const HEX = /#[0-9a-fA-F]{3,8}\b/g;
const IMPORTANT = /!important\b/g;
const RAW_RADIUS = /border-radius:\s*\d/g;

describe("token-debt ratchets only ever shrink", () => {
  const all = sheets().map(([, css]) => css).join("\n");

  it("raw hex colour uses stay at or below the ceiling", () => {
    const uses = all.match(HEX) ?? [];
    assert.ok(
      uses.length <= CEILINGS.rawHexUses,
      `raw hex uses grew to ${uses.length} (ceiling ${CEILINGS.rawHexUses}) — use a --df-* token`,
    );
    const distinct = new Set(uses.map((u) => u.toLowerCase()));
    assert.ok(
      distinct.size <= CEILINGS.distinctHex,
      `distinct hex values grew to ${distinct.size} (ceiling ${CEILINGS.distinctHex})`,
    );
  });

  it("!important count stays at or below the ceiling", () => {
    const n = (all.match(IMPORTANT) ?? []).length;
    assert.ok(n <= CEILINGS.important, `!important grew to ${n} (ceiling ${CEILINGS.important})`);
  });

  it("raw numeric border-radius stays at or below the ceiling", () => {
    const n = (all.match(RAW_RADIUS) ?? []).length;
    assert.ok(n <= CEILINGS.rawRadius, `raw border-radius grew to ${n} (ceiling ${CEILINGS.rawRadius}) — use var(--df-radius-*)`);
  });
});
