import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

// Imported from the module itself, not the package barrel: the barrel pulls in
// React components that a plain node:test run cannot render.
import {
  BRAND_COLORS,
  BRAND_MARK_GEOMETRY,
  brandMarkSvg,
} from "../../../../packages/design-system/src/brand/mark";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const FAVICON = path.join(WEB, "public", "favicon.svg");

test("the shipped favicon is the canonical mark, byte for byte", () => {
  // A hand-edited favicon is exactly how the app ended up shipping two logos:
  // the icon set stayed on an older mark while the exports moved on.
  assert.equal(readFileSync(FAVICON, "utf8"), brandMarkSvg());
});

test("the mark keeps its proportions", () => {
  const g = BRAND_MARK_GEOMETRY;
  // Straps are centred and equal length, or the mark reads lopsided at 32px.
  assert.equal(g.horizontalStrap.width, g.verticalStrap.height);
  assert.equal(g.horizontalStrap.height, g.verticalStrap.width);
  assert.equal(g.horizontalStrap.x + g.horizontalStrap.width, g.tile.width - g.horizontalStrap.x);
  assert.equal(g.verticalStrap.y + g.verticalStrap.height, g.tile.height - g.verticalStrap.y);
  // The seal sits dead centre over the strap crossing.
  assert.equal(g.seal.x + g.seal.width / 2, g.tile.width / 2);
  assert.equal(g.seal.y + g.seal.height / 2, g.tile.height / 2);
});

test("the tile and the seal share one colour", () => {
  // The seal is a void punched out of the tile, not a filled square.
  assert.ok(brandMarkSvg().includes(`rx="${BRAND_MARK_GEOMETRY.seal.rx}" fill="${BRAND_COLORS.tile}"`));
});
