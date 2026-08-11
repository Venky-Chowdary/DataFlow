/**
 * The Datawrap mark, as geometry — the one definition every surface renders.
 *
 * The React component, `public/favicon.svg` and every exported raster used to
 * carry their own copy of these shapes, so the app shipped an older mark in the
 * favicon and manifest while the social exports had moved on. Anything that
 * draws the mark reads it from here, and `brandMark.test.ts` fails if the
 * checked-in SVG drifts from it.
 */

export const BRAND_MARK_VIEWBOX = 64;

export const BRAND_COLORS = {
  /** App tile — deep teal. */
  tile: "#0A3D3A",
  /** Horizontal wrap strap. */
  strap: "#F59E0B",
  /** Vertical wrap strap on the tile. */
  strapOnTile: "#F8FAFC",
  /** Vertical wrap strap when the mark sits on a light surface. */
  strapPlain: "#0F766E",
  /** Directional wrap strap → arrow. */
  arrow: "#2DD4BF",
  /** Centre seal void when the mark sits on a light surface. */
  sealPlain: "#FFFFFF",
} as const;

export const BRAND_MARK_GEOMETRY = {
  tile: { width: 64, height: 64, rx: 14 },
  horizontalStrap: { x: 10, y: 28, width: 44, height: 8, rx: 2.5 },
  verticalStrap: { x: 28, y: 10, width: 8, height: 44, rx: 2.5 },
  /** Diagonal strap drawn as a round-capped stroke. */
  diagonal: { d: "M16 48 L40 24", strokeWidth: 8 },
  arrowHead: { d: "M36 18 L52 14 L44 30 Z" },
  seal: { x: 27, y: 27, width: 10, height: 10, rx: 2 },
} as const;

/** Serialize the mark to standalone SVG — used by the icon export and tests. */
export function brandMarkSvg(): string {
  const g = BRAND_MARK_GEOMETRY;
  const c = BRAND_COLORS;
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${BRAND_MARK_VIEWBOX} ${BRAND_MARK_VIEWBOX}" fill="none" role="img" aria-label="Datawrap">`,
    `  <rect width="${g.tile.width}" height="${g.tile.height}" rx="${g.tile.rx}" fill="${c.tile}"/>`,
    `  <rect x="${g.horizontalStrap.x}" y="${g.horizontalStrap.y}" width="${g.horizontalStrap.width}" height="${g.horizontalStrap.height}" rx="${g.horizontalStrap.rx}" fill="${c.strap}"/>`,
    `  <rect x="${g.verticalStrap.x}" y="${g.verticalStrap.y}" width="${g.verticalStrap.width}" height="${g.verticalStrap.height}" rx="${g.verticalStrap.rx}" fill="${c.strapOnTile}"/>`,
    `  <path d="${g.diagonal.d}" stroke="${c.arrow}" stroke-width="${g.diagonal.strokeWidth}" stroke-linecap="round"/>`,
    `  <path d="${g.arrowHead.d}" fill="${c.arrow}"/>`,
    `  <rect x="${g.seal.x}" y="${g.seal.y}" width="${g.seal.width}" height="${g.seal.height}" rx="${g.seal.rx}" fill="${c.tile}"/>`,
    `</svg>`,
    "",
  ].join("\n");
}
