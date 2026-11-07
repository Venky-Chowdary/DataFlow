/**
 * Datawrap mark — wrap lattice.
 *
 * Geometry and colours come from `lib/brandMark`, the one definition the
 * favicon, manifest icons and social exports are also generated from.
 *
 * Uniqueness notes (research pass):
 * - NOT headphones (no twin uprights + headband)
 * - NOT Fivetran blue blob, Airbyte octopus, Confluent hex, Jira chevron
 * - NOT Datawrapper (wordmark-only charting brand — different product)
 * Concept: three cargo-style wrap straps sealing a center void, diagonal
 * strap ends as directional arrow (secured anywhere→anywhere).
 */
import { BRAND_COLORS, BRAND_MARK_GEOMETRY, BRAND_MARK_VIEWBOX } from "../lib/brandMark";

interface DtLogoProps {
  size?: number;
  title?: string;
  variant?: "app" | "plain";
  fidelity?: "svg" | "raster";
}

export function DtLogo({
  size = 36,
  title = "Datawrap",
  variant = "app",
  fidelity = "svg",
}: DtLogoProps) {
  const decorative = !title;

  if (fidelity === "raster" && variant === "app") {
    return (
      <img
        className="dt-brand-mark dt-brand-mark--raster"
        src="/brand/datawrap-mark.png"
        width={size}
        height={size}
        alt={decorative ? "" : title}
        aria-hidden={decorative ? true : undefined}
        draggable={false}
      />
    );
  }

  const isApp = variant === "app";
  const g = BRAND_MARK_GEOMETRY;

  return (
    <svg
      className="dt-brand-mark"
      width={size}
      height={size}
      viewBox={`0 0 ${BRAND_MARK_VIEWBOX} ${BRAND_MARK_VIEWBOX}`}
      fill="none"
      role={decorative ? undefined : "img"}
      aria-hidden={decorative ? true : undefined}
      aria-label={decorative ? undefined : title}
      shapeRendering="geometricPrecision"
    >
      {!decorative && <title>{title}</title>}
      {isApp && <rect {...g.tile} fill={BRAND_COLORS.tile} />}

      <rect {...g.horizontalStrap} fill={BRAND_COLORS.strap} />
      <rect
        {...g.verticalStrap}
        fill={isApp ? BRAND_COLORS.strapOnTile : BRAND_COLORS.strapPlain}
      />
      <path
        d={g.diagonal.d}
        stroke={BRAND_COLORS.arrow}
        strokeWidth={g.diagonal.strokeWidth}
        strokeLinecap="round"
      />
      <path d={g.arrowHead.d} fill={BRAND_COLORS.arrow} />

      {/* Center seal void — reads as secured packet, not ear cup */}
      <rect
        {...g.seal}
        fill={isApp ? BRAND_COLORS.tile : BRAND_COLORS.sealPlain}
        stroke={isApp ? BRAND_COLORS.tile : BRAND_COLORS.sealPlain}
        strokeWidth="1"
      />
    </svg>
  );
}
