/**
 * Datawrap mark — wrap lattice (original geometry).
 *
 * Uniqueness notes (research pass):
 * - NOT headphones (no twin uprights + headband)
 * - NOT Fivetran blue blob, Airbyte octopus, Confluent hex, Jira chevron
 * - NOT Datawrapper (wordmark-only charting brand — different product)
 * Concept: three cargo-style wrap straps sealing a center void, diagonal
 * strap ends as directional arrow (secured anywhere→anywhere).
 */

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

  return (
    <svg
      className="dt-brand-mark"
      width={size}
      height={size}
      viewBox="0 0 64 64"
      fill="none"
      role={decorative ? undefined : "img"}
      aria-hidden={decorative ? true : undefined}
      aria-label={decorative ? undefined : title}
      shapeRendering="geometricPrecision"
    >
      {!decorative && <title>{title}</title>}
      {isApp && <rect width="64" height="64" rx="14" fill="#0A3D3A" />}

      {/* Horizontal wrap strap */}
      <rect x="10" y="28" width="44" height="8" rx="2.5" fill="#F59E0B" />
      {/* Vertical wrap strap */}
      <rect x="28" y="10" width="8" height="44" rx="2.5" fill={isApp ? "#F8FAFC" : "#0F766E"} />
      {/* Diagonal wrap strap → arrow (unique third axis) */}
      <path
        d="M16 48 L40 24"
        stroke="#2DD4BF"
        strokeWidth="8"
        strokeLinecap="round"
      />
      <path d="M36 18 L52 14 L44 30 Z" fill="#2DD4BF" />

      {/* Center seal void — reads as secured packet, not ear cup */}
      <rect
        x="27"
        y="27"
        width="10"
        height="10"
        rx="2"
        fill={isApp ? "#0A3D3A" : "#FFFFFF"}
        stroke={isApp ? "#0A3D3A" : "#FFFFFF"}
        strokeWidth="1"
      />
    </svg>
  );
}
