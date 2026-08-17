/**
 * Datawrap mark — the canonical wrap lattice.
 *
 * Geometry comes from `brand/mark`, the same definition the favicon, manifest
 * icons and social exports are generated from, so no surface can drift onto an
 * older logo.
 */
import { BRAND_COLORS, BRAND_MARK_GEOMETRY, BRAND_MARK_VIEWBOX } from "../brand/mark";

export function IconFlowMark({ size = 32 }: { size?: number }) {
  const g = BRAND_MARK_GEOMETRY;
  return (
    <svg
      width={size}
      height={size}
      viewBox={`0 0 ${BRAND_MARK_VIEWBOX} ${BRAND_MARK_VIEWBOX}`}
      fill="none"
      role="img"
      shapeRendering="geometricPrecision"
    >
      <title>Datawrap</title>
      <rect {...g.tile} fill={BRAND_COLORS.tile} />
      <rect {...g.horizontalStrap} fill={BRAND_COLORS.strap} />
      <rect {...g.verticalStrap} fill={BRAND_COLORS.strapOnTile} />
      <path
        d={g.diagonal.d}
        stroke={BRAND_COLORS.arrow}
        strokeWidth={g.diagonal.strokeWidth}
        strokeLinecap="round"
      />
      <path d={g.arrowHead.d} fill={BRAND_COLORS.arrow} />
      <rect {...g.seal} fill={BRAND_COLORS.tile} />
    </svg>
  );
}

export function IconOverview({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="none" aria-hidden>
      <rect x="2" y="2" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
      <rect x="10" y="2" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
      <rect x="2" y="10" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
      <rect x="10" y="10" width="6" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
    </svg>
  );
}

export function IconHome({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="none" aria-hidden>
      <path
        d="M3 7.5L9 3L15 7.5V14.5C15 15.05 14.55 15.5 14 15.5H4C3.45 15.5 3 15.05 3 14.5V7.5Z"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M7 15.5V9.5H11V15.5"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function IconTransfer({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="none" aria-hidden>
      <path d="M3 9H13M13 9L10 6M13 9L10 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export function IconConnector({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="none" aria-hidden>
      <circle cx="5" cy="9" r="2.5" stroke="currentColor" strokeWidth="1.5" />
      <circle cx="13" cy="5" r="2.5" stroke="currentColor" strokeWidth="1.5" />
      <circle cx="13" cy="13" r="2.5" stroke="currentColor" strokeWidth="1.5" />
      <path d="M7.2 8.2L10.5 6M7.2 9.8L10.5 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

export function IconJobs({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="none" aria-hidden>
      <path d="M3 4.5H15M3 9H15M3 13.5H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
