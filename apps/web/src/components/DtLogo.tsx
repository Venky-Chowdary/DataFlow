/** DataFlow brand mark — clean node-flow icon */

import { useId } from "react";

interface DtLogoProps {
  size?: number;
}

export function DtLogo({ size = 36 }: DtLogoProps) {
  const gradId = useId().replace(/:/g, "");
  return (
    <svg
      className="dt-brand-mark"
      width={size}
      height={size}
      viewBox="0 0 36 36"
      fill="none"
      aria-hidden
    >
      <rect width="36" height="36" rx="9" fill={`url(#${gradId})`} />
      <path
        d="M10 24V14M10 14h6l2 4 4-8h4"
        stroke="white"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.95"
      />
      <circle cx="10" cy="24" r="2.5" fill="#F59E0B" />
      <circle cx="10" cy="14" r="2.5" fill="#2DD4BF" />
      <circle cx="26" cy="10" r="2.5" fill="#A7F3D0" />
      <defs>
        <linearGradient id={gradId} x1="4" y1="4" x2="32" y2="32" gradientUnits="userSpaceOnUse">
          <stop stopColor="#134E4A" />
          <stop offset="1" stopColor="#111827" />
        </linearGradient>
      </defs>
    </svg>
  );
}
