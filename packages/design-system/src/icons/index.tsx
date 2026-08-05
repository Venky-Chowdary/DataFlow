/**
 * Datawrap mark — source · wrap gate · destination
 */

export function IconFlowMark({ size = 32 }: { size?: number }) {
  const gradId = `dw-bg-${size}`;
  return (
    <svg width={size} height={size} viewBox="0 0 512 512" fill="none" aria-hidden role="img">
      <title>Datawrap</title>
      <defs>
        <linearGradient id={gradId} x1="72" y1="48" x2="440" y2="464" gradientUnits="userSpaceOnUse">
          <stop stopColor="#0F766E" />
          <stop offset="0.55" stopColor="#134E4A" />
          <stop offset="1" stopColor="#0B1220" />
        </linearGradient>
      </defs>
      <rect width="512" height="512" rx="112" fill={`url(#${gradId})`} />
      <path
        d="M128 256 L176 176 H336 L384 256 L336 336 H176 Z"
        stroke="#FFFFFF"
        strokeWidth="28"
        strokeLinejoin="round"
        fill="none"
      />
      <path
        d="M256 198 L314 256 L256 314 L198 256 Z"
        stroke="#FFFFFF"
        strokeWidth="26"
        strokeLinejoin="round"
        fill="none"
      />
      <path d="M72 256 H128" stroke="#FFFFFF" strokeWidth="28" strokeLinecap="round" />
      <path d="M384 256 H440" stroke="#FFFFFF" strokeWidth="28" strokeLinecap="round" />
      <circle cx="72" cy="256" r="34" fill="#F59E0B" />
      <circle cx="440" cy="256" r="34" fill="#2DD4BF" />
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
