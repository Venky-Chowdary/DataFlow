/**
 * DataFlow mark — source · gate · destination
 */

export function IconFlowMark({ size = 32 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 40 40" fill="none" aria-hidden role="img">
      <title>DataFlow</title>
      <defs>
        <linearGradient id="df-flow-orange" x1="4" y1="20" x2="20" y2="20" gradientUnits="userSpaceOnUse">
          <stop stopColor="#FF4D00" />
          <stop offset="1" stopColor="#FF4D00" stopOpacity="0.6" />
        </linearGradient>
        <linearGradient id="df-flow-mint" x1="20" y1="20" x2="36" y2="20" gradientUnits="userSpaceOnUse">
          <stop stopColor="#00B87A" stopOpacity="0.6" />
          <stop offset="1" stopColor="#00B87A" />
        </linearGradient>
      </defs>
      <circle cx="8" cy="20" r="4" fill="#FF4D00" />
      <path d="M12 20 H17" stroke="url(#df-flow-orange)" strokeWidth="2.5" strokeLinecap="round" />
      <rect x="17.5" y="17.5" width="5" height="5" rx="1" transform="rotate(45 20 20)" fill="#FF4D00" />
      <path d="M23 20 H32" stroke="url(#df-flow-mint)" strokeWidth="2.5" strokeLinecap="round" />
      <circle cx="32" cy="20" r="4" fill="#00B87A" />
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
      <path d="M4 4H14V14H4V4Z" stroke="currentColor" strokeWidth="1.5" />
      <path d="M6 7H12M6 10H10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
