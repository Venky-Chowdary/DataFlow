/** DataTransfer.space brand mark — Meridian copper/teal flow */

export function DtLogo({ size = 36 }: { size?: number }) {
  return (
    <svg
      className="dt-brand-mark"
      width={size}
      height={size}
      viewBox="0 0 36 36"
      fill="none"
      aria-hidden
    >
      <rect width="36" height="36" rx="10" fill="url(#dt-logo-bg)" />
      <path
        d="M7 18h5l2.5-5 2.5 10 2.5-5h5.5"
        stroke="white"
        strokeWidth="2.2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="9.5" cy="18" r="2" fill="#e8a677" />
      <circle cx="26.5" cy="18" r="2" fill="#3cb8a4" />
      <defs>
        <linearGradient id="dt-logo-bg" x1="0" y1="0" x2="36" y2="36" gradientUnits="userSpaceOnUse">
          <stop stopColor="#c67a4a" />
          <stop offset="0.5" stopColor="#1f8a7a" />
          <stop offset="1" stopColor="#0f141c" />
        </linearGradient>
      </defs>
    </svg>
  );
}
