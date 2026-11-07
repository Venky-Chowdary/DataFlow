/** UI icon set */

const paths: Record<string, React.ReactNode> = {
  dashboard: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
    </>
  ),
  transfer: (
    <>
      <path d="M4 8h16M20 8l-4-4M20 8l-4 4" />
      <path d="M20 16H4M4 16l4-4M4 16l4 4" />
    </>
  ),
  "arrow-right": <path d="M5 12h14M12 5l7 7-7 7" />,
  "arrow-left": <path d="M19 12H5M12 19l-7-7 7-7" />,
  connectors: (
    <>
      <circle cx="6" cy="6" r="3" />
      <circle cx="18" cy="18" r="3" />
      <path d="M8.5 8.5l7 7" />
    </>
  ),
  jobs: (
    <>
      <rect x="3" y="4" width="18" height="18" rx="2" />
      <path d="M3 10h18M9 4v6" />
    </>
  ),
  settings: (
    <>
      <circle cx="12" cy="12" r="3" />
      <path d="M12 1v2M12 21v2M4.2 4.2l1.4 1.4M18.4 18.4l1.4 1.4M1 12h2M21 12h2M4.2 19.8l1.4-1.4M18.4 5.6l1.4-1.4" />
    </>
  ),
  sun: (
    <>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" />
    </>
  ),
  moon: (
    <path d="M21 14.5A8.5 8.5 0 1111.5 3 7 7 0 0021 14.5z" />
  ),
  plus: <path d="M12 5v14M5 12h14" />,
  menu: (
    <>
      <path d="M4 7h16M4 12h16M4 17h16" />
    </>
  ),
  upload: (
    <>
      <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
      <polyline points="17,8 12,3 7,8" />
      <line x1="12" y1="3" x2="12" y2="15" />
    </>
  ),
  check: <polyline points="20,6 9,17 4,12" />,
  x: (
    <>
      <line x1="18" y1="6" x2="6" y2="18" />
      <line x1="6" y1="6" x2="18" y2="18" />
    </>
  ),
  search: (
    <>
      <circle cx="11" cy="11" r="7" />
      <path d="M16 16l4 4" />
    </>
  ),
  bell: (
    <>
      <path d="M18 8a6 6 0 10-12 0c0 7-3 7-3 7h18s-3 0-3-7" />
      <path d="M13.7 21a2 2 0 003.4 0" />
    </>
  ),
  database: (
    <>
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </>
  ),
  book: (
    <>
      <path d="M4 19.5A2.5 2.5 0 016.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 014 19.5v-15A2.5 2.5 0 016.5 2z" />
    </>
  ),
  shield: <path d="M12 2l8 4v6c0 5.25-3.5 9.74-8 10-4.5-.26-8-4.75-8-10V6l8-4z" />,
  users: (
    <>
      <path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2" />
      <circle cx="9" cy="7" r="4" />
      <path d="M23 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75" />
    </>
  ),
  key: (
    <>
      <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 11-7.778 7.778 5.5 5.5 0 017.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" />
    </>
  ),
  trend: (
    <>
      <polyline points="23,6 13.5,15.5 8.5,10.5 1,18" />
      <polyline points="17,6 23,6 23,12" />
    </>
  ),
  activity: <polyline points="22,12 18,12 15,21 9,3 6,12 2,12" />,
  zap: <polygon points="13,2 3,14 12,14 11,22 21,10 12,10 13,2" />,
  sparkle: (
    <>
      <path d="M12 3l1.5 4.5L18 9l-4.5 1.5L12 15l-1.5-4.5L6 9l4.5-1.5L12 3z" />
      <path d="M19 14l.75 2.25L22 17l-2.25.75L19 20l-.75-2.25L16 17l2.25-.75L19 14z" />
    </>
  ),
  gate: (
    <>
      <path d="M12 3l8 4v5c0 5-3.5 9-8 10-4.5-1-8-5-8-10V7l8-4z" />
      <path d="M9 12l2 2 4-4" />
    </>
  ),
  alert: (
    <>
      <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
      <line x1="12" y1="9" x2="12" y2="13" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
    </>
  ),
  /* Alias used by transfer quarantine / result dashboards */
  warning: (
    <>
      <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
      <line x1="12" y1="9" x2="12" y2="13" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="10" />
      <polyline points="12,6 12,12 16,14" />
    </>
  ),
  download: (
    <>
      <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
      <polyline points="7,10 12,15 17,10" />
      <line x1="12" y1="15" x2="12" y2="3" />
    </>
  ),
  scan: (
    <>
      <path d="M3 7V5a2 2 0 012-2h2" />
      <path d="M17 3h2a2 2 0 012 2v2" />
      <path d="M21 17v2a2 2 0 01-2 2h-2" />
      <path d="M7 21H5a2 2 0 01-2-2v-2" />
    </>
  ),
  eye: (
    <>
      <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
      <circle cx="12" cy="12" r="3" />
    </>
  ),
  "eye-off": (
    <>
      <path d="M17.94 17.94A10.07 10.07 0 0112 20c-7 0-11-8-11-8a18.45 18.45 0 015.06-5.94" />
      <path d="M9.9 4.24A9.12 9.12 0 0112 4c7 0 11 8 11 8a18.5 18.5 0 01-2.16 3.19" />
      <path d="M14.12 14.12a3 3 0 11-4.24-4.24" />
      <line x1="1" y1="1" x2="23" y2="23" />
    </>
  ),
  code: (
    <>
      <polyline points="16,18 22,12 16,6" />
      <polyline points="8,6 2,12 8,18" />
    </>
  ),
  lock: (
    <>
      <rect x="5" y="11" width="14" height="10" rx="2" />
      <path d="M7 11V7a5 5 0 0110 0v4" />
    </>
  ),
  trash: (
    <>
      <polyline points="3,6 5,6 21,6" />
      <path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
      <line x1="10" y1="11" x2="10" y2="17" />
      <line x1="14" y1="11" x2="14" y2="17" />
    </>
  ),
  "chevron-left": <polyline points="15,18 9,12 15,6" />,
  "chevron-right": <polyline points="9,18 15,12 9,6" />,
  "chevron-down": <polyline points="6,9 12,15 18,9" />,
  "chevron-up": <polyline points="18,15 12,9 6,15" />,
  /** Left navigation panel toggle (Cursor / VS Code layout control). */
  "panel-left": (
    <>
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <line x1="9" y1="3" x2="9" y2="21" />
    </>
  ),
  minus: <line x1="5" y1="12" x2="19" y2="12" />,
  "git-branch": (
    <>
      <line x1="6" y1="3" x2="6" y2="15" />
      <circle cx="18" cy="6" r="3" />
      <circle cx="6" cy="18" r="3" />
      <path d="M18 9a9 9 0 01-9 9" />
    </>
  ),
  "arrow-up-right": <path d="M7 17L17 7M7 7h10v10" />,
  edit: (
    <>
      <path d="M12 20h9" />
      <path d="M16.5 3.5a2.12 2.12 0 013 3L7 19l-4 1 1-4 12.5-12.5z" />
    </>
  ),
  expand: (
    <>
      <polyline points="15,3 21,3 21,9" />
      <polyline points="9,21 3,21 3,15" />
      <line x1="21" y1="3" x2="14" y2="10" />
      <line x1="3" y1="21" x2="10" y2="14" />
    </>
  ),
  calendar: (
    <>
      <rect x="3" y="4" width="18" height="18" rx="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </>
  ),
  file: (
    <>
      <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z" />
      <polyline points="13,2 13,9 20,9" />
    </>
  ),
  cloud: (
    <path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z" />
  ),
  server: (
    <>
      <rect x="2" y="2" width="20" height="8" rx="2" />
      <rect x="2" y="14" width="20" height="8" rx="2" />
      <line x1="6" y1="6" x2="6.01" y2="6" />
      <line x1="6" y1="18" x2="6.01" y2="18" />
    </>
  ),
  globe: (
    <>
      <circle cx="12" cy="12" r="10" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
    </>
  ),
  layers: (
    <>
      <polygon points="12 2 2 7 12 12 22 7 12 2" />
      <polyline points="2 17 12 22 22 17" />
      <polyline points="2 12 12 17 22 12" />
    </>
  ),
  send: (
    <>
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22,2 15,22 11,13 2,9 22,2" />
    </>
  ),
  speed: (
    <>
      <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
      <circle cx="12" cy="12" r="9" />
    </>
  ),
  play: <polygon points="5,3 19,12 5,21 5,3" />,
  pause: (
    <>
      <rect x="6" y="4" width="4" height="16" rx="1" />
      <rect x="14" y="4" width="4" height="16" rx="1" />
    </>
  ),
  cpu: (
    <>
      <rect x="6" y="6" width="12" height="12" rx="2" />
      <path d="M9 6V3M15 6V3M9 18v3M15 18v3M6 9H3M6 15H3M18 9h3M18 15h3" />
    </>
  ),
  refresh: (
    <>
      <path d="M21 12a9 9 0 11-2.6-6.4" />
      <polyline points="21,3 21,9 15,9" />
    </>
  ),
  spinner: (
    <>
      <path d="M21 12a9 9 0 11-6.5-8.6" />
    </>
  ),
  /* Recipe canvas. Same stroke, one picture per operation. */
  filter: <path d="M4 4h16l-6 7v6l-4 2V11L4 4z" />,
  cast: (
    <>
      <path d="M4 7h7M7.5 7v10" />
      <path d="M14 17l3-10 3 10M15 14h4" />
    </>
  ),
  trim: (
    <>
      <path d="M4 8h16M4 16h8" />
      <path d="M14 13l4 3-4 3" />
    </>
  ),
  mask: (
    <>
      <path d="M2 12s4-6 10-6 10 6 10 6-4 6-10 6S2 12 2 12z" />
      <circle cx="12" cy="12" r="2.5" />
      <path d="M4 4l16 16" />
    </>
  ),
  hash: (
    <>
      <circle cx="12" cy="12" r="8" />
      <path d="M12 8c-2 2-2 6 0 8M9 10c2 1 4 1 6 0M9 14c2-1 4-1 6 0" />
    </>
  ),
  dedupe: (
    <>
      <rect x="8" y="8" width="11" height="11" rx="1.5" />
      <path d="M5 15V5h10" />
      <path d="M11 13.5h5" />
    </>
  ),
  split: (
    <>
      <path d="M4 6h6l3 6-3 6H4" />
      <path d="M13 12h7" />
    </>
  ),
  unnest: (
    <>
      <rect x="4" y="4" width="16" height="6" rx="1" />
      <path d="M12 10v3" />
      <rect x="8" y="15" width="8" height="5" rx="1" />
    </>
  ),
  join: (
    <>
      <rect x="3" y="7" width="7" height="10" rx="1" />
      <rect x="14" y="7" width="7" height="10" rx="1" />
      <path d="M10 12h4" />
    </>
  ),
  aggregate: (
    <>
      <path d="M4 18L12 5l8 13" />
      <path d="M7.5 13h9" />
    </>
  ),
  sort: (
    <>
      <path d="M8 19V5M8 5L5 8M8 5l3 3" />
      <path d="M16 5v14M16 19l-3-3M16 19l3-3" />
    </>
  ),
  rank: (
    <>
      <path d="M6 20V11M12 20V4M18 20v-7" />
    </>
  ),
  window: (
    <>
      <rect x="3" y="5" width="18" height="14" rx="1.5" />
      <path d="M3 9h18" />
    </>
  ),
  router: (
    <>
      <path d="M12 4v6" />
      <path d="M12 10l-7 8M12 10l7 8" />
      <circle cx="12" cy="10" r="1.5" />
    </>
  ),
  union: (
    <>
      <rect x="3" y="7" width="10" height="10" rx="1" />
      <rect x="11" y="7" width="10" height="10" rx="1" />
    </>
  ),
  normalizer: (
    <>
      <path d="M4 6h16M4 12h10M4 18h16" />
    </>
  ),
  mapplet: (
    <>
      <rect x="3" y="3" width="8" height="8" rx="1" />
      <rect x="13" y="13" width="8" height="8" rx="1" />
      <path d="M11 7h2l2 2" />
    </>
  ),
  flatten: (
    <>
      <path d="M4 8h16M4 12h16M4 16h16" />
    </>
  ),
  rename: (
    <>
      <path d="M4 18h8" />
      <path d="M14 6h6M17 6v12" />
    </>
  ),
  concat: <path d="M4 12h6M14 12h6M10 12l2-3 2 3-2 3-2-3z" />,
  collapse: <path d="M4 8h16M7 12h10M9 16h6" />,
  case: (
    <>
      <path d="M5 17l3-10 3 10M6 14h4" />
      <path d="M15 17v-7a2 2 0 014 0v7M15 13h4" />
    </>
  ),
  strip: <path d="M5 8h14M8 12h8M10 16h4" />,
  pad: (
    <>
      <path d="M4 12h16" />
      <path d="M7 8v8M17 8v8" />
    </>
  ),
  replace: (
    <>
      <path d="M4 8h8M4 16h6" />
      <path d="M14 8h6M14 16h6M17 5v6M17 13v6" />
    </>
  ),
  default: (
    <>
      <circle cx="12" cy="12" r="8" />
      <path d="M12 8v8M8 12h8" />
    </>
  ),
  null: (
    <>
      <circle cx="12" cy="12" r="8" />
      <path d="M8 12h8" />
    </>
  ),
  round: (
    <>
      <circle cx="12" cy="12" r="8" />
      <path d="M12 8v4l3 2" />
    </>
  ),
  truncate: <path d="M5 12h14M14 8l4 4-4 4" />,
  absolute: <path d="M7 18V6M7 6h6a4 4 0 010 8H7M17 8v10" />,
  clamp: (
    <>
      <path d="M4 8h16M4 16h16" />
      <path d="M8 8v8M16 8v8" />
    </>
  ),
  number: <path d="M8 5l-2 14M16 5l2 14M6 10h12M5 14h12" />,
  pivot: (
    <>
      <path d="M4 4v16M4 20h16" />
      <path d="M8 16V10M12 16V7M16 16v-3" />
    </>
  ),
  unpivot: (
    <>
      <path d="M4 4h16M4 4v16" />
      <path d="M8 8h8M8 12h6M8 16h4" />
    </>
  ),
};

export function DtIcon({
  name,
  size = 20,
  chrome = "nav",
}: {
  name: string;
  size?: number;
  /**
   * `nav` is the historical wrapper and stroke, so existing screens stay put.
   * `product` is the studio family: 1.75 at 16/20, 2 at 24, no nav metrics,
   * and an unknown name draws nothing.
   */
  chrome?: "nav" | "product";
}) {
  const known = Object.prototype.hasOwnProperty.call(paths, name);
  if (!known && chrome === "product" && typeof console !== "undefined") {
    console.warn(`DtIcon: unknown icon "${name}"`);
  }
  const stroke = chrome === "product" ? (size >= 22 ? 2.4 : 2) : (size >= 18 ? 2.15 : 2);
  const svg = (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={stroke}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {known ? paths[name] : chrome === "product" ? null : paths.database}
    </svg>
  );
  if (chrome === "product") return svg;
  return <span className="dt-nav-icon">{svg}</span>;
}
